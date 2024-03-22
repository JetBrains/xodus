/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.cache.persistent.PersistentCacheClient;
import jetbrains.exodus.core.dataStructures.*;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.crypto.EncryptedBlobVault;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.entitystore.iterate.link.OLinkExistsEntityIterable;
import jetbrains.exodus.entitystore.iterate.link.OLinkToEntityIterable;
import jetbrains.exodus.entitystore.iterate.property.*;
import jetbrains.exodus.entitystore.orientdb.ODatabaseSessionsKt;
import jetbrains.exodus.entitystore.orientdb.OEntity;
import jetbrains.exodus.entitystore.orientdb.OEntityId;
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction;
import jetbrains.exodus.env.*;
import jetbrains.exodus.util.StringBuilderSpinAllocator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

@SuppressWarnings({"rawtypes"})
public class PersistentStoreTransaction implements OStoreTransaction, StoreTransaction, TxnGetterStrategy, TxnProvider {
    private static final Logger logger = LoggerFactory.getLogger(PersistentStoreTransaction.class);

    enum TransactionType {
        Regular,
        Exclusive,
        Readonly
    }

    private static final int LOCAL_CACHE_GENERATIONS = 2;
    @NotNull
    private static final ByteIterable ZERO_VERSION_ENTRY = IntegerBinding.intToCompressedEntry(0);

    @NotNull
    protected final PersistentEntityStoreImpl store;
    @NotNull
    protected final Transaction txn;
    private final ObjectCacheBase<PropertyId, Comparable> propsCache;
    @NotNull
    private final ObjectCacheBase<PropertyId, PersistentEntityId> linksCache;
    @NotNull
    private final ObjectCacheBase<PropertyId, String> blobStringsCache;
    private EntityIterableCacheAdapter localCache;
    private int localCacheAttempts;
    private int localCacheHits;
    @Nullable
    private EntityIterableCacheAdapterMutable mutableCache;
    private List<Updatable> mutatedInTxn;
    @Nullable
    private LongHashMap<Triple<Path, Boolean, Long>> blobStreams;

    private volatile LongHashMap<ArrayList<InputStream>> openedBlobStreams;
    private final ReentrantLock openedBlobStreamsLock = new ReentrantLock();

    @Nullable
    private LongHashMap<Path> blobFiles;

    private LongSet deferredBlobsToDelete;
    private QueryCancellingPolicy queryCancellingPolicy;

    private boolean checkInvalidateBlobsFlag;

    public PersistentStoreTransaction(@NotNull final PersistentEntityStoreImpl store) {
        this(store, TransactionType.Regular);
    }

    /**
     * Ctor for creating snapshot transactions.
     *
     * @param source  source txn which snapshot should be created of.
     * @param txnType type of snapshot transaction, only {@linkplain TransactionType#Regular} or
     *                {@linkplain TransactionType#Readonly} are allowed
     */
    PersistentStoreTransaction(@NotNull final PersistentStoreTransaction source,
                               @NotNull final TransactionType txnType) {
        this.store = source.store;
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        localCache = source.localCache;
        localCacheAttempts = localCacheHits = 0;
        switch (txnType) {
            case Regular:
                txn = source.txn.getSnapshot(getInitCachesBeginHook());
                break;
            case Readonly:
                txn = source.txn.getReadonlySnapshot();
                break;
            default:
                throw new EntityStoreException("Can't create exclusive snapshot transaction");
        }
    }

    protected PersistentStoreTransaction(@NotNull final PersistentEntityStoreImpl store,
                                         @NotNull final TransactionType txnType) {
        this.store = store;
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        localCacheAttempts = localCacheHits = 0;
        final Runnable beginHook = getInitCachesBeginHook();
        final Environment env = store.getEnvironment();
        switch (txnType) {
            case Regular:
                txn = env.beginTransaction(beginHook);
                break;
            case Exclusive:
                txn = env.beginExclusiveTransaction(beginHook);
                mutableCache = createMutableCache();
                break;
            case Readonly:
                txn = env.beginReadonlyTransaction(beginHook);
                break;
            default:
                throw new EntityStoreException("Can't create " + txnType + " transaction");
        }
    }

    @NotNull
    @Override
    public ODatabaseDocument activeSession() {
        return ODatabaseSession.getActiveSession();
    }

    @Override
    @NotNull
    public PersistentEntityStoreImpl getStore() {
        return store;
    }

    @Override
    public boolean isIdempotent() {
        return getEnvironmentTransaction().isIdempotent() &&
                (blobStreams == null || blobStreams.isEmpty()) &&
                (blobFiles == null || blobFiles.isEmpty());
    }

    @Override
    public boolean isReadonly() {
        return txn.isReadonly();
    }

    @Override
    public boolean isFinished() {
        return txn.isFinished();
    }

    @Override
    public boolean commit() {
        // txn can be read-only if Environment is in read-only mode
        if (!isReadonly()) {
            apply();
            return doCommit();
        } else if (txn.isExclusive()) {
            applyExclusiveTransactionCaches();
        }
        return true;
    }

    public boolean isCurrent() {
        return true;
    }

    private boolean doCommit() {
        if (txn.commit()) {
            store.unregisterTransaction(this);
            flushNonTransactionalBlobs();
            flushCaches(true);
            return true;
        }
        revert();
        return false;
    }

    @Override
    public void abort() {
        try {
            closeOpenedBlobStreams();
            store.unregisterTransaction(this);
            revertCaches();
        } finally {
            txn.abort();
        }
    }

    @Override
    public boolean flush() {
        // txn can be read-only if Environment is in read-only mode
        if (!isReadonly()) {
            apply();
            return doFlush();
        } else if (txn.isExclusive()) {
            applyExclusiveTransactionCaches();
        }
        return true;
    }

    // exposed only for tests
    boolean doFlush() {
        if (txn.flush()) {
            flushNonTransactionalBlobs();
            flushCaches(false); // do not clear props & links caches
            return true;
        }
        revert();
        return false;
    }

    @Override
    public void revert() {
        closeOpenedBlobStreams();
        txn.revert();
        revertCaches();
    }

    public PersistentStoreTransaction getSnapshot() {
        // this snapshots should not be registered in store, hence no de-registration
        return new PersistentStoreTransactionSnapshot(this);
    }

    @Override
    @NotNull
    public PersistentEntity newEntity(@NotNull final String entityType) {
        try {
            final int entityTypeId = store.getEntityTypeId(this, entityType, true);
            final long entityLocalId = store.getEntitiesSequence(this, entityTypeId).increment();
            if (store.useVersion1Format()) {
                //noinspection deprecation
                store.getEntitiesTable(this, entityTypeId).putRight(
                        txn, LongBinding.longToCompressedEntry(entityLocalId), ZERO_VERSION_ENTRY);
            } else {
                store.getEntitiesBitmapTable(this, entityTypeId).set(txn, entityLocalId, true);
            }
            final PersistentEntityId id = new PersistentEntityId(entityTypeId, entityLocalId);
            // update iterables' cache
            new EntityAddedHandleCheckerImpl(this, id, mutableCache(), mutatedInTxn).updateCache();
            return new PersistentEntity(store, id);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    public void saveEntity(@NotNull final Entity entity) {
        try {
            final EntityId entityId = entity.getId();
            if (store.useVersion1Format()) {
                @SuppressWarnings("deprecation") final Store entitiesTable = store.getEntitiesTable(this, entityId.getTypeId());
                entitiesTable.put(txn, LongBinding.longToCompressedEntry(entityId.getLocalId()), ZERO_VERSION_ENTRY);
            } else {
                final Bitmap bitmapEntitiesTable = store.getEntitiesBitmapTable(this, entityId.getTypeId());
                bitmapEntitiesTable.set(txn, entityId.getLocalId(), true);
            }
            new EntityAddedHandleCheckerImpl(this, entityId, mutableCache(), mutatedInTxn).updateCache();
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    @NotNull
    public Entity getEntity(@NotNull final EntityId id) {
        final int version = store.getLastVersion(this, id);
        if (version < 0) {
            throw new EntityRemovedInDatabaseException(store.getEntityType(this, id.getTypeId()), id);
        }
        if (id instanceof OEntityId) {
            var oid = ((OEntityId) id).asOId();
            return ODatabaseSessionsKt.getVertexEntity(activeSession(), oid);
        }
        return new PersistentEntity(store, (PersistentEntityId) id);
    }

    @Override
    @NotNull
    public List<String> getEntityTypes() {
        return store.getEntityTypes(this);
    }

    @Override
    @NotNull
    public EntityIterable getAll(@NotNull final String entityType) {
        return new OEntityOfTypeIterable(this, entityType);
    }

    @Override
    @NotNull
    public EntityIterable getSingletonIterable(@NotNull final Entity entity) {
        return new SingleEntityIterable(this, entity.getId());
    }

    @Override
    @NotNull
    public EntityIterable find(@NotNull final String entityType,
                               @NotNull final String propertyName,
                               @NotNull final Comparable value) {
        return new OPropertyEqualIterable(this, entityType, propertyName, value);
    }

    @Override
    @NotNull
    public EntityIterable find(@NotNull final String entityType, @NotNull final String propertyName,
                               @NotNull final Comparable minValue, @NotNull final Comparable maxValue) {
        if (minValue instanceof Boolean) {
            final boolean min = (Boolean) minValue;
            final boolean max = (Boolean) maxValue;
            if (min == max) {
                if (min) {
                    return findWithProp(entityType, propertyName);
                }
            } else {
                if (max) {
                    return getAll(entityType);
                }
            }
            return EntityIterableBase.EMPTY;
        }
        return new OPropertyRangeIterable(this, entityType, propertyName, minValue, maxValue);
    }

    // ignoreCase param is not supported and defined on the property level
    // https://orientdb.com/docs/3.2.x/sql/SQL-Alter-Property.html?highlight=Collation#supported-attributes
    @Override
    public @NotNull EntityIterable findContaining(@NotNull final String entityType, @NotNull final String propertyName,
                                                  @NotNull final String value, final boolean ignoreCase) {
        if (value.isEmpty()) {
            return findWithPropSortedByValue(entityType, propertyName);
        }
        return new OPropertyContainsIterable(this, entityType, propertyName, value);
    }

    @Override
    @NotNull
    public EntityIterable findIds(@NotNull final String entityType, final long minValue, final long maxValue) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesOfTypeRangeIterable(this, entityTypeId, minValue, maxValue);
    }

    @NotNull
    @Override
    public EntityIterableBase findWithProp(@NotNull final String entityType, @NotNull final String propertyName) {
        return new OPropertyExistsIterable(this, entityType, propertyName);
    }

    public EntityIterableBase findWithPropSortedByValue(@NotNull final String entityType, @NotNull final String propertyName) {
        return new OPropertyExistsSortedIterable(this, entityType, propertyName);
    }

    @Override
    @NotNull
    public EntityIterable findStartingWith(@NotNull final String entityType,
                                           @NotNull final String propertyName,
                                           @NotNull final String value) {
        final int len = value.length();
        if (len == 0) {
            return getAll(entityType);
        }
        return new OPropertyStartsWithIterable(this, entityType, propertyName, value);
    }

    @NotNull
    @Override
    public EntityIterable findWithBlob(@NotNull final String entityType, @NotNull final String blobName) {
        return new OPropertyBlobExistsEntityIterable(this, entityType, blobName);
    }

    @Override
    @NotNull
    public EntityIterable findLinks(@NotNull final String entityType,
                                    @NotNull final Entity entity,
                                    @NotNull final String linkName) {
        return new OLinkToEntityIterable(this, entityType, linkName, ((OEntity) entity).getId());
    }

    @Override
    @NotNull
    public EntityIterable findLinks(@NotNull final String entityType,
                                    @NotNull final EntityIterable entities,
                                    @NotNull final String linkName) {
        // create balanced union tree
        List<EntityIterable> links = null;
        for (final Entity entity : entities) {
            if (links == null) {
                links = new ArrayList<>();
            }
            links.add(findLinks(entityType, entity, linkName));
        }
        if (links == null) {
            return EntityIterableBase.EMPTY;
        }
        if (links.size() > 1) {
            for (int i = 0; i < links.size() - 1; i += 2) {
                links.add(links.get(i).union(links.get(i + 1)));
            }
        }
        return links.get(links.size() - 1);
    }

    @Override
    @NotNull
    public EntityIterable findWithLinks(@NotNull final String entityType, @NotNull final String linkName) {
        return new OLinkExistsEntityIterable(this, entityType, linkName);
    }

    @Override
    @NotNull
    public EntityIterable findWithLinks(@NotNull final String entityType,
                                        @NotNull final String linkName,
                                        @NotNull final String oppositeEntityType,
                                        @NotNull final String oppositeLinkName) {
        return new OLinkExistsEntityIterable(this, entityType, linkName);
    }

    @Override
    @NotNull
    public EntityIterable sort(@NotNull final String entityType,
                               @NotNull final String propertyName,
                               final boolean ascending) {
        return new OPropertySortedIterable(this, entityType, propertyName, ascending);
    }

    @Override
    @NotNull
    public EntityIterable sort(@NotNull final String entityType,
                               @NotNull final String propertyName,
                               @NotNull final EntityIterable rightOrder,
                               final boolean ascending) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0 || rightOrder == EntityIterableBase.EMPTY) {
            return rightOrder;
        }
        return new SortIterable(this, findWithPropSortedByValue(
                entityType, propertyName), (EntityIterableBase) rightOrder, entityTypeId, propertyId, ascending);
    }

    @Override
    @NotNull
    public EntityIterable sortLinks(@NotNull final String entityType,
                                    @NotNull final EntityIterable sortedLinks,
                                    final boolean isMultiple,
                                    @NotNull final String linkName,
                                    @NotNull final EntityIterable rightOrder) {
        final EntityIterable result = new SortIndirectIterable(this, store, entityType,
                ((EntityIterableBase) sortedLinks).getSource(), linkName, (EntityIterableBase) rightOrder,
                null, null);
        return isMultiple ? result.distinct() : result;
    }

    @Override
    @NotNull
    public EntityIterable sortLinks(@NotNull final String entityType,
                                    @NotNull final EntityIterable sortedLinks,
                                    final boolean isMultiple,
                                    @NotNull final String linkName,
                                    @NotNull final EntityIterable rightOrder,
                                    @NotNull final String oppositeEntityType,
                                    @NotNull final String oppositeLinkName) {
        final EntityIterable result = new SortIndirectIterable(this, store, entityType,
                ((EntityIterableBase) sortedLinks).getSource(), linkName, (EntityIterableBase) rightOrder, oppositeEntityType, oppositeLinkName);
        return isMultiple ? result.distinct() : result;
    }

    @Override
    @Deprecated
    @NotNull
    public EntityIterable mergeSorted(@NotNull final List<EntityIterable> sorted,
                                      @NotNull final Comparator<Entity> comparator) {
        List<EntityIterable> filtered = null;
        for (final EntityIterable it : sorted) {
            if (it != EntityIterableBase.EMPTY) {
                if (filtered == null) {
                    filtered = new ArrayList<>();
                }
                filtered.add(it);
            }
        }
        return filtered == null ? EntityIterableBase.EMPTY : new MergeSortedIterable(this, filtered, comparator);
    }

    @NotNull
    public EntityIterable mergeSorted(@NotNull final List<EntityIterable> sorted,
                                      @NotNull ComparableGetter valueGetter,
                                      @NotNull final Comparator<Comparable<Object>> comparator) {
        List<EntityIterable> filtered = null;
        for (final EntityIterable it : sorted) {
            if (it != EntityIterableBase.EMPTY) {
                if (filtered == null) {
                    filtered = new ArrayList<>();
                }
                filtered.add(it);
            }
        }
        return filtered == null ? EntityIterableBase.EMPTY : new MergeSortedIterableWithValueGetter(this,
                filtered, valueGetter, comparator);
    }

    @Override
    @NotNull
    public EntityId toEntityId(@NotNull final String representation) {
        return PersistentEntityId.toEntityId(representation);
    }

    @Override
    @NotNull
    public Sequence getSequence(@NotNull final String sequenceName) {
        try {
            return store.getSequence(this, sequenceName);
        } catch (Exception e) {
            throw ExodusException.wrap(e);
        }
    }

    @Override
    public @NotNull Sequence getSequence(@NotNull final String sequenceName, final long initialValue) {
        try {
            return store.getSequence(this, sequenceName, initialValue);
        } catch (Exception e) {
            throw ExodusException.wrap(e);
        }
    }

    @Override
    public void setQueryCancellingPolicy(QueryCancellingPolicy policy) {
        queryCancellingPolicy = policy;
    }

    @Override
    @Nullable
    public QueryCancellingPolicy getQueryCancellingPolicy() {
        return queryCancellingPolicy;
    }

    public void registerMutatedHandle(@NotNull final EntityIterableHandle handle, @NotNull final CachedInstanceIterable iterable) {
        final EntityIterableCacheAdapterMutable cache = this.mutableCache;
        if (cache == null) {
            throw new IllegalStateException("Transaction wasn't mutated");
        }
        cache.cacheObject(handle, iterable);
    }

    public void registerStickyObject(@NotNull final EntityIterableHandle handle, Updatable object) {
        mutableCache().registerStickyObject(handle, object);
    }

    @NotNull
    public Updatable getStickyObject(@NotNull final EntityIterableHandle handle) {
        return getLocalCache().getStickyObject(handle);
    }

    @SuppressWarnings({"HardCodedStringLiteral"})
    public String toString() {
        final StringBuilder builder = StringBuilderSpinAllocator.alloc();
        try {
            builder.append(store.getLocation());
            builder.append(", thread = ");
            builder.append(Thread.currentThread());
            return builder.toString();
        } finally {
            StringBuilderSpinAllocator.dispose(builder);
        }
    }

    @NotNull
    public Transaction getEnvironmentTransaction() {
        return txn;
    }

    @Override
    public PersistentStoreTransaction getTxn(@NotNull EntityIterableBase iterable) {
        return this; // this is fun
    }

    @Override
    public @NotNull PersistentStoreTransaction getTransaction() {
        return this;
    }

    @Nullable
    public CachedInstanceIterable getCachedInstance(@NotNull final EntityIterableBase sample) {
        final EntityIterableHandle handle = sample.getHandle();
        final EntityIterableCacheAdapter localCache = getLocalCache();
        return localCache.getObject(handle);
    }

    @Nullable
    public CachedInstanceIterable getCachedInstanceFast(@NotNull final EntityIterableBase sample) {
        final EntityIterableHandle handle = sample.getHandle();
        final EntityIterableCacheAdapter localCache = getLocalCache();
        return localCache.getObject(handle);
    }

    public void addCachedInstance(@NotNull final CachedInstanceIterable cached) {
        final EntityIterableCacheAdapter localCache = getLocalCache();
        final EntityIterableHandle handle = cached.getHandle();
        if (localCache.getObject(handle) == null) {
            localCache.cacheObject(handle, cached);
            store.getEntityIterableCache().setCachedCount(handle, cached.size());
        }
    }

    @NotNull
    EntityIterableCacheAdapter getLocalCache() {
        return mutableCache != null ? mutableCache : localCache;
    }

    void localCacheAttempt() {
        ++localCacheAttempts;
    }

    void localCacheHit() {
        ++localCacheHits;
    }

    boolean isCachingRelevant() {
        // HEURISTICS:
        // caching is irrelevant for the transaction if there are more than a quarter of the EntityIterablesCache's size
        // attempts to query an EntityIterable with local cache hit rate less than 25%
        return localCacheAttempts <= localCache.size() >> 2 || localCacheHits >= localCacheAttempts >> 2;
    }

    void cacheProperty(@NotNull final PersistentEntityId fromId, final int propId, @NotNull final Comparable value) {
        final PropertyId fromEnd = new PropertyId(fromId, propId);
        if (propsCache.getObject(fromEnd) == null) {
            propsCache.cacheObject(fromEnd, value);
        }
    }

    Comparable getCachedProperty(@NotNull final PersistentEntity from, final int propId) {
        return propsCache.tryKey(new PropertyId(from.getId(), propId));
    }

    void cacheLink(@NotNull final PersistentEntity from, final int linkId, @NotNull final PersistentEntityId to) {
        final PropertyId fromEnd = new PropertyId(from.getId(), linkId);
        if (linksCache.getObject(fromEnd) != null) {
            throw new IllegalStateException("Link is already cached, at first it should be invalidated");
        }
        linksCache.cacheObject(fromEnd, to);
    }

    PersistentEntityId getCachedLink(@NotNull final PersistentEntity from, final int linkId) {
        return linksCache.tryKey(new PropertyId(from.getId(), linkId));
    }

    void cacheBlobString(@NotNull final PersistentEntity from, final int blobId, @NotNull final String value) {
        final PropertyId fromEnd = new PropertyId(from.getId(), blobId);
        if (blobStringsCache.getObject(fromEnd) != null) {
            throw new IllegalStateException("Blob string is already cached, at first it should be invalidated");
        }
        blobStringsCache.cacheObject(fromEnd, value);
    }

    String getCachedBlobString(@NotNull final PersistentEntity from, final int blobId) {
        return blobStringsCache.tryKey(new PropertyId(from.getId(), blobId));
    }

    void invalidateCachedBlobString(@NotNull final PersistentEntity from, final int blobId) {
        blobStringsCache.remove(new PropertyId(from.getId(), blobId));
    }

    boolean isMutable() {
        return mutableCache != null;
    }

    void entityDeleted(@NotNull final PersistentEntityId id) {
        new EntityDeletedHandleCheckerImpl(this, id, mutableCache(), mutatedInTxn).updateCache();
    }

    void propertyChanged(@NotNull final PersistentEntityId id,
                         final int propertyId,
                         @Nullable final Comparable oldValue,
                         @Nullable final Comparable newValue) {
        final PropertyId propId = new PropertyId(id, propertyId);
        propsCache.remove(propId);
        new PropertyChangedHandleCheckerImpl(this, id, propertyId,
                oldValue, newValue, mutableCache(), mutatedInTxn).updateCache();
    }

    void linkAdded(@NotNull final PersistentEntityId sourceId,
                   @NotNull final PersistentEntityId targetId,
                   final int linkId) {
        final PropertyId propId = new PropertyId(sourceId, linkId);
        linksCache.remove(propId);
        new LinkAddedHandleChecker(this, sourceId, targetId, linkId, mutableCache(), mutatedInTxn).updateCache();
    }

    void linkDeleted(@NotNull final PersistentEntityId sourceId,
                     @NotNull final PersistentEntityId targetId,
                     final int linkId) {
        final PropertyId propId = new PropertyId(sourceId, linkId);
        linksCache.remove(propId);
        new LinkDeletedHandleChecker(this, sourceId, targetId, linkId, mutableCache(), mutatedInTxn).updateCache();
    }

    void addBlobStream(final long blobHandle, final long tmpHandle,
                       @NotNull Path tmpFilePath,
                       final boolean invalidateOnRollback) {
        LongHashMap<Triple<Path, Boolean, Long>> blobStreams = this.blobStreams;

        if (blobStreams == null) {
            blobStreams = new LongHashMap<>();
            this.blobStreams = blobStreams;
        }

        blobStreams.put(blobHandle, new Triple<>(tmpFilePath, Boolean.valueOf(invalidateOnRollback), tmpHandle));
    }

    void addBlobFile(final long blobHandle, @NotNull final Path file) {
        LongHashMap<Path> blobFiles = this.blobFiles;
        if (blobFiles == null) {
            blobFiles = new LongHashMap<>();
            this.blobFiles = blobFiles;
        }
        blobFiles.put(blobHandle, file);
    }

    void deleteBlob(final long blobHandle) {
        if (blobStreams != null) {
            final Triple<Path, Boolean, Long> pair = blobStreams.remove(blobHandle);

            if (pair != null) {
                closeOpenedBlobStreams(blobHandle);

                final Path path = pair.first;
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new ExodusException("Error during removal of blob " + path, e);
                }
            }
        }

        if (blobFiles != null) {
            blobFiles.remove(blobHandle);
        }
    }

    long getBlobSize(final long blobHandle) throws IOException {
        final LongHashMap<Triple<Path, Boolean, Long>> blobStreams = this.blobStreams;

        if (blobStreams != null) {
            final Triple<Path, Boolean, Long> streamTriple = blobStreams.get(blobHandle);
            if (streamTriple != null) {
                return Files.size(streamTriple.first);
            }
        }

        final LongHashMap<Path> blobFiles = this.blobFiles;
        if (blobFiles != null) {
            final Path file = blobFiles.get(blobHandle);
            if (file != null) {
                return Files.size(file);
            }
        }

        return -1;
    }

    @Nullable
    InputStream getBlobStream(final long blobHandle) throws IOException {
        final LongHashMap<Triple<Path, Boolean, Long>> blobStreams = this.blobStreams;

        InputStream result = null;
        if (blobStreams != null) {
            final Triple<Path, Boolean, Long> streamTriple = blobStreams.get(blobHandle);
            if (streamTriple != null) {
                result = ((DiskBasedBlobVault) store.getBlobVault()).openTmpStream(streamTriple.third,
                        streamTriple.first);
            }
        }

        if (result == null) {
            final LongHashMap<Path> blobFiles = this.blobFiles;
            if (blobFiles != null) {
                final Path file = blobFiles.get(blobHandle);
                if (file != null) {
                    result = store.getBlobVault().getFileStream(file.toFile());
                }
            }
        }

        if (result != null) {
            addOpenBlobStream(blobHandle, result);

            return new FilterInputStream(result) {
                @Override
                public void close() throws IOException {
                    super.close();

                    removeOpenBlobInputStream(blobHandle, in);
                }
            };
        }

        return null;
    }

    private void addOpenBlobStream(long blobHandle, InputStream stream) {
        openedBlobStreamsLock.lock();
        try {
            LongHashMap<ArrayList<InputStream>> openedBlobStreams = this.openedBlobStreams;
            if (openedBlobStreams == null) {
                openedBlobStreams = new LongHashMap<>();
                this.openedBlobStreams = openedBlobStreams;
            }

            ArrayList<InputStream> streams = openedBlobStreams.get(blobHandle);
            if (streams == null) {
                streams = new ArrayList<>();
                openedBlobStreams.put(blobHandle, streams);
            }

            streams.add(stream);
        } finally {
            openedBlobStreamsLock.unlock();
        }
    }

    private void removeOpenBlobInputStream(long blobHandle, InputStream stream) {
        if (this.openedBlobStreams != null) {
            openedBlobStreamsLock.lock();
            try {
                final LongHashMap<ArrayList<InputStream>> openedBlobStreams = this.openedBlobStreams;
                if (openedBlobStreams != null) {
                    final ArrayList<InputStream> streams = openedBlobStreams.get(blobHandle);
                    if (streams != null) {
                        streams.remove(stream);

                        if (streams.isEmpty()) {
                            openedBlobStreams.remove(blobHandle);
                        }
                    }
                }
            } finally {
                openedBlobStreamsLock.unlock();
            }
        }
    }

    void deferBlobDeletion(final long blobHandle) {
        if (deferredBlobsToDelete == null) {
            deferredBlobsToDelete = new LongHashSet();
        }
        deferredBlobsToDelete.add(blobHandle);
    }

    public void checkInvalidateBlobsFlag() {
        checkInvalidateBlobsFlag = true;
    }

    void closeCaches() {
        propsCache.close();
        linksCache.close();
        blobStringsCache.close();
    }

    private PersistentCacheClient cacheClient = null;

    private void initCaches() {
        if (mutableCache != null) {
            // Mutable cache might not be null in case of transaction being reverted
            mutableCache.release();
        }
        resetCaches(false);
        if (cacheClient == null) {
            cacheClient = localCache.registerClient();
        }
    }

    public void flushCaches(final boolean clearPropsAndLinksCache) {
        resetCaches(clearPropsAndLinksCache);
    }

    public void revertCaches() {
        if (mutableCache != null) {
            mutableCache.release();
        }
        resetCaches(true);
    }

    private void resetCaches(final boolean clearPropsAndLinksCache) {
        if (clearPropsAndLinksCache) {
            propsCache.clear();
            linksCache.clear();
            blobStringsCache.clear();
        }

        if (cacheClient != null) {
            cacheClient.unregister();
            cacheClient = null;
        }
        localCache = (EntityIterableCacheAdapter) store.getEntityIterableCache().getCacheAdapter();
        mutableCache = null;
        mutatedInTxn = null;

        try {
            closeOpenedBlobStreams();

            if (blobStreams != null && !blobStreams.isEmpty()) {
                for (final Triple<Path, Boolean, Long> streamTriple : blobStreams.values()) {
                    try {
                        if (!checkInvalidateBlobsFlag || streamTriple.second.booleanValue()) {
                            final Path path = streamTriple.first;
                            Files.deleteIfExists(path);
                        }
                    } catch (IOException e) {
                        throw new ExodusException("Can not remove temporary blob " + streamTriple + " during rollback.", e);
                    }
                }
            }
        } finally {
            checkInvalidateBlobsFlag = false;
        }

        blobStreams = null;
        blobFiles = null;
        deferredBlobsToDelete = null;
    }

    // exposed only for tests
    void apply() {
        final FlushLog log = new FlushLog();
        store.logOperations(txn, log);
        final BlobVault blobVault = store.getBlobVault();
        if (blobVault.requiresTxn()) {
            try {
                flushBlobs(blobVault);
            } catch (Exception e) {
                // out of disk space not expected there
                throw ExodusException.toEntityStoreException(e);
            }
        }

        txn.setCommitHook(() -> {
            log.flushed();
            final EntityIterableCacheAdapterMutable cache = this.mutableCache;
            if (cache != null) { // mutableCache can be null if only blobs are modified
                applyAtomicCaches(cache);
            }
        });
    }

    private void closeOpenedBlobStreams() {
        if (openedBlobStreams != null) {
            int closed = 0;
            openedBlobStreamsLock.lock();
            try {
                final LongHashMap<ArrayList<InputStream>> openedBlobStreams = this.openedBlobStreams;
                if (openedBlobStreams != null) {
                    for (ArrayList<InputStream> streams : openedBlobStreams.values()) {
                        for (InputStream stream : streams) {
                            try {
                                stream.close();
                                closed++;
                            } catch (IOException e) {
                                logger.error("Error during closing of stream acquired from blob", e);
                            }
                        }
                    }

                }

                this.openedBlobStreams = null;
            } finally {
                openedBlobStreamsLock.unlock();
            }
            if (closed > 0) {
                logger.warn("There are " + closed + " streams left open after reading of blobs.");
            }
        }
    }

    private void closeOpenedBlobStreams(long blobHandle) {
        if (openedBlobStreams != null) {
            openedBlobStreamsLock.lock();
            try {
                final LongHashMap<ArrayList<InputStream>> openedBlobStreams = this.openedBlobStreams;
                if (openedBlobStreams != null) {
                    ArrayList<InputStream> streams = openedBlobStreams.remove(blobHandle);
                    if (streams != null) {
                        for (InputStream stream : streams) {
                            try {
                                stream.close();
                            } catch (IOException e) {
                                logger.error("Error during closing of stream acquired from blob", e);
                            }
                        }
                    }
                }
            } finally {
                openedBlobStreamsLock.unlock();
            }
        }
    }

    private void flushBlobs(final BlobVault blobVault) throws Exception {
        if (blobStreams != null || blobFiles != null) {
            closeOpenedBlobStreams();

            final LongHashMap<Path> tmpBlobFiles;
            final LongHashMap<InputStream> tmpStreams;

            final ArrayList<Path> filesToDelete = new ArrayList<>();

            if (blobStreams != null) {
                tmpBlobFiles = new LongHashMap<>();
                tmpStreams = new LongHashMap<>();


                boolean encryptedStorage = blobVault instanceof EncryptedBlobVault;

                for (final Map.Entry<Long, Triple<Path, Boolean, Long>> entry : blobStreams.entrySet()) {
                    long blobHandle = entry.getKey();
                    Triple<Path, Boolean, Long> triple = entry.getValue();
                    long tmpBlobHandle = triple.third;

                    if (!encryptedStorage || blobHandle == tmpBlobHandle) {
                        tmpBlobFiles.put(entry.getKey(), entry.getValue().first);
                    } else {
                        tmpStreams.put(blobHandle, ((DiskBasedBlobVault) blobVault).openTmpStream(tmpBlobHandle, triple.first));
                        filesToDelete.add(triple.first);
                    }
                }
            } else {
                tmpBlobFiles = null;
                tmpStreams = null;
            }

            blobVault.flushBlobs(tmpStreams, blobFiles, tmpBlobFiles, deferredBlobsToDelete, store.getEnvironment());

            for (final Path fileToDelete : filesToDelete) {
                Files.deleteIfExists(fileToDelete);
            }

            blobStreams = null;
            blobFiles = null;
        }
    }

    private void applyAtomicCaches(@NotNull EntityIterableCacheAdapterMutable mutableCache) {
        final EntityIterableCache entityIterableCache = store.getEntityIterableCache();
        for (final Updatable it : mutatedInTxn) {
            it.endUpdate(PersistentStoreTransaction.this);
        }
        EntityIterableCacheAdapter oldCache = localCache;
        if (!entityIterableCache.compareAndSetCacheAdapter(localCache, mutableCache.endWrite())) {
            throw new EntityStoreException("This exception should never be thrown");
        }
        oldCache.release();
    }

    private void applyExclusiveTransactionCaches() {
        final EntityIterableCacheAdapterMutable cache = this.mutableCache;
        if (cache != null) {
            UnsafeKt.executeInMetaWriteLock((EnvironmentImpl) store.getEnvironment(), () -> {
                applyAtomicCaches(cache);
                return null;
            });
        }
    }

    private Runnable getInitCachesBeginHook() {
        return this::initCaches;
    }

    private void flushNonTransactionalBlobs() {
        final BlobVault blobVault = store.getBlobVault();

        if (blobStreams != null || blobFiles != null || deferredBlobsToDelete != null) {
            ((EnvironmentImpl) txn.getEnvironment()).flushAndSync();
            try {
                flushBlobs(blobVault);
            } catch (Exception e) {
                handleOutOfDiskSpace(e);
                throw ExodusException.toEntityStoreException(e);
            }
        }

    }

    private EntityIterableCacheAdapterMutable mutableCache() {
        EntityIterableCacheAdapterMutable cache = mutableCache;
        if (mutableCache == null) {
            cache = createMutableCache();
            mutableCache = cache; // preemptive version mismatch disabled
        }
        return cache;
    }

    private EntityIterableCacheAdapterMutable createMutableCache() {
        final EntityIterableCacheAdapterMutable result = localCache.cloneToMutable();
        mutatedInTxn = new ArrayList<>();
        return result;
    }

    private void handleOutOfDiskSpace(final Exception e) {
        if (e instanceof IOException && store.getUsableSpace() < 4096) {
            throw new OutOfDiskSpaceException(e);
        }
    }

    private EntityIterableBase getPropertyIterable(@NotNull final String entityType,
                                                   @NotNull final String propertyName,
                                                   @NotNull final BiFunction<Integer, Integer, EntityIterableBase> instantiator) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0) {
            return EntityIterableBase.EMPTY;
        }

        return instantiator.apply(Integer.valueOf(entityTypeId), Integer.valueOf(propertyId));
    }

    @SuppressWarnings("unchecked")
    public static <T> T getUpdatable(@NotNull final HandleChecker handleChecker,
                                     @NotNull final EntityIterableHandle handle,
                                     @NotNull final Class<T> handleType) {
        final HandleCheckerAdapter checker = (HandleCheckerAdapter) handleChecker;
        Updatable instance = checker.get(handle);
        if (instance != null) {
            if (!instance.isMutated()) {
                instance = instance.beginUpdate(checker.txn);
                if (handleType.isAssignableFrom(instance.getClass())) {
                    checker.beginUpdate(instance);
                    return (T) instance;
                }
            } else if (handleType.isAssignableFrom(instance.getClass())) {
                return (T) instance;
            }
            checker.remove(handle);
            if (logger.isErrorEnabled()) {
                final String handlePart;
                if (instance instanceof EntityIterableBase) {
                    handlePart = ", handle = " + ((EntityIterableBase) instance).getHandle();
                } else {
                    handlePart = "";
                }
                logger.error("Iterable doesn't match expected class " + handleType.getName()
                        + ", handle = " + handle + ", found = " + instance.getClass().getName() + handlePart);
            }
        }
        return null;
    }

    private <V> ObjectCacheBase<PropertyId, V> createObjectCache(final int size) {
        return size == 0 ?
                new FakeObjectCache<>() :
                new TransactionObjectCache<>(size);
    }

    abstract static class HandleCheckerAdapter implements HandleChecker {
        @NotNull
        final PersistentStoreTransaction txn;
        @NotNull
        final List<Updatable> mutatedInTxn;
        @NotNull
        final EntityIterableCacheAdapterMutable mutableCache;

        HandleCheckerAdapter(@NotNull PersistentStoreTransaction txn,
                             @NotNull List<Updatable> mutatedInTxn,
                             @NotNull EntityIterableCacheAdapterMutable mutableCache) {
            this.txn = txn;
            this.mutatedInTxn = mutatedInTxn;
            this.mutableCache = mutableCache;
        }

        @Override
        public int getLinkId() {
            return -1;
        }

        public int getPropertyId() {
            return -1;
        }

        public int getTypeId() {
            return -1;
        }

        public int getTypeIdAffectingCreation() {
            return -1;
        }

        @Override
        @NotNull
        public PersistentStoreTransaction getTxn() {
            return txn;
        }

        public void beginUpdate(@NotNull Updatable instance) {
            mutatedInTxn.add(instance);
        }

        void updateCache() {
            mutableCache.update(this);
        }

        abstract boolean checkHandle(@NotNull final EntityIterableHandle handle);

        Updatable get(@NotNull EntityIterableHandle handle) {
            return mutableCache.getUpdatable(handle);
        }

        void remove(@NotNull EntityIterableHandle handle) {
            mutableCache.remove(handle);
        }

        @Override
        @Deprecated
        public Updatable getUpdatableIterable(@NotNull EntityIterableHandle handle) {
            return PersistentStoreTransaction.getUpdatable(this, handle, Updatable.class);
        }
    }

    private static class PropertyId {

        @NotNull
        private final PersistentEntityId entityId;
        private final int propId;

        private PropertyId(@NotNull final PersistentEntityId entityId, final int propId) {
            this.entityId = entityId;
            this.propId = propId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            final PropertyId propertyId = (PropertyId) obj;
            return propId == propertyId.propId && entityId.equals(propertyId.entityId);
        }

        @Override
        public int hashCode() {
            return 31 * entityId.hashCode() + propId;
        }
    }

    private abstract static class EntityAddedOrDeletedHandleCheckerAdapter extends HandleCheckerAdapter implements EntityAddedOrDeletedHandleChecker {

        protected final EntityId id;

        EntityAddedOrDeletedHandleCheckerAdapter(@NotNull PersistentStoreTransaction txn,
                                                 @NotNull final EntityId id,
                                                 @NotNull List<Updatable> mutatedInTxn,
                                                 @NotNull EntityIterableCacheAdapterMutable mutableCache) {
            super(txn, mutatedInTxn, mutableCache);
            this.id = id;
        }

        @Override
        public EntityId getId() {
            return id;
        }

        @Override
        public int getTypeIdAffectingCreation() {
            return id.getTypeId();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            EntityAddedOrDeletedHandleCheckerAdapter that = (EntityAddedOrDeletedHandleCheckerAdapter) obj;

            return id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private static class EntityDeletedHandleCheckerImpl extends EntityAddedOrDeletedHandleCheckerAdapter {

        private EntityDeletedHandleCheckerImpl(@NotNull PersistentStoreTransaction txn,
                                               @NotNull EntityId id,
                                               @NotNull EntityIterableCacheAdapterMutable mutatedInTxn,
                                               @NotNull List<Updatable> mutableCache) {
            super(txn, id, mutableCache, mutatedInTxn);
        }

        @Override
        public int getTypeId() {
            return id.getTypeId();
        }

        @Override
        public boolean checkHandle(@NotNull final EntityIterableHandle handle) {
            return handle.isMatchedEntityDeleted(id)
                    && !handle.onEntityDeleted(this);
        }
    }

    private static class EntityAddedHandleCheckerImpl extends EntityAddedOrDeletedHandleCheckerAdapter {

        private EntityAddedHandleCheckerImpl(@NotNull PersistentStoreTransaction txn,
                                             @NotNull EntityId id,
                                             @NotNull EntityIterableCacheAdapterMutable mutableCache,
                                             @NotNull List<Updatable> mutatedInTxn) {
            super(txn, id, mutatedInTxn, mutableCache);
        }

        @Override
        public boolean checkHandle(@NotNull final EntityIterableHandle handle) {
            return handle.isMatchedEntityAdded(id)
                    && !handle.onEntityAdded(this);
        }
    }

    private abstract static class LinkChangedHandleCheckerImpl extends HandleCheckerAdapter implements LinkChangedHandleChecker {
        @NotNull
        final PersistentEntityId sourceId;
        @NotNull
        final PersistentEntityId targetId;
        protected final int linkId;

        private LinkChangedHandleCheckerImpl(@NotNull PersistentStoreTransaction txn,
                                             @NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId,
                                             int linkId, @NotNull List<Updatable> mutatedInTxn,
                                             @NotNull EntityIterableCacheAdapterMutable mutableCache) {
            super(txn, mutatedInTxn, mutableCache);
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.linkId = linkId;
        }

        @Override
        public int getLinkId() {
            return linkId;
        }

        @Override
        public EntityId getSourceId() {
            return sourceId;
        }

        @Override
        public EntityId getTargetId() {
            return targetId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            LinkChangedHandleCheckerImpl that = (LinkChangedHandleCheckerImpl) obj;

            if (linkId != that.linkId) return false;
            //noinspection SimplifiableIfStatement
            if (!sourceId.equals(that.sourceId)) return false;
            return targetId.equals(that.targetId);
        }

        @Override
        public int hashCode() {
            int result = sourceId.hashCode();
            result = 31 * result + targetId.hashCode();
            result = 31 * result + linkId;
            return result;
        }
    }

    private static final class LinkAddedHandleChecker extends LinkChangedHandleCheckerImpl {

        private LinkAddedHandleChecker(@NotNull PersistentStoreTransaction txn,
                                       @NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId,
                                       int linkId, @NotNull EntityIterableCacheAdapterMutable mutatedInTxn,
                                       @NotNull List<Updatable> mutableCache) {
            super(txn, sourceId, targetId, linkId, mutableCache, mutatedInTxn);
        }

        @Override
        public boolean checkHandle(@NotNull EntityIterableHandle handle) {
            return handle.isMatchedLinkAdded(sourceId, targetId, linkId)
                    && !handle.onLinkAdded(this);
        }
    }

    private static final class LinkDeletedHandleChecker extends LinkChangedHandleCheckerImpl {

        private LinkDeletedHandleChecker(@NotNull PersistentStoreTransaction txn,
                                         @NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId,
                                         int linkId, @NotNull EntityIterableCacheAdapterMutable mutatedInTxn,
                                         @NotNull List<Updatable> mutableCache) {
            super(txn, sourceId, targetId, linkId, mutableCache, mutatedInTxn);
        }

        @Override
        public boolean checkHandle(@NotNull EntityIterableHandle handle) {
            return handle.isMatchedLinkDeleted(sourceId, targetId, linkId)
                    && !handle.onLinkDeleted(this);
        }
    }

    private static final class PropertyChangedHandleCheckerImpl extends HandleCheckerAdapter implements PropertyChangedHandleChecker {

        private final EntityId id;
        private final int propertyId;
        @Nullable
        private final Comparable oldValue;
        @Nullable
        private final Comparable newValue;

        private PropertyChangedHandleCheckerImpl(@NotNull PersistentStoreTransaction txn,
                                                 EntityId id, int propertyId,
                                                 @Nullable Comparable oldValue, @Nullable Comparable newValue,
                                                 @NotNull EntityIterableCacheAdapterMutable mutatedInTxn,
                                                 @NotNull List<Updatable> mutableCache) {
            super(txn, mutableCache, mutatedInTxn);
            if (oldValue == null && newValue == null) {
                throw new IllegalArgumentException("Either oldValue or newValue should be not null");
            }
            this.id = id;
            this.propertyId = propertyId;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        @Override
        public int getPropertyId() {
            return propertyId;
        }

        @Override
        public int getTypeId() {
            return id.getTypeId();
        }

        @Override
        public long getLocalId() {
            return id.getLocalId();
        }

        @Nullable
        @Override
        public Comparable getOldValue() {
            return oldValue;
        }

        @Nullable
        @Override
        public Comparable getNewValue() {
            return newValue;
        }

        @Override
        public boolean checkHandle(@NotNull EntityIterableHandle handle) {
            return handle.isMatchedPropertyChanged(id, propertyId, oldValue, newValue)
                    && !handle.onPropertyChanged(this);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            PropertyChangedHandleCheckerImpl that = (PropertyChangedHandleCheckerImpl) obj;

            if (propertyId != that.propertyId) return false;
            if (!id.equals(that.id)) return false;
            //noinspection SimplifiableIfStatement
            if (!Objects.equals(newValue, that.newValue)) return false;
            return Objects.equals(oldValue, that.oldValue);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + propertyId;
            result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
            result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
            return result;
        }
    }

    private final class TransactionObjectCache<V> extends ObjectCacheDecorator<PropertyId, V> {

        private TransactionObjectCache(final int size) {
            super(size, () -> !((TransactionBase) txn).isDisableStoreGetCache());
        }

        @Override
        protected ObjectCacheBase<PropertyId, V> createdDecorated() {
            return new NonAdjustableConcurrentObjectCache<>(size(), LOCAL_CACHE_GENERATIONS);
        }
    }
}
