/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.FakeObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.ObjectCacheDecorator;
import jetbrains.exodus.core.dataStructures.decorators.HashSetDecorator;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.execution.SharedTimer;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.StringBuilderSpinAllocator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"RawUseOfParameterizedType", "rawtypes"})
public class PersistentStoreTransaction implements StoreTransaction, TxnGetterStrategy, TxnProvider {
    private static final Logger logger = LoggerFactory.getLogger(PersistentStoreTransaction.class);

    enum TransactionType {
        Regular,
        Exclusive,
        Readonly
    }

    @NotNull
    private static final ByteIterable ZERO_VERSION_ENTRY = IntegerBinding.intToCompressedEntry(0);

    @NotNull
    protected final PersistentEntityStoreImpl store;
    @NotNull
    protected final Transaction txn;
    @NotNull
    private final Set<EntityIterator> createdIterators;
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
    private LongHashMap<InputStream> blobStreams;
    @Nullable
    private LongHashMap<File> blobFiles;
    @Nullable
    private LongSet preservedBlobs;
    private LongSet deferredBlobsToDelete;
    private QueryCancellingPolicy queryCancellingPolicy;

    PersistentStoreTransaction(@NotNull final PersistentEntityStoreImpl store) {
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
        createdIterators = new HashSetDecorator<>();
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        localCache = source.localCache;
        localCacheAttempts = localCacheHits = 0;
        switch (txnType) {
            case Regular:
                txn = source.txn.getSnapshot(getRevertCachesBeginHook());
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
        createdIterators = new HashSetDecorator<>();
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        localCacheAttempts = localCacheHits = 0;
        final Runnable beginHook = getRevertCachesBeginHook();
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
            revertCaches();
            return true;
        }
        revert();
        return false;
    }

    @Override
    public void abort() {
        try {
            disposeCreatedIterators();
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
        }
        return true;
    }

    // exposed only for tests
    boolean doFlush() {
        if (txn.flush()) {
            flushNonTransactionalBlobs();
            revertCaches(false); // do not clear props & links caches
            return true;
        }
        revert();
        return false;
    }

    @Override
    public void revert() {
        disposeCreatedIterators();
        txn.revert();
        mutableCache = null;
        mutatedInTxn = new ArrayList<>();
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
            store.getEntitiesTable(this, entityTypeId).putRight(
                    txn, LongBinding.longToCompressedEntry(entityLocalId), ZERO_VERSION_ENTRY);
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
            final Store entitiesTable = store.getEntitiesTable(this, entityId.getTypeId());
            entitiesTable.put(txn, LongBinding.longToCompressedEntry(entityId.getLocalId()), ZERO_VERSION_ENTRY);
            new EntityAddedHandleCheckerImpl(this, entityId, mutableCache(), mutatedInTxn).updateCache();
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    @NotNull
    public PersistentEntity getEntity(@NotNull final EntityId id) {
        final int version = store.getLastVersion(this, id);
        if (version < 0) {
            throw new EntityRemovedInDatabaseException(store.getEntityType(this, id.getTypeId()));
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
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesOfTypeIterable(this, entityTypeId);
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
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new PropertyValueIterable(this, entityTypeId, propertyId, value);
    }

    @Override
    @NotNull
    public EntityIterable find(@NotNull final String entityType, @NotNull final String propertyName,
                               @NotNull final Comparable minValue, @NotNull final Comparable maxValue) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new PropertyRangeIterable(this, entityTypeId, propertyId, minValue, maxValue);
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
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesWithPropertyIterable(this, entityTypeId, propertyId);
    }

    public EntityIterableBase findWithPropSortedByValue(@NotNull final String entityType, @NotNull final String propertyName) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int propertyId = store.getPropertyId(this, propertyName, false);
        if (propertyId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new PropertiesIterable(this, entityTypeId, propertyId);
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
        return find(entityType, propertyName, value, value + Character.MAX_VALUE);
    }

    @NotNull
    @Override
    public EntityIterable findWithBlob(@NotNull final String entityType, @NotNull final String blobName) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int blobId = store.getPropertyId(this, blobName, false);
        if (blobId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesWithBlobIterable(this, entityTypeId, blobId);
    }

    @Override
    @NotNull
    public EntityIterable findLinks(@NotNull final String entityType,
                                    @NotNull final Entity entity,
                                    @NotNull final String linkName) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int linkId = store.getLinkId(this, linkName, false);
        if (linkId < 0) {
            return EntityIterableBase.EMPTY;
        }
        if (entity instanceof PersistentEntity) {
            return new EntityToLinksIterable(this, ((PersistentEntity) entity).getId(), entityTypeId, linkId);
        }
        EntityId id = entity.getId();
        if (id instanceof PersistentEntityId) {
            return new EntityToLinksIterable(this, id, entityTypeId, linkId);
        }
        return EntityIterableBase.EMPTY;
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
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int linkId = store.getLinkId(this, linkName, false);
        if (linkId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesWithLinkIterable(this, entityTypeId, linkId);
    }

    @Override
    @NotNull
    public EntityIterable findWithLinks(@NotNull final String entityType,
                                        @NotNull final String linkName,
                                        @NotNull final String oppositeEntityType,
                                        @NotNull final String oppositeLinkName) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int linkId = store.getLinkId(this, linkName, false);
        if (linkId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int oppositeEntityId = store.getEntityTypeId(this, oppositeEntityType, false);
        if (oppositeEntityId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int oppositeLinkId = store.getLinkId(this, oppositeLinkName, false);
        if (oppositeLinkId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesWithLinkSortedIterable(this, entityTypeId, linkId, oppositeEntityId, oppositeLinkId);
    }

    @Override
    @NotNull
    public EntityIterable sort(@NotNull final String entityType,
                               @NotNull final String propertyName,
                               final boolean ascending) {
        return sort(entityType, propertyName, getAll(entityType), ascending);
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
                ((EntityIterableBase) sortedLinks).getSource(), linkName, (EntityIterableBase) rightOrder, null, null);
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
        return filtered == null ? EntityIterableBase.EMPTY : new MergeSortedIterableWithValueGetter(this, filtered, valueGetter, comparator);
    }

    @Override
    @NotNull
    public EntityId toEntityId(@NotNull final String representation) {
        return PersistentEntityId.toEntityId(representation, store);
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
        // cache new mutated iterable instance not affecting HandlesDistribution
        cache.cacheObjectNotAffectingHandleDistribution(handle, iterable);
    }

    public void registerStickyObject(@NotNull final EntityIterableHandle handle, Updatable object) {
        mutableCache().registerStickyObject(handle, object);
    }

    public Updatable getStickyObject(@NotNull final EntityIterableHandle handle) {
        return getLocalCache().getStickyObject(handle);
    }

    @SuppressWarnings({"HardCodedStringLiteral"})
    public String toString() {
        final StringBuilder builder = StringBuilderSpinAllocator.alloc();
        try {
            builder.append(store.getLocation());
            builder.append(", thread = ");
            builder.append(Thread.currentThread().toString());
            return builder.toString();
        } finally {
            StringBuilderSpinAllocator.dispose(builder);
        }
    }

    public void registerEntityIterator(@NotNull final EntityIterator iterator) {
        if (!txn.isIdempotent()) {
            createdIterators.add(iterator);
        }
    }

    public void deregisterEntityIterator(@NotNull final EntityIterator iterator) {
        if (!createdIterators.isEmpty()) {
            createdIterators.remove(iterator);
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
        return localCache.tryKey(handle);
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

    void disposeCreatedIterators() {
        // dispose opened entity iterables
        final EntityIterator[] copy = createdIterators.toArray(new EntityIterator[createdIterators.size()]);
        createdIterators.clear();
        for (final EntityIterator iterator : copy) {
            iterator.dispose();
        }
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
        new PropertyChangedHandleCheckerImpl(this, id.getTypeId(), id.getLocalId(), propertyId,
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

    void addBlob(final long blobHandle, @NotNull final InputStream stream) throws IOException {
        LongHashMap<InputStream> blobStreams = this.blobStreams;
        if (blobStreams == null) {
            blobStreams = new LongHashMap<>();
            this.blobStreams = blobStreams;
        }
        if (!stream.markSupported()) {
            throw new EntityStoreException("Blob input stream should support the mark and reset methods");
        }
        stream.mark(Integer.MAX_VALUE);
        blobStreams.put(blobHandle, stream);
    }

    void addBlob(final long blobHandle, @NotNull final File file) {
        LongHashMap<File> blobFiles = this.blobFiles;
        if (blobFiles == null) {
            blobFiles = new LongHashMap<>();
            this.blobFiles = blobFiles;
        }
        blobFiles.put(blobHandle, file);
    }

    void deleteBlob(final long blobHandle) {
        if (blobStreams != null) {
            blobStreams.remove(blobHandle);
        }
        if (blobFiles != null) {
            blobFiles.remove(blobHandle);
        }
    }

    long getBlobSize(final long blobHandle) throws IOException {
        final LongHashMap<InputStream> blobStreams = this.blobStreams;
        if (blobStreams != null) {
            final InputStream stream = blobStreams.get(blobHandle);
            if (stream != null) {
                stream.reset();
                return stream.skip(Long.MAX_VALUE); // warning, this may return inaccurate results
            }
        }
        final LongHashMap<File> blobFiles = this.blobFiles;
        if (blobFiles != null) {
            final File file = blobFiles.get(blobHandle);
            if (file != null) {
                return file.length();
            }
        }
        return -1;
    }

    @Nullable
    InputStream getBlobStream(final long blobHandle) throws IOException {
        final LongHashMap<InputStream> blobStreams = this.blobStreams;
        if (blobStreams != null) {
            final InputStream stream = blobStreams.get(blobHandle);
            if (stream != null) {
                stream.reset();
                return stream;
            }
        }
        final LongHashMap<File> blobFiles = this.blobFiles;
        if (blobFiles != null) {
            final File file = blobFiles.get(blobHandle);
            if (file != null) {
                return store.getBlobVault().cloneStream(new FileInputStream(file), true);
            }
        }
        return null;
    }

    void preserveBlob(final long blobHandle) {
        if (preservedBlobs == null) {
            preservedBlobs = new LongHashSet();
        }
        preservedBlobs.add(blobHandle);
    }

    boolean isBlobPreserved(final long blobHandle) {
        return preservedBlobs != null && preservedBlobs.contains(blobHandle);
    }

    void deferBlobDeletion(final long blobHandle) {
        if (deferredBlobsToDelete == null) {
            deferredBlobsToDelete = new LongHashSet();
        }
        deferredBlobsToDelete.add(blobHandle);
    }

    void closeCaches() {
        propsCache.close();
        linksCache.close();
        blobStringsCache.close();
    }

    void revertCaches() {
        revertCaches(true);
    }

    private void revertCaches(final boolean clearPropsAndLinksCache) {
        if (clearPropsAndLinksCache) {
            propsCache.clear();
            linksCache.clear();
            blobStringsCache.clear();
        }
        localCache = store.getEntityIterableCache().getCacheAdapter();
        mutableCache = null;
        mutatedInTxn = null;
        blobStreams = null;
        blobFiles = null;
        preservedBlobs = null;
        deferredBlobsToDelete = null;
    }

    // exposed only for tests
    void apply() {
        disposeCreatedIterators();
        final FlushLog log = new FlushLog();
        store.logOperations(txn, log);
        final BlobVault blobVault = store.getBlobVault();
        if (blobVault.requiresTxn()) {
            try {
                blobVault.flushBlobs(blobStreams, blobFiles, deferredBlobsToDelete, txn);
            } catch (Exception e) {
                // out of disk space not expected there
                throw ExodusException.toEntityStoreException(e);
            }
        }
        txn.setCommitHook(new Runnable() {
            @Override
            public void run() {
                log.flushed();
                if (mutableCache != null) { // mutableCache can be null if only blobs are modified
                    final EntityIterableCache entityIterableCache = store.getEntityIterableCache();
                    for (final Updatable it : mutatedInTxn) {
                        it.endUpdate(PersistentStoreTransaction.this);
                    }
                    if (!entityIterableCache.compareAndSetCacheAdapter(localCache, mutableCache.endWrite())) {
                        throw new EntityStoreException("This exception should never be thrown");
                    }
                }
            }
        });
    }

    private Runnable getRevertCachesBeginHook() {
        return new Runnable() {
            @Override
            public void run() {
                revertCaches();
            }
        };
    }

    private void flushNonTransactionalBlobs() {
        final BlobVault blobVault = store.getBlobVault();
        if (!blobVault.requiresTxn()) {
            if (blobStreams != null || blobFiles != null || deferredBlobsToDelete != null) {
                ((EnvironmentImpl) txn.getEnvironment()).flushAndSync();
                try {
                    blobVault.flushBlobs(blobStreams, blobFiles, deferredBlobsToDelete, txn);
                } catch (Exception e) {
                    handleOutOfDiskSpace(e);
                    throw ExodusException.toEntityStoreException(e);
                }
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
        final EntityIterableCacheAdapterMutable result = localCache.getClone();
        mutatedInTxn = new ArrayList<>();
        return result;
    }

    private void handleOutOfDiskSpace(final Exception e) {
        if (e instanceof IOException && store.getUsableSpace() < 4096) {
            throw new OutOfDiskSpaceException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getUpdatable(
            @NotNull final HandleChecker handleChecker, @NotNull final EntityIterableHandle handle, @NotNull final Class<T> handleType
    ) {
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

    private static <V> ObjectCacheBase<PropertyId, V> createObjectCache(final int size) {
        return size == 0 ?
                new FakeObjectCache<PropertyId, V>() :
                new TransactionObjectCache<V>(size);
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

        private final int entityTypeId;
        private final long localId;
        private final int propertyId;
        @Nullable
        private final Comparable oldValue;
        @Nullable
        private final Comparable newValue;

        private PropertyChangedHandleCheckerImpl(@NotNull PersistentStoreTransaction txn,
                                                 int entityTypeId, long localId, int propertyId,
                                                 @Nullable Comparable oldValue, @Nullable Comparable newValue,
                                                 @NotNull EntityIterableCacheAdapterMutable mutatedInTxn,
                                                 @NotNull List<Updatable> mutableCache) {
            super(txn, mutableCache, mutatedInTxn);
            if (oldValue == null && newValue == null) {
                throw new IllegalArgumentException("Either oldValue or newValue should be not null");
            }
            this.entityTypeId = entityTypeId;
            this.localId = localId;
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
            return entityTypeId;
        }

        @Override
        public long getLocalId() {
            return localId;
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
            return handle.isMatchedPropertyChanged(entityTypeId, propertyId, oldValue, newValue)
                    && !handle.onPropertyChanged(this);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            PropertyChangedHandleCheckerImpl that = (PropertyChangedHandleCheckerImpl) obj;

            if (propertyId != that.propertyId) return false;
            if (entityTypeId != that.entityTypeId) return false;
            //noinspection SimplifiableIfStatement
            if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
            return !(oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null);
        }

        @Override
        public int hashCode() {
            int result = entityTypeId;
            result = 31 * result + propertyId;
            result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
            result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
            return result;
        }
    }

    private static final class TransactionObjectCache<V> extends ObjectCacheDecorator<PropertyId, V> {

        private TransactionObjectCache(final int size) {
            super(size);
        }

        @Override
        protected ObjectCacheBase<PropertyId, V> createdDecorated() {
            return new ConcurrentObjectCache<PropertyId, V>(size()) {
                @Nullable
                @Override
                protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
                    return null;
                }
            };
        }
    }
}
