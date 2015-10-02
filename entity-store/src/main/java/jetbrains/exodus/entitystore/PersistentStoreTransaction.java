/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.FakeObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.ObjectCacheDecorator;
import jetbrains.exodus.core.dataStructures.decorators.HashSetDecorator;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.entitystore.metadata.Index;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.StringBuilderSpinAllocator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"RawUseOfParameterizedType", "rawtypes"})
public class PersistentStoreTransaction implements StoreTransaction, TxnGetterStategy {

    private static final int COPY_CACHED_VALUES = 50;

    @NotNull
    public static final ByteIterable ZERO_VERSION_ENTRY = IntegerBinding.intToCompressedEntry(0);
    @NotNull
    protected final PersistentEntityStoreImpl store;
    @NotNull
    protected final Transaction txn;
    @NotNull
    protected final Set<EntityIterator> createdIterators;
    private final ObjectCacheBase<PropertyId, Comparable> propsCache;
    @NotNull
    private final ObjectCacheBase<PropertyId, PersistentEntityId> linksCache;
    @NotNull
    private final ObjectCacheBase<PropertyId, String> blobStringsCache;
    @NotNull
    private EntityIterableCacheAdapter localCache;
    private int localCacheAttempts;
    private int localCacheHits;
    @Nullable
    private EntityIterableCacheAdapter mutableCache;
    private List<UpdatableCachedWrapperIterable> mutatedInTxn;
    @Nullable
    private LongHashMap<InputStream> blobStreams;
    @Nullable
    private LongHashMap<File> blobFiles;
    @Nullable
    private LongSet preservedBlobs;
    private LongSet deferredBlobsToDelete;
    private QueryCancellingPolicy queryCancellingPolicy;
    @Nullable
    ReplayData replayData;

    PersistentStoreTransaction(@NotNull final PersistentEntityStoreImpl store) {
        this(store, false);
    }

    PersistentStoreTransaction(@NotNull final PersistentStoreTransaction source,
                               @NotNull final Transaction txn) {
        this.store = source.store;
        this.txn = txn;
        createdIterators = new HashSetDecorator<>();
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        localCache = source.localCache;
        localCacheAttempts = localCacheHits = 0;
    }

    protected PersistentStoreTransaction(@NotNull final PersistentEntityStoreImpl store, final boolean readOnly) {
        this.store = store;
        createdIterators = new HashSetDecorator<>();
        final PersistentEntityStoreConfig config = store.getConfig();
        propsCache = createObjectCache(config.getTransactionPropsCacheSize());
        linksCache = createObjectCache(config.getTransactionLinksCacheSize());
        blobStringsCache = createObjectCache(config.getTransactionBlobStringsCacheSize());
        final Runnable beginHook = new Runnable() {
            @Override
            public void run() {
                revertCaches();
            }
        };
        final Environment env = store.getEnvironment();
        txn = readOnly ? env.beginReadonlyTransaction(beginHook) : env.beginTransaction(beginHook);
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
    public boolean commit() {
        // txn can be read-only if Environment is in read-only mode
        if (!txn.isReadonly()) {
            apply();
            return doCommit();
        }
        return true;
    }

    public boolean isCurrent() {
        return true;
    }

    // exposed only for tests
    boolean doCommit() {
        boolean reverted = false;
        try {
            if (txn.commit()) {
                store.unregisterTransaction(this);
                flushNonTransactionalBlobs();
                return true;
            }
            revert();
            reverted = true;
            return false;
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        } finally {
            if (!reverted) {
                revertCaches();
            }
        }
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
        if (!txn.isReadonly()) {
            apply();
            return doFlush();
        }
        return true;
    }

    // exposed only for tests
    boolean doFlush() {
        boolean reverted = false;
        try {
            if (txn.flush()) {
                flushNonTransactionalBlobs();
                return true;
            }
            revert();
            reverted = true;
            return false;
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        } finally {
            if (!reverted) {
                revertCaches(false); // do not clear props & links caches
            }
        }
    }

    @Override
    public void revert() {
        disposeCreatedIterators();
        txn.revert();
        if (replayData != null) {
            if (mutableCache == null) {
                mutableCache = localCache.getClone();
                mutatedInTxn = new ArrayList<>();
            }
            replayData.init(mutableCache.getCacheInstance());
            replayData.apply(mutableCache);
        }
    }

    public PersistentStoreTransaction getSnapshot() {
        // this snapshots should not be registered in store, hence no de-registration
        return new PersistentStoreTransactionSnapshot(this, txn);
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
            updateMutableCache(new EntityAddedHandleChecker(id));
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
            updateMutableCache(new EntityAddedHandleChecker(entityId));
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
        return new EntitiesOfTypeIterable(this, store, entityTypeId);
    }

    @Override
    @NotNull
    public EntityIterable getSingletonIterable(@NotNull final Entity entity) {
        return new SingleEntityIterable(store, entity.getId());
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
        return new PropertyValueIterable(store, entityTypeId, propertyId, value);
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
        return new PropertyRangeIterable(store, entityTypeId, propertyId, minValue, maxValue);
    }

    @Override
    @NotNull
    public EntityIterable findIds(@NotNull final String entityType, final long minValue, final long maxValue) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesOfTypeRangeIterable(this, store, entityTypeId, minValue, maxValue);
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
        return new EntitiesWithPropertyIterable(store, entityTypeId, propertyId);
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
        return new PropertiesIterable(store, entityTypeId, propertyId);
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
    public EntityIterable findWithBlob(@NotNull final String entityType, @NotNull final String propertyName) {
        final int entityTypeId = store.getEntityTypeId(this, entityType, false);
        if (entityTypeId < 0) {
            return EntityIterableBase.EMPTY;
        }
        final int blobId = store.getPropertyId(this, propertyName, false);
        if (blobId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return new EntitiesWithBlobIterable(store, entityTypeId, blobId);
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
            return new EntityToLinksIterable(store, ((PersistentEntity) entity).getId(), entityTypeId, linkId);
        }
        EntityId id = entity.getId();
        if (id instanceof PersistentEntityId) {
            return new EntityToLinksIterable(store, id, entityTypeId, linkId);
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
        return new EntitiesWithLinkIterable(store, entityTypeId, linkId);
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
        return new EntitiesWithLinkSortedIterable(store, entityTypeId, linkId, oppositeEntityId, oppositeLinkId);
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
        return new SortIterable(
                store, findWithPropSortedByValue(entityType, propertyName), (EntityIterableBase) rightOrder, entityTypeId, ascending);
    }

    @Override
    @NotNull
    public EntityIterable sortLinks(@NotNull final String entityType,
                                    @NotNull final EntityIterable sortedLinks,
                                    final boolean isMultiple,
                                    @NotNull final String linkName,
                                    @NotNull final EntityIterable rightOrder) {
        final EntityIterable result = new SortIndirectIterable(store, entityType,
                (EntityIterableBase) sortedLinks.getSource(), linkName, (EntityIterableBase) rightOrder, null, null);
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
        final EntityIterable result = new SortIndirectIterable(store, entityType,
                (EntityIterableBase) sortedLinks.getSource(), linkName, (EntityIterableBase) rightOrder, oppositeEntityType, oppositeLinkName);
        return isMultiple ? result.distinct() : result;
    }

    @Override
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
        return filtered == null ? EntityIterableBase.EMPTY : new MergeSortedIterable(store, filtered, comparator);
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
    public void clearHistory(@NotNull final String entityType) {
        store.clearHistory(this, entityType);
    }

    @Override
    public void setQueryCancellingPolicy(QueryCancellingPolicy policy) {
        queryCancellingPolicy = policy;
    }

    @Override
    public QueryCancellingPolicy getQueryCancellingPolicy() {
        return queryCancellingPolicy;
    }

    @Override
    public void insertUniqueKey(@NotNull final Index index,
                                @NotNull final List<Comparable> propValues,
                                @NotNull final Entity entity) {
        store.insertUniqueKey(this, index, propValues, entity);
    }

    @Override
    public void deleteUniqueKey(@NotNull final Index index,
                                @NotNull final List<Comparable> propValues) {
        store.deleteUniqueKey(this, index, propValues);
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

    @Nullable
    public CachedWrapperIterable getCachedWrapper(@NotNull final EntityIterableBase sample) {
        final EntityIterableHandle handle = sample.getHandle();
        final EntityIterableCacheAdapter localCache = getLocalCache();
        return localCache.tryKey(handle);
    }

    public void addCachedWrapper(@NotNull final CachedWrapperIterable cached) {
        // don't remember wrappers on txn replay. this looks innocent by now.
        if (replayData == null || !replayData.hasCacheSnapshot()) {
            final EntityIterableCacheAdapter localCache = getLocalCache();
            final EntityIterableHandle handle = cached.getHandle();
            if (localCache.getObject(handle) == null) {
                localCache.cacheObject(handle, cached);
                final EntityIterableCacheImpl entityIterableCache = store.getEntityIterableCache();
                if (entityIterableCache.getCacheAdapter() == localCache) {
                    entityIterableCache.setCachedCount(handle, cached.size());
                }
            }
        }
    }

    @Override
    public void enableReplayData() {
        replayData = new ReplayData();
    }

    @Override
    public void disableReplayData() {
        replayData = null;
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

    @NotNull
    EntityIterableCacheAdapter getLocalCache() {
        return mutableCache != null ? mutableCache : localCache;
    }

    boolean isMutable() {
        return mutableCache != null;
    }

    void entityDeleted(@NotNull final PersistentEntityId id) {
        updateMutableCache(new EntityDeletedHandleChecker(id));
    }

    void propertyChanged(@NotNull final PersistentEntityId id,
                         final int propertyId,
                         @Nullable final Comparable oldValue,
                         @Nullable final Comparable newValue) {
        final PropertyId propId = new PropertyId(id, propertyId);
        propsCache.remove(propId);
        updateMutableCache(new PropertyChangedHandleChecker(id.getTypeId(), id.getLocalId(), propertyId, oldValue, newValue));
    }

    void linkAdded(@NotNull final PersistentEntityId sourceId,
                   @NotNull final PersistentEntityId targetId,
                   final int linkId) {
        final PropertyId propId = new PropertyId(sourceId, linkId);
        linksCache.remove(propId);
        updateMutableCache(new LinkAddedHandleChecker(sourceId, targetId, linkId));
    }

    void linkDeleted(@NotNull final PersistentEntityId sourceId,
                     @NotNull final PersistentEntityId targetId,
                     final int linkId) {
        final PropertyId propId = new PropertyId(sourceId, linkId);
        linksCache.remove(propId);
        updateMutableCache(new LinkDeletedHandleChecker(sourceId, targetId, linkId));
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

    protected void revertCaches() {
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
                    final EntityIterableCacheImpl entityIterableCache = store.getEntityIterableCache();
                    for (final UpdatableCachedWrapperIterable it : mutatedInTxn) {
                        it.endUpdate();
                        entityIterableCache.setCachedCount(it.getHandle(), it.size());
                    }
                    if (!entityIterableCache.compareAndSetCacheAdapter(localCache, mutableCache)) {
                        throw new EntityStoreException("This exception should never be thrown");
                    }
                }
            }
        });
    }

    private void flushNonTransactionalBlobs() {
        final BlobVault blobVault = store.getBlobVault();
        if (!blobVault.requiresTxn()) {
            try {
                blobVault.flushBlobs(blobStreams, blobFiles, deferredBlobsToDelete, txn);
            } catch (Exception e) {
                handleOutOfDiskSpace(e);
                throw ExodusException.toEntityStoreException(e);
            }
        }
    }

    private void updateMutableCache(@NotNull final HandleChecker checker) {
        if (mutableCache == null) {
            mutableCache = localCache.getClone(); // preemptive version mismatch disabled
            mutatedInTxn = new ArrayList<>();
            if (replayData != null) {
                replayData.setCacheSnapshot(mutableCache.getClone());
            }
        }
        final EntityIterableCacheAdapter mutableCache = this.mutableCache;
        if (replayData != null) {
            replayData.updateMutableCache(mutableCache, mutatedInTxn, checker);
        } else {
            mutableCache.forEachKey(new ObjectProcedure<EntityIterableHandle>() {
                @Override
                public boolean execute(EntityIterableHandle object) {
                    switch (checker.checkHandle(object, mutableCache)) {
                        case KEEP:
                            break; // do nothing, keep handle
                        case REMOVE:
                            mutableCache.remove(object);
                            break;
                        case UPDATE:
                            UpdatableCachedWrapperIterable it = (UpdatableCachedWrapperIterable) mutableCache.getObject(object);
                            if (it != null) {
                                if (!it.isMutated()) {
                                    it = it.beginUpdate();
                                    // cache new mutated iterable wrapper
                                    mutableCache.cacheObject(object, it);
                                    mutatedInTxn.add(it);
                                }
                                checker.update(object, it);
                            }
                    }
                    return true;
                }
            });
        }
    }

    private void handleOutOfDiskSpace(final Exception e) {
        if (e instanceof IOException && store.getUsableSpace() < 4096) {
            throw new OutOfDiskSpaceException(e);
        }
    }

    private static <V> ObjectCacheBase<PropertyId, V> createObjectCache(final int size) {
        return size == 0 ?
                new FakeObjectCache<PropertyId, V>() :
                new ObjectCacheDecorator<PropertyId, V>(size) {
                    @Override
                    protected ObjectCacheBase<PropertyId, V> createdDecorated() {
                        return new ObjectCache<PropertyId, V>(size());
                    }
                };
    }

    enum HandleCheckResult {
        KEEP,
        REMOVE,
        UPDATE
    }

    abstract static class HandleChecker {

        abstract HandleCheckResult checkHandle(@NotNull final EntityIterableHandle handle,
                                               @NotNull final EntityIterableCacheAdapter mutableCache);

        void update(@NotNull final EntityIterableHandle handle,
                    @NotNull final UpdatableCachedWrapperIterable iterable) {
            // do nothing
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

    private abstract static class EntityAddedOrRemovedHandleChecker extends HandleChecker {

        protected final EntityId id;

        protected EntityAddedOrRemovedHandleChecker(@NotNull final EntityId id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            EntityAddedOrRemovedHandleChecker that = (EntityAddedOrRemovedHandleChecker) obj;

            return id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private static class EntityDeletedHandleChecker extends EntityAddedOrRemovedHandleChecker {

        private EntityDeletedHandleChecker(@NotNull EntityId id) {
            super(id);
        }

        @Override
        public HandleCheckResult checkHandle(@NotNull final EntityIterableHandle handle,
                                             @NotNull final EntityIterableCacheAdapter mutableCache) {
            final boolean result = handle.isMatchedEntityDeleted(id);
            if (result && handle instanceof EntitiesOfTypeIterable.EntitiesOfTypeIterableHandle) {
                return HandleCheckResult.UPDATE;
            }
            return result ? HandleCheckResult.REMOVE : HandleCheckResult.KEEP;
        }

        @Override
        void update(@NotNull final EntityIterableHandle handle,
                    @NotNull final UpdatableCachedWrapperIterable iterable) {
            ((EntitiesOfTypeIterableWrapper) iterable).removeEntity(id);
        }
    }

    private static class EntityAddedHandleChecker extends EntityAddedOrRemovedHandleChecker {

        private EntityAddedHandleChecker(@NotNull EntityId id) {
            super(id);
        }

        @Override
        public HandleCheckResult checkHandle(@NotNull final EntityIterableHandle handle,
                                             @NotNull final EntityIterableCacheAdapter mutableCache) {
            final boolean result = handle.isMatchedEntityAdded(id);
            if (result && handle instanceof EntitiesOfTypeIterable.EntitiesOfTypeIterableHandle) {
                return HandleCheckResult.UPDATE;
            }
            return result ? HandleCheckResult.REMOVE : HandleCheckResult.KEEP;
        }

        @Override
        void update(@NotNull final EntityIterableHandle handle,
                    @NotNull final UpdatableCachedWrapperIterable iterable) {
            ((EntitiesOfTypeIterableWrapper) iterable).addEntity(id);
        }
    }

    private abstract static class LinkChangedHandleChecker extends HandleChecker {
        @NotNull
        protected final PersistentEntityId sourceId;
        @NotNull
        protected final PersistentEntityId targetId;
        protected final int linkId;

        private LinkChangedHandleChecker(@NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId, int linkId) {
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.linkId = linkId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            LinkChangedHandleChecker that = (LinkChangedHandleChecker) obj;

            if (linkId != that.linkId) return false;
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

    private static final class LinkAddedHandleChecker extends LinkChangedHandleChecker {

        private LinkAddedHandleChecker(@NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId, int linkId) {
            super(sourceId, targetId, linkId);
        }

        @Override
        public HandleCheckResult checkHandle(@NotNull EntityIterableHandle handle, @NotNull EntityIterableCacheAdapter mutableCache) {
            return handle.hasLinkId(linkId) && handle.isMatchedLinkAdded(sourceId, targetId, linkId) ? HandleCheckResult.REMOVE : HandleCheckResult.KEEP;
        }
    }

    private static final class LinkDeletedHandleChecker extends LinkChangedHandleChecker {

        private LinkDeletedHandleChecker(@NotNull PersistentEntityId sourceId, @NotNull PersistentEntityId targetId, int linkId) {
            super(sourceId, targetId, linkId);
        }

        @Override
        public HandleCheckResult checkHandle(@NotNull EntityIterableHandle handle, @NotNull EntityIterableCacheAdapter mutableCache) {
            return handle.hasLinkId(linkId) && handle.isMatchedLinkDeleted(sourceId, targetId, linkId) ? HandleCheckResult.REMOVE : HandleCheckResult.KEEP;
        }
    }

    private static final class PropertyChangedHandleChecker extends HandleChecker {

        private final int typeId;
        private final long localId;
        private final int propertyId;
        @Nullable
        private final Comparable oldValue;
        @Nullable
        private final Comparable newValue;

        private PropertyChangedHandleChecker(int typeId, long localId, int propertyId,
                                             @Nullable Comparable oldValue, @Nullable Comparable newValue) {
            this.typeId = typeId;
            this.localId = localId;
            this.propertyId = propertyId;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        @Override
        public HandleCheckResult checkHandle(@NotNull EntityIterableHandle handle, @NotNull EntityIterableCacheAdapter mutableCache) {
            final boolean result = handle.isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue);
            if (result && handle instanceof PropertiesIterable.PropertiesIterableHandle) {
                return HandleCheckResult.UPDATE;
            }
            return result ? HandleCheckResult.REMOVE : HandleCheckResult.KEEP;
        }

        @Override
        void update(@NotNull final EntityIterableHandle handle,
                    @NotNull final UpdatableCachedWrapperIterable iterable) {
            ((PropertiesIterableWrapper) iterable).update(typeId, localId, oldValue, newValue);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            PropertyChangedHandleChecker that = (PropertyChangedHandleChecker) obj;

            if (propertyId != that.propertyId) return false;
            if (typeId != that.typeId) return false;
            if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
            return !(oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null);
        }

        @Override
        public int hashCode() {
            int result = typeId;
            result = 31 * result + propertyId;
            result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
            result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
            return result;
        }
    }
}
