/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Priority;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

public final class EntityIterableCacheImpl implements EntityIterableCache {

    private static final Log log = LogFactory.getLog(EntityIterableCacheImpl.class);

    @NotNull
    private final PersistentEntityStoreImpl store;
    @NotNull
    private EntityIterableCacheAdapter cacheAdapter;
    private final long cachingTimeout;
    @NotNull
    private final ObjectCacheBase<EntityIterableHandle, Long> deferredIterablesCache;
    private final int deferredCacheDelay;
    @NotNull
    private final ObjectCacheBase<EntityIterableHandle, Long> iterableCountsCache;
    @NotNull
    final EntityStoreSharedAsyncProcessor processor;

    public EntityIterableCacheImpl(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
        final PersistentEntityStoreConfig config = store.getConfig();
        final int cacheSize = config.getEntityIterableCacheSize();
        cacheAdapter = new EntityIterableCacheAdapter(config, cacheSize);
        cachingTimeout = config.getEntityIterableCacheCachingTimeout();
        deferredIterablesCache = new ConcurrentObjectCache<EntityIterableHandle, Long>(cacheSize);
        deferredCacheDelay = config.getEntityIterableCacheDeferredDelay();
        iterableCountsCache = new ConcurrentObjectCache<EntityIterableHandle, Long>(cacheSize * 2);
        processor = new EntityStoreSharedAsyncProcessor(config.getEntityIterableCacheThreadCount());
        processor.start();
    }

    @Override
    public double hitRate() {
        return cacheAdapter.hitRate();
    }

    @Override
    public int count() {
        return cacheAdapter.count();
    }

    @Override
    public void clear() {
        cacheAdapter.clear();
    }

    /**
     * @param it iterable.
     * @return iterable which is cached or "it" itself if it's not cached.
     */
    public EntityIterableBase putIfNotCached(@NotNull final EntityIterableBase it) {
        if (!store.isCachingEnabled() || it.hasCustomTxn() || !it.canBeCached()) {
            return it;
        }

        final EntityIterableHandle handle = it.getHandle();
        final PersistentStoreTransaction txn = it.getTransaction();
        final EntityIterableCacheAdapter localCache = txn.getLocalCache();

        if (localCache.isHandleTooLong(handle)) {
            // don't try to find in cache or instantiate something that can't be cached
            return it;
        }

        final EntityIterableBase cached = localCache.tryKey(handle);
        if (cached != null) {
            if (!cached.getHandle().isExpired()) {
                return cached;
            }
            localCache.remove(handle);
        }

        if (txn.isMutable() || !txn.isCurrent()) {
            return it;
        }

        // if cache is enough full, then cache iterables after they live some time in deferred cache
        if (!localCache.isSparse()) {
            final long currentMillis = System.currentTimeMillis();
            final Long whenCached = deferredIterablesCache.tryKey(handle);
            if (whenCached == null) {
                deferredIterablesCache.cacheObject(handle, currentMillis);
                return it;
            }
            if (whenCached + deferredCacheDelay > currentMillis) {
                return it;
            }
        }

        // if we are already within an EntityStoreSharedAsyncProcessor's dispatcher,
        // then instantiate iterable without queueing a job.
        if (isDispatcherThread()) {
            return it.getOrCreateCachedWrapper(txn);
        }
        if (!isCachingQueueFull()) {
            new EntityIterableAsyncInstantiation(handle, it).queue(Priority.below_normal);
        }

        return it;
    }

    public long getCachedCount(@NotNull final EntityIterableHandle handle) {
        final Long result = iterableCountsCache.tryKey(handle);
        return result == null ? -1 : result;
    }

    public void setCachedCount(@NotNull final EntityIterableHandle handle, final long count) {
        iterableCountsCache.cacheObject(handle, count);
    }

    public boolean isDispatcherThread() {
        return processor.isDispatcherThread();
    }

    boolean isCachingQueueFull() {
        return processor.pendingJobs() > cacheAdapter.size();
    }

    @NotNull
    EntityIterableCacheAdapter getCacheAdapter() {
        return cacheAdapter;
    }

    boolean compareAndSetCacheAdapter(@NotNull final EntityIterableCacheAdapter oldValue,
                                      @NotNull final EntityIterableCacheAdapter newValue) {
        if (cacheAdapter == oldValue) {
            cacheAdapter = newValue;
            return true;
        }
        return false;
    }

    @SuppressWarnings({"EqualsAndHashcode"})
    private final class EntityIterableAsyncInstantiation extends Job {

        @NotNull
        private final EntityIterableBase it;
        @NotNull
        private final EntityIterableHandle handle;
        @NotNull
        private final CachingCancellingPolicy cancellingPolicy;

        private EntityIterableAsyncInstantiation(@NotNull final EntityIterableHandle handle, @NotNull final EntityIterableBase it) {
            this.it = it;
            this.handle = handle;
            cancellingPolicy = new CachingCancellingPolicy();
            setProcessor(processor);
        }

        @Override
        public String getName() {
            return "Caching job for handle " + it.getHandle().getStringHandle();
        }

        @Override
        public String getGroup() {
            return store.getLocation();
        }

        @Override
        public boolean isEqualTo(Job job) {
            return handle.equals(((EntityIterableAsyncInstantiation) job).handle);
        }

        public int hashCode() {
            return handle.hashCode();
        }

        @Override
        protected void execute() throws Throwable {
            final long started = System.currentTimeMillis();
            if (cancellingPolicy.isOverdue(started) || isCachingQueueFull()) {
                return;
            }
            Thread.yield();
            final PersistentStoreTransaction txn = store.beginTransaction();
            try {
                cancellingPolicy.setLocalCache(txn.getLocalCache());
                txn.setQueryCancellingPolicy(cancellingPolicy);
                try {
                    if (!log.isInfoEnabled()) {
                        it.getOrCreateCachedWrapper(txn);
                    } else {
                        it.getOrCreateCachedWrapper(txn);
                        final long cachedIn = System.currentTimeMillis() - started;
                        if (cachedIn > 1000) {
                            log.info("Cached in " + cachedIn + " ms, handle=" + handle.getStringHandle());
                        }
                    }
                } catch (TooLongEntityIterableInstantiationException e) {
                    if (log.isInfoEnabled()) {
                        log.info("Caching forcedly stopped: " + handle.getStringHandle());
                    }
                }
            } finally {
                txn.abort();
            }
        }
    }

    private final class CachingCancellingPolicy implements QueryCancellingPolicy {

        private final long startTime;
        @NotNull
        private EntityIterableCacheAdapter localCache;

        private CachingCancellingPolicy() {
            startTime = System.currentTimeMillis();
        }

        private boolean isOverdue(final long currentMillis) {
            return currentMillis - startTime > cachingTimeout;
        }

        private void setLocalCache(@NotNull final EntityIterableCacheAdapter localCache) {
            this.localCache = localCache;
        }

        @Override
        public boolean needToCancel() {
            return cacheAdapter != localCache || isOverdue(System.currentTimeMillis());
        }

        @Override
        public void doCancel() {
            throw new TooLongEntityIterableInstantiationException();
        }
    }

    @SuppressWarnings({"serial", "SerializableClassInSecureContext", "EmptyClass", "SerializableHasSerializationMethods", "DeserializableClassInSecureContext"})
    private static class TooLongEntityIterableInstantiationException extends ExodusException {
    }
}
