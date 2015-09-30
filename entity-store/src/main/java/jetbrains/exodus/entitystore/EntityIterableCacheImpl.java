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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Priority;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.SharedTimer;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;

public final class EntityIterableCacheImpl implements EntityIterableCache {

    private static final Logger logger = LoggerFactory.getLogger(EntityIterableCacheImpl.class);

    @NotNull
    private final PersistentEntityStoreImpl store;
    @NotNull
    private final PersistentEntityStoreConfig config;
    @NotNull
    private EntityIterableCacheAdapter cacheAdapter;
    @NotNull
    private final ObjectCacheBase<Object, Long> deferredIterablesCache;
    @NotNull
    private final ObjectCacheBase<Object, Long> iterableCountsCache;
    @NotNull
    final EntityStoreSharedAsyncProcessor processor;

    public EntityIterableCacheImpl(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
        config = store.getConfig();
        cacheAdapter = new EntityIterableCacheAdapter(config);
        final int cacheSize = config.getEntityIterableCacheSize();
        deferredIterablesCache = new ConcurrentObjectCache<>(cacheSize);
        iterableCountsCache = new ConcurrentObjectCache<>(cacheSize * 2);
        processor = new EntityStoreSharedAsyncProcessor(config.getEntityIterableCacheThreadCount());
        processor.start();
        SharedTimer.registerPeriodicTask(new CacheHitRateAdjuster(this));
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
        if (config.isCachingDisabled() || !it.canBeCached()) {
            return it;
        }

        final EntityIterableHandle handle = it.getHandle();
        final PersistentStoreTransaction txn = it.getTransaction();
        final EntityIterableCacheAdapter localCache = txn.getLocalCache();

        txn.localCacheAttempt();

        final EntityIterableBase cached = localCache.tryKey(handle);
        if (cached != null) {
            if (!cached.getHandle().isExpired()) {
                txn.localCacheHit();
                return cached;
            }
            localCache.remove(handle);
        }

        if (txn.isMutable() || !txn.isCurrent() || !txn.isCachingRelevant()) {
            return it;
        }

        // if cache is enough full, then cache iterables after they live some time in deferred cache
        if (!localCache.isSparse()) {
            final long currentMillis = System.currentTimeMillis();
            final Object handleIdentity = handle.getIdentity();
            final Long whenCached = deferredIterablesCache.tryKey(handleIdentity);
            if (whenCached == null) {
                deferredIterablesCache.cacheObject(handleIdentity, currentMillis);
                return it;
            }
            if (whenCached + config.getEntityIterableCacheDeferredDelay() > currentMillis) {
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
        final Long result = iterableCountsCache.tryKey(handle.getIdentity());
        return result == null ? -1L : result;
    }

    public void setCachedCount(@NotNull final EntityIterableHandle handle, final long count) {
        iterableCountsCache.cacheObject(handle.getIdentity(), count);
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

    private String getStringPresentation(@NotNull final EntityIterableHandle handle) {
        return config.getEntityIterableCacheUseHumanReadable() ?
                EntityIterableBase.getHumanReadablePresentation(handle) : handle.toString();
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
            return "Caching job for handle " + it.getHandle();
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
                    if (!logger.isInfoEnabled()) {
                        it.getOrCreateCachedWrapper(txn);
                    } else {
                        it.getOrCreateCachedWrapper(txn);
                        final long cachedIn = System.currentTimeMillis() - started;
                        if (cachedIn > 1000) {
                            logger.info("Cached in " + cachedIn + " ms, handle=" + getStringPresentation(handle));
                        }
                    }
                } catch (TooLongEntityIterableInstantiationException e) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Caching forcedly stopped: " + getStringPresentation(handle));
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
            return currentMillis - startTime > config.getEntityIterableCacheCachingTimeout();
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

    private static class CacheHitRateAdjuster implements SharedTimer.ExpirablePeriodicTask {

        @NotNull
        private final WeakReference<EntityIterableCacheImpl> cacheRef;

        private CacheHitRateAdjuster(@NotNull final EntityIterableCacheImpl cache) {
            cacheRef = new WeakReference<>(cache);
        }

        @Override
        public boolean isExpired() {
            return cacheRef.get() == null;
        }

        @Override
        public void run() {
            final EntityIterableCacheImpl cache = cacheRef.get();
            if (cache != null) {
                cache.cacheAdapter.adjustHitRate();
            }
        }
    }
}
