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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Priority;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.SharedTimer;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.env.ReadonlyTransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;

public final class EntityIterableCache {

    private static final Logger logger = LoggerFactory.getLogger(EntityIterableCache.class);

    @NotNull
    private final PersistentEntityStoreImpl store;
    @NotNull
    private final PersistentEntityStoreConfig config;
    @NotNull
    private EntityIterableCacheAdapter cacheAdapter;
    @NotNull
    private ObjectCacheBase<Object, Long> deferredIterablesCache;
    @NotNull
    private ObjectCacheBase<Object, Long> iterableCountsCache;
    @NotNull
    final EntityStoreSharedAsyncProcessor processor;

    public EntityIterableCache(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
        config = store.getConfig();
        cacheAdapter = new EntityIterableCacheAdapter(config);
        clear();
        processor = new EntityStoreSharedAsyncProcessor(config.getEntityIterableCacheThreadCount());
        processor.start();
        SharedTimer.registerPeriodicTask(new CacheHitRateAdjuster(this));
    }

    public float hitRate() {
        return cacheAdapter.hitRate();
    }

    public int count() {
        return cacheAdapter.count();
    }

    public void clear() {
        cacheAdapter.clear();
        final int cacheSize = config.getEntityIterableCacheSize();
        deferredIterablesCache = new ConcurrentObjectCache<>(cacheSize);
        iterableCountsCache = new ConcurrentObjectCache<>(cacheSize * 2);
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
            return it.getOrCreateCachedInstance(txn);
        }
        if (!isCachingQueueFull()) {
            new EntityIterableAsyncInstantiation(handle, it, true).queue(Priority.below_normal);
        }

        return it;
    }

    @Nullable
    public Long getCachedCount(@NotNull final EntityIterableHandle handle) {
        return iterableCountsCache.tryKey(handle.getIdentity());
    }

    public long getCachedCount(@NotNull final EntityIterableBase it) {
        if (isDispatcherThread()) {
            return it.getOrCreateCachedInstance(it.getTransaction()).size();
        }
        final EntityIterableHandle handle = it.getHandle();
        @Nullable final Long result = getCachedCount(handle);
        if (it.isThreadSafe() && !isCachingQueueFull()) {
            new EntityIterableAsyncInstantiation(handle, it, false).queue(Priority.normal);
        }
        return result == null ? -1 : result;
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

        private EntityIterableAsyncInstantiation(@NotNull final EntityIterableHandle handle,
                                                 @NotNull final EntityIterableBase it,
                                                 final boolean isConsistent) {
            this.it = it;
            this.handle = handle;
            cancellingPolicy = new CachingCancellingPolicy(isConsistent && handle.isConsistent());
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
            final long started;
            if (isCachingQueueFull() || cancellingPolicy.isOverdue(started = System.currentTimeMillis())) {
                return;
            }
            Thread.yield();
            store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
                @Override
                public void execute(@NotNull final StoreTransaction tx) {
                    if (!handle.isConsistent()) {
                        handle.resetBirthTime();
                    }
                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                    cancellingPolicy.setLocalCache(txn.getLocalCache());
                    txn.setQueryCancellingPolicy(cancellingPolicy);
                    try {
                        if (!logger.isInfoEnabled()) {
                            it.getOrCreateCachedInstance(txn, !cancellingPolicy.isConsistent);
                        } else {
                            it.getOrCreateCachedInstance(txn, !cancellingPolicy.isConsistent);
                            final long cachedIn = System.currentTimeMillis() - started;
                            if (cachedIn > 1000) {
                                String action = cancellingPolicy.isConsistent ? "Cached" : "Cached (inconsistent)";
                                logger.info(action + " in " + cachedIn + " ms, handle=" + getStringPresentation(handle));
                            }
                        }
                    } catch (ReadonlyTransactionException rte) {
                        // work around XD-626
                        final String action = cancellingPolicy.isConsistent ? "Caching" : "Caching (inconsistent)";
                        logger.error(action + " failed with ReadonlyTransactionException. Re-queueing...");
                        queue(Priority.below_normal);
                    } catch (TooLongEntityIterableInstantiationException e) {
                        if (logger.isInfoEnabled()) {
                            final String action = cancellingPolicy.isConsistent ? "Caching" : "Caching (inconsistent)";
                            logger.info(action + " forcedly stopped, " + e.reason.message + ": " + getStringPresentation(handle));
                        }
                    }
                }
            });
        }
    }

    private final class CachingCancellingPolicy implements QueryCancellingPolicy {

        private final boolean isConsistent;
        private final long startTime;
        private EntityIterableCacheAdapter localCache;

        private CachingCancellingPolicy(final boolean isConsistent) {
            this.isConsistent = isConsistent;
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
            return (isConsistent && cacheAdapter != localCache) || isOverdue(System.currentTimeMillis());
        }

        @Override
        public void doCancel() {
            final TooLongEntityIterableInstantiationReason reason;
            if (isConsistent && cacheAdapter != localCache) {
                reason = TooLongEntityIterableInstantiationReason.CACHE_ADAPTER_OBSOLETE;
            } else {
                reason = TooLongEntityIterableInstantiationReason.JOB_OVERDUE;
            }
            throw new TooLongEntityIterableInstantiationException(reason);
        }
    }

    @SuppressWarnings({"serial", "SerializableClassInSecureContext", "EmptyClass", "SerializableHasSerializationMethods", "DeserializableClassInSecureContext"})
    private static class TooLongEntityIterableInstantiationException extends ExodusException {
        private final TooLongEntityIterableInstantiationReason reason;

        TooLongEntityIterableInstantiationException(TooLongEntityIterableInstantiationReason reason) {
            super(reason.message);

            this.reason = reason;
        }
    }

    private enum TooLongEntityIterableInstantiationReason {
        CACHE_ADAPTER_OBSOLETE("cache adapter is obsolete"),
        JOB_OVERDUE("caching job is overdue");

        private final String message;

        TooLongEntityIterableInstantiationReason(String message) {
            this.message = message;
        }
    }

    private static class CacheHitRateAdjuster implements SharedTimer.ExpirablePeriodicTask {

        @NotNull
        private final WeakReference<EntityIterableCache> cacheRef;

        private CacheHitRateAdjuster(@NotNull final EntityIterableCache cache) {
            cacheRef = new WeakReference<>(cache);
        }

        @Override
        public boolean isExpired() {
            return cacheRef.get() == null;
        }

        @Override
        public void run() {
            final EntityIterableCache cache = cacheRef.get();
            if (cache != null) {
                cache.cacheAdapter.adjustHitRate();
            }
        }
    }
}
