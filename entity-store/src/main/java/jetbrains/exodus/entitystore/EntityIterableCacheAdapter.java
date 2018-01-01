/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.persistent.EvictListener;
import jetbrains.exodus.core.dataStructures.persistent.PersistentObjectCache;
import jetbrains.exodus.core.execution.SharedTimer;
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;

class EntityIterableCacheAdapter {

    @NotNull
    protected final PersistentEntityStoreConfig config;
    @NotNull
    protected final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache;
    @NotNull
    protected final HashMap<EntityIterableHandle, Updatable> stickyObjects;

    EntityIterableCacheAdapter(@NotNull final PersistentEntityStoreConfig config) {
        this(config, new NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem>(config.getEntityIterableCacheSize()), new HashMap<EntityIterableHandle, Updatable>());
    }

    EntityIterableCacheAdapter(@NotNull final PersistentEntityStoreConfig config,
                               @NotNull final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache,
                               @NotNull final HashMap<EntityIterableHandle, Updatable> stickyObjects) {
        this.config = config;
        this.cache = cache;
        this.stickyObjects = stickyObjects;
    }

    @NotNull
    public PersistentObjectCache<EntityIterableHandle, CacheItem> getCacheInstance() {
        return cache;
    }

    @Nullable
    CachedInstanceIterable tryKey(@NotNull final EntityIterableHandle key) {
        if (key.isSticky()) {
            return (CachedInstanceIterable) getStickyObject(key);
        }
        return parseCachedObject(key, cache.tryKey(key));
    }

    @Nullable
    CachedInstanceIterable getObject(@NotNull final EntityIterableHandle key) {
        if (key.isSticky()) {
            return (CachedInstanceIterable) getStickyObject(key);
        }
        return parseCachedObject(key, cache.getObject(key));
    }

    @Nullable
    Updatable getUpdatable(@NotNull final EntityIterableHandle key) {
        if (key.isSticky()) {
            return getStickyObject(key);
        }
        return (Updatable) parseCachedObject(key, cache.getObject(key));
    }

    void cacheObject(@NotNull final EntityIterableHandle key, @NotNull final CachedInstanceIterable it) {
        cache.cacheObject(key, new CacheItem(it, config.getEntityIterableCacheMaxSizeOfDirectValue()));
    }

    void forEachKey(final ObjectProcedure<EntityIterableHandle> procedure) {
        cache.forEachKey(procedure);
    }

    void remove(@NotNull final EntityIterableHandle key) {
        cache.remove(key);
    }

    float hitRate() {
        return cache.hitRate();
    }

    int count() {
        return cache.count();
    }

    int size() {
        return cache.size();
    }

    void clear() {
        cache.clear();
    }

    boolean isSparse() {
        return cache.count() < cache.size() / 2;
    }

    EntityIterableCacheAdapterMutable getClone() {
        return EntityIterableCacheAdapterMutable.create(this);
    }

    void adjustHitRate() {
        cache.adjustHitRate();
    }

    Updatable getStickyObject(@NotNull final EntityIterableHandle handle) {
        Updatable result = stickyObjects.get(handle);
        if (result == null) {
            throw new IllegalStateException("Sticky object not found");
        }
        return result;
    }

    private CachedInstanceIterable parseCachedObject(@NotNull final EntityIterableHandle key, @Nullable final CacheItem item) {
        if (item == null) {
            return null;
        }
        CachedInstanceIterable cached = item.cached;
        if (cached == null) {
            cached = item.ref.get();
            if (cached == null) {
                cache.remove(key);
            }
        }
        return cached;
    }

    static CachedInstanceIterable getCachedValue(@NotNull CacheItem item) {
        CachedInstanceIterable cached = item.cached;
        if (cached == null) {
            cached = item.ref.get();
        }
        return cached;
    }

    static final class CacheItem {
        private final CachedInstanceIterable cached;
        private final SoftReference<CachedInstanceIterable> ref;

        private CacheItem(@NotNull final CachedInstanceIterable it, final int maxSizeOfDirectValue) {
            if (it.isUpdatable() || it.size() <= maxSizeOfDirectValue) {
                cached = it;
                ref = null;
            } else {
                cached = null;
                ref = new SoftReference<>(it);
            }
        }
    }

    /*
    NonAdjustablePersistentObjectCache doesn't adjust itself in order to avoid as many cache adjusters
    as many versions of the cache (as a persistent data structure) can be.
     */
    static class NonAdjustablePersistentObjectCache<K, V> extends PersistentObjectCache<K, V> {

        private NonAdjustablePersistentObjectCache(final int size) {
            super(size);
        }

        private NonAdjustablePersistentObjectCache(@NotNull final NonAdjustablePersistentObjectCache<K, V> source,
                                                   @Nullable final EvictListener<K, V> listener) {
            super(source, listener);
        }

        @Override
        public NonAdjustablePersistentObjectCache<K, V> getClone(final EvictListener<K, V> listener) {
            return new NonAdjustablePersistentObjectCache<>(this, listener);
        }

        NonAdjustablePersistentObjectCache<K, V> endWrite() {
            return new NonAdjustablePersistentObjectCache<>(this, null);
        }

        @Nullable
        @Override
        protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
            return null;
        }
    }
}
