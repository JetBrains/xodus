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

import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.persistent.PersistentObjectCache;
import jetbrains.exodus.entitystore.iterate.CachedWrapperIterable;
import jetbrains.exodus.entitystore.iterate.PropertiesIterableWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;

@SuppressWarnings("unchecked")
final class EntityIterableCacheAdapter {

    private final int maxKeySize;
    private final int maxSizeOfDirectValue;
    @NotNull
    final PersistentObjectCache<EntityIterableHandle, CacheItem> cache;

    EntityIterableCacheAdapter(@NotNull final PersistentEntityStoreConfig config, final int cacheSize) {
        maxKeySize = config.getEntityIterableCacheMaxKeySize();
        maxSizeOfDirectValue = config.getEntityIterableCacheMaxSizeOfDirectValue();
        cache = new PersistentObjectCache<EntityIterableHandle, CacheItem>(cacheSize);
    }

    private EntityIterableCacheAdapter(@NotNull final EntityIterableCacheAdapter source) {
        maxKeySize = source.maxKeySize;
        maxSizeOfDirectValue = source.maxSizeOfDirectValue;
        cache = source.cache.getClone();
    }

    @Nullable
    CachedWrapperIterable tryKey(@NotNull final EntityIterableHandle key) {
        if (isHandleTooLong(key)) {
            return null;
        }
        return parseCachedObject(key, cache.tryKey(key));
    }

    @Nullable
    CachedWrapperIterable getObject(@NotNull final EntityIterableHandle key) {
        if (isHandleTooLong(key)) {
            return null;
        }
        return parseCachedObject(key, cache.getObject(key));
    }

    void cacheObject(@NotNull final EntityIterableHandle key, @NotNull final CachedWrapperIterable it) {
        if (isHandleTooLong(key)) {
            return;
        }
        cache.cacheObject(key, new CacheItem(it, maxSizeOfDirectValue));
    }

    void forEachKey(final ObjectProcedure<EntityIterableHandle> procedure) {
        cache.forEachKey(procedure);
    }

    void remove(@NotNull final EntityIterableHandle key) {
        cache.remove(key);
    }

    double hitRate() {
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

    EntityIterableCacheAdapter getClone() {
        return new EntityIterableCacheAdapter(this);
    }

    boolean isHandleTooLong(@NotNull final EntityIterableHandle handle) {
        return handle.getStringHandle().length() > maxKeySize;
    }

    private CachedWrapperIterable parseCachedObject(@NotNull final EntityIterableHandle key, @Nullable final CacheItem item) {
        if (item == null) {
            return null;
        }
        CachedWrapperIterable cached = item.cached;
        if (cached == null) {
            cached = item.ref.get();
            if (cached == null) {
                cache.remove(key);
            }
        }
        return cached;
    }

    static final class CacheItem {
        private final CachedWrapperIterable cached;
        private final SoftReference<CachedWrapperIterable> ref;

        private CacheItem(@NotNull final CachedWrapperIterable it, final int maxSizeOfDirectValue) {
            if (!(it instanceof PropertiesIterableWrapper) && it.size() <= maxSizeOfDirectValue) {
                cached = it;
                ref = null;
            } else {
                cached = null;
                ref = new SoftReference<CachedWrapperIterable>(it);
            }
        }
    }
}
