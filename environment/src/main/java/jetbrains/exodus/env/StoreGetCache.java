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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.core.dataStructures.SoftConcurrentLongObjectCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Caches Store.get() results retrieved from immutable trees.
 * For each key and tree address (KeyEntry), value is immutable, so lock-free caching is ok.
 */
class StoreGetCache {

    private final SoftConcurrentLongObjectCache<ValueEntry> cache;

    StoreGetCache(final int cacheSize) {
        cache = new SoftConcurrentLongObjectCache<>(cacheSize);
    }

    void close() {
        cache.close();
    }

    @Nullable
    ByteIterable tryKey(final long treeRootAddress, @NotNull final ByteIterable key) {
        final ValueEntry ve = cache.tryKey(cacheKey(treeRootAddress, key));
        return ve == null || ve.treeRootAddress != treeRootAddress || !ve.key.equals(key) ? null : ve.value;
    }

    void cacheObject(final long treeRootAddress, @NotNull final ByteIterable key, @NotNull final ArrayByteIterable value) {
        cache.cacheObject(cacheKey(treeRootAddress, key), new ValueEntry(treeRootAddress, key, value));
    }

    float hitRate() {
        return cache.hitRate();
    }

    private static long cacheKey(final long treeRootAddress, @NotNull final ByteIterable key) {
        return treeRootAddress ^ key.hashCode();
    }

    private static class ValueEntry {

        private final long treeRootAddress;
        @NotNull
        private final ByteIterable key;
        @NotNull
        private final ArrayByteIterable value;

        ValueEntry(final long treeRootAddress, @NotNull final ByteIterable key, @NotNull final ArrayByteIterable value) {
            this.treeRootAddress = treeRootAddress;
            this.key = key;
            this.value = value;
        }
    }
}
