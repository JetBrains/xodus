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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache;
import jetbrains.exodus.core.dataStructures.SoftConcurrentLongObjectCache;
import jetbrains.exodus.core.execution.SharedTimer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Caches Store.get() results retrieved from immutable trees.
 * For each key and tree address (KeyEntry), value is immutable, so lock-free caching is ok.
 */
class StoreGetCache {

    private static final int SINGLE_CHUNK_GENERATIONS = 4;

    private final SoftConcurrentLongObjectCache<ValueEntry> cache;
    private final int minTreeSize;
    private final int maxValueSize;

    StoreGetCache(final int cacheSize, final int minTreeSize, final int maxValueSize) {
        cache = new SoftConcurrentLongObjectCache<ValueEntry>(cacheSize) {
            @NotNull
            @Override
            protected ConcurrentLongObjectCache<ValueEntry> newChunk(int chunkSize) {
                return new ConcurrentLongObjectCache<ValueEntry>(chunkSize, SINGLE_CHUNK_GENERATIONS) {
                    @Nullable
                    @Override
                    protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
                        return null;
                    }
                };
            }
        };
        this.minTreeSize = minTreeSize;
        this.maxValueSize = maxValueSize;
    }

    int getMinTreeSize() {
        return minTreeSize;
    }

    int getMaxValueSize() {
        return maxValueSize;
    }

    void close() {
        cache.close();
    }

    @Nullable
    ByteIterable tryKey(final long treeRootAddress, @NotNull final ByteIterable key) {
        final int keyHashCode = key.hashCode();
        final ValueEntry ve = cache.tryKey(treeRootAddress ^ keyHashCode);
        return ve == null || ve.treeRootAddress != treeRootAddress ||
            ve.keyHashCode != keyHashCode || !ve.key.equals(key) ? null : ve.value;
    }

    void cacheObject(final long treeRootAddress, @NotNull final ByteIterable key, @NotNull final ArrayByteIterable value) {
        final ArrayByteIterable keyCopy = key instanceof ArrayByteIterable ? (ArrayByteIterable) key : new ArrayByteIterable(key);
        final int keyHashCode = keyCopy.hashCode();
        cache.cacheObject(treeRootAddress ^ keyHashCode, new ValueEntry(treeRootAddress, keyHashCode, keyCopy, value));
    }

    float hitRate() {
        return cache.hitRate();
    }


    private static class ValueEntry {

        private final long treeRootAddress;
        private final int keyHashCode;
        @NotNull
        private final ArrayByteIterable key;
        @NotNull
        private final ArrayByteIterable value;

        ValueEntry(final long treeRootAddress,
                   final int keyHashCode,
                   @NotNull final ArrayByteIterable key,
                   @NotNull final ArrayByteIterable value) {
            this.treeRootAddress = treeRootAddress;
            this.keyHashCode = keyHashCode;
            this.key = key;
            this.value = value;
        }
    }
}
