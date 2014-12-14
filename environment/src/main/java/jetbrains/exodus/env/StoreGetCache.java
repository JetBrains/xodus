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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.core.dataStructures.SoftConcurrentObjectCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Caches Store.get() results retrieved from immutable trees.
 * For each key and tree address (KeyEntry), value is immutable, so lock-free caching is ok.
 */
class StoreGetCache {

    private final SoftConcurrentObjectCache<KeyEntry, ByteIterable> cache;

    StoreGetCache(final int cacheSize) {
        cache = new SoftConcurrentObjectCache<KeyEntry, ByteIterable>(cacheSize);
    }

    @Nullable
    ByteIterable tryKey(final long treeRootAddress, @NotNull final ByteIterable key) {
        return cache.tryKey(new KeyEntry(treeRootAddress, key));
    }

    void cacheObject(final long treeRootAddress, @NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        cache.cacheObject(new KeyEntry(treeRootAddress, key), new ArrayByteIterable(value));
    }

    private static class KeyEntry {

        private final long treeRootAddress;
        @NotNull
        private final ByteIterable key;

        KeyEntry(final long treeRootAddress, @NotNull final ByteIterable key) {
            this.treeRootAddress = treeRootAddress;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KeyEntry right = (KeyEntry) o;
            return treeRootAddress == right.treeRootAddress && key.equals(right.key);
        }

        @Override
        public int hashCode() {
            final int keyHashCode = key.hashCode();
            int result = (int) ((treeRootAddress + keyHashCode) ^ (treeRootAddress >>> 32));
            result = 31 * result + keyHashCode;
            return result;
        }
    }
}
