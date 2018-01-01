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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.HashUtil;
import jetbrains.exodus.util.MathUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConcurrentLongObjectCache<V> extends LongObjectCacheBase<V> {

    private static final int DEFAULT_NUMBER_OF_GENERATIONS = 3;

    private final int numberOfGenerations;
    private final int generationSize;
    private final int mask;
    private final CacheEntry<V>[] cache;

    public ConcurrentLongObjectCache() {
        this(DEFAULT_SIZE);
    }

    public ConcurrentLongObjectCache(final int size) {
        this(size, DEFAULT_NUMBER_OF_GENERATIONS);
    }

    public ConcurrentLongObjectCache(final int size, final int numberOfGenerations) {
        super(size);
        this.numberOfGenerations = numberOfGenerations;
        generationSize = HashUtil.getFloorPrime(size / numberOfGenerations);
        mask = (1 << MathUtil.integerLogarithm(generationSize)) - 1;
        cache = new CacheEntry[numberOfGenerations * generationSize];
        clear();
    }

    @Override
    public V tryKeyLocked(long key) {
        return tryKey(key);
    }

    @Override
    public void clear() {
        for (int i = 0; i < cache.length; ++i) {
            cache[i] = CacheEntry.NULL_OBJECT;
        }
    }

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public V cacheObject(final long key, @NotNull final V x) {
        int cacheIndex = HashUtil.indexFor(key, generationSize, mask) * numberOfGenerations;
        for (int i = 0; i < numberOfGenerations; ++i, ++cacheIndex) {
            final CacheEntry<V> entry = cache[cacheIndex];
            if (entry.key == key) {
                cache[cacheIndex] = new CacheEntry<>(key, x);
                // in highly concurrent environment we can't definitely know if a value is pushed out from the cache
                return null;
            }
        }
        cache[cacheIndex - 1] = new CacheEntry<>(key, x);
        return null;
    }

    @Override
    public V remove(final long key) {
        int cacheIndex = HashUtil.indexFor(key, generationSize, mask) * numberOfGenerations;
        for (int i = 0; i < numberOfGenerations; ++i, ++cacheIndex) {
            final CacheEntry<V> entry = cache[cacheIndex];
            if (entry.key == key) {
                final V result = entry.value;
                entry.value = null;
                return result;
            }
        }
        return null;
    }

    @Override
    public V tryKey(final long key) {
        incAttempts();
        int cacheIndex = HashUtil.indexFor(key, generationSize, mask) * numberOfGenerations;
        CacheEntry<V> entry = cache[cacheIndex];
        if (entry.key == key) {
            incHits();
            return entry.value;
        }
        for (int i = 1; i < numberOfGenerations; ++i) {
            entry = cache[++cacheIndex];
            if (entry.key == key) {
                incHits();
                final CacheEntry<V> temp = cache[cacheIndex - 1];
                cache[cacheIndex - 1] = entry;
                cache[cacheIndex] = temp;
                return entry.value;
            }
        }
        return null;
    }

    @Override
    public V getObject(final long key) {
        int cacheIndex = HashUtil.indexFor(key, generationSize, mask) * numberOfGenerations;
        for (int i = 0; i < numberOfGenerations; ++i, ++cacheIndex) {
            final CacheEntry<V> entry = cache[cacheIndex];
            if (entry.key == key) {
                return entry.value;
            }
        }
        return null;
    }

    @Override
    public int count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CriticalSection newCriticalSection() {
        return TRIVIAL_CRITICAL_SECTION;
    }

    private static class CacheEntry<V> {

        private static final CacheEntry NULL_OBJECT = new CacheEntry(Long.MIN_VALUE, null);

        private final long key;
        @Nullable
        private V value;

        private CacheEntry(final long key, @Nullable final V value) {
            this.key = key;
            this.value = value;
        }
    }
}
