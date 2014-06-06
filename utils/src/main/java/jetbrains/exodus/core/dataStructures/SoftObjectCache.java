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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.HashUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SoftObjectCache<K, V> extends ObjectCacheBase<K, V> {

    public static final int DEFAULT_SIZE = 4096;
    public static final int MIN_SIZE = 16;

    private final Lock lock;
    private final SoftReference<ObjectCache<K, V>>[] chunks;
    private final int chuckSize;

    public SoftObjectCache() {
        this(DEFAULT_SIZE);
    }

    public SoftObjectCache(int cacheSize) {
        super(cacheSize);
        lock = new ReentrantLock();
        if (cacheSize < MIN_SIZE) {
            cacheSize = MIN_SIZE;
        }
        //noinspection unchecked
        chunks = new SoftReference[computeNumberOfChunks(cacheSize)];
        chuckSize = cacheSize / chunks.length;
        clear();
    }

    public void clear() {
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = null;
        }
        attempts = 0L;
        hits = 0L;
    }

    @Override
    public double hitRate() {
        return attempts == 0 ? 0 : ((double) hits) / ((double) attempts);
    }

    @Override
    public V tryKey(@NotNull final K key) {
        ++attempts;
        final ObjectCache<K, V> chunk = getChunk(key, false);
        final V result = chunk == null ? null : chunk.tryKey(key);
        if (result != null) {
            ++hits;
        }
        return result;
    }

    @Override
    public V getObject(@NotNull final K key) {
        final ObjectCache<K, V> chunk = getChunk(key, false);
        return chunk == null ? null : chunk.getObject(key);
    }

    @Override
    public V cacheObject(@NotNull final K key, @NotNull final V value) {
        final ObjectCache<K, V> chunk = getChunk(key, true);
        assert chunk != null;
        return chunk.cacheObject(key, value);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public int count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(@NotNull final K key) {
        final ObjectCache<K, V> chunk = getChunk(key, false);
        return chunk == null ? null : chunk.remove(key);
    }

    @Nullable
    private ObjectCache<K, V> getChunk(@NotNull final K key, final boolean create) {
        final int chunkIndex = (key.hashCode() & 0x7fffffff) % chunks.length;
        final SoftReference<ObjectCache<K, V>> ref = chunks[chunkIndex];
        ObjectCache<K, V> result = ref == null ? null : ref.get();
        if (result == null && create) {
            result = new ObjectCache<K, V>(chuckSize);
            chunks[chunkIndex] = new SoftReference<ObjectCache<K, V>>(result);
        }
        return result;
    }

    static int computeNumberOfChunks(final int cacheSize) {
        int result = (int) Math.sqrt(cacheSize);
        while (result * result < cacheSize) {
            ++result;
        }
        return HashUtil.getCeilingPrime(result);
    }
}
