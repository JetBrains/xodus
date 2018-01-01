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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;

public abstract class SoftObjectCacheBase<K, V> extends ObjectCacheBase<K, V> {

    public static final int MIN_SIZE = 16;

    private final int chunkSize;
    private final SoftReference<ObjectCacheBase<K, V>>[] chunks;

    SoftObjectCacheBase(int cacheSize) {
        super(cacheSize);
        if (cacheSize < MIN_SIZE) {
            cacheSize = MIN_SIZE;
        }
        //noinspection unchecked
        chunks = new SoftReference[computeNumberOfChunks(cacheSize)];
        chunkSize = cacheSize / chunks.length;
        clear();
    }

    public void clear() {
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = null;
        }
    }

    @Override
    public final void lock() {
    }

    @Override
    public final void unlock() {
    }

    @Override
    public V tryKey(@NotNull final K key) {
        incAttempts();
        final ObjectCacheBase<K, V> chunk = getChunk(key, false);
        final V result = chunk == null ? null : chunk.tryKeyLocked(key);
        if (result != null) {
            incHits();
        }
        return result;
    }

    @Override
    public V getObject(@NotNull final K key) {
        final ObjectCacheBase<K, V> chunk = getChunk(key, false);
        if (chunk == null) {
            return null;
        }
        try (CriticalSection ignored = chunk.newCriticalSection()) {
            return chunk.getObject(key);
        }
    }

    @Override
    public V cacheObject(@NotNull final K key, @NotNull final V value) {
        final ObjectCacheBase<K, V> chunk = getChunk(key, true);
        assert chunk != null;
        try (CriticalSection ignored = chunk.newCriticalSection()) {
            return chunk.cacheObject(key, value);
        }
    }

    @Override
    public V remove(@NotNull final K key) {
        final ObjectCacheBase<K, V> chunk = getChunk(key, false);
        if (chunk == null) {
            return null;
        }
        try (CriticalSection ignored = chunk.newCriticalSection()) {
            return chunk.remove(key);
        }
    }

    @Override
    public int count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CriticalSection newCriticalSection() {
        return TRIVIAL_CRITICAL_SECTION;
    }

    static int computeNumberOfChunks(final int cacheSize) {
        int result = (int) Math.sqrt(cacheSize);
        while (result * result < cacheSize) {
            ++result;
        }
        return HashUtil.getCeilingPrime(result);
    }

    protected abstract ObjectCacheBase<K, V> newChunk(final int chunkSize);

    @Nullable
    private ObjectCacheBase<K, V> getChunk(@NotNull final K key, final boolean create) {
        final int hc = key.hashCode();
        final int chunkIndex = ((hc + (hc >> 31)) & 0x7fffffff) % chunks.length;
        final SoftReference<ObjectCacheBase<K, V>> ref = chunks[chunkIndex];
        ObjectCacheBase<K, V> result = ref == null ? null : ref.get();
        if (result == null && create) {
            result = newChunk(chunkSize);
            chunks[chunkIndex] = new SoftReference<>(result);
        }
        return result;
    }
}
