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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;

public abstract class SoftLongObjectCacheBase<V> extends LongObjectCacheBase<V> {

    public static final int DEFAULT_SIZE = 4096;
    public static final int MIN_SIZE = 16;

    private final SoftReference<LongObjectCacheBase<V>>[] chunks;
    private final int chunkSize;

    public SoftLongObjectCacheBase() {
        this(DEFAULT_SIZE);
    }

    public SoftLongObjectCacheBase(int cacheSize) {
        super(cacheSize);
        if (cacheSize < MIN_SIZE) {
            cacheSize = MIN_SIZE;
        }
        //noinspection unchecked
        chunks = new SoftReference[SoftObjectCacheBase.computeNumberOfChunks(cacheSize)];
        chunkSize = cacheSize / chunks.length;
        clear();
    }

    @Override
    public void clear() {
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = null;
        }
    }

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public V tryKey(final long key) {
        incAttempts();
        final LongObjectCacheBase<V> chunk = getChunk(key, false);
        final V result = chunk == null ? null : chunk.tryKeyLocked(key);
        if (result != null) {
            incHits();
        }
        return result;
    }

    @Override
    public V getObject(final long key) {
        final LongObjectCacheBase<V> chunk = getChunk(key, false);
        if (chunk == null) {
            return null;
        }
        try (CriticalSection ignored = chunk.newCriticalSection()) {
            return chunk.getObject(key);
        }
    }

    @Override
    public V cacheObject(final long key, @NotNull final V value) {
        final LongObjectCacheBase<V> chunk = getChunk(key, true);
        if (chunk == null) {
            throw new NullPointerException();
        }
        try (CriticalSection ignored = chunk.newCriticalSection()) {
            return chunk.cacheObject(key, value);
        }
    }

    @Override
    public V remove(final long key) {
        final LongObjectCacheBase<V> chunk = getChunk(key, false);
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

    @NotNull
    protected abstract LongObjectCacheBase<V> newChunk(final int chunkSize);

    @Nullable
    private LongObjectCacheBase<V> getChunk(final long key, final boolean create) {
        final int chunkIndex = (int) ((key & 0x7fffffffffffffffL) % chunks.length);
        final SoftReference<LongObjectCacheBase<V>> ref = chunks[chunkIndex];
        LongObjectCacheBase<V> result = ref == null ? null : ref.get();
        if (result == null && create) {
            result = newChunk(chunkSize);
            chunks[chunkIndex] = new SoftReference<>(result);
        }
        return result;
    }
}
