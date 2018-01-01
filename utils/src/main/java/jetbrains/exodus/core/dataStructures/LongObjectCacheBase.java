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

@SuppressWarnings({"ProtectedField"})
public abstract class LongObjectCacheBase<V> extends CacheHitRateable {

    public static final int DEFAULT_SIZE = 8192;
    public static final int MIN_SIZE = 4;

    static final CriticalSection TRIVIAL_CRITICAL_SECTION = new CriticalSection() {
        @Override
        public void close() {
        }
    };

    protected final int size;
    private final CriticalSection criticalSection = new CriticalSection() {

        @Override
        public void close() {
            unlock();
        }
    };

    protected LongObjectCacheBase(final int size) {
        this.size = Math.max(MIN_SIZE, size);
    }

    public boolean isEmpty() {
        return count() == 0;
    }

    public boolean containsKey(final long key) {
        return isCached(key);
    }

    public V get(final long key) {
        return tryKey(key);
    }

    @SuppressWarnings({"VariableNotUsedInsideIf"})
    public V put(final long key, final V value) {
        final V oldValue = tryKey(key);
        if (oldValue != null) {
            remove(key);
        }
        cacheObject(key, value);
        return oldValue;
    }

    public V tryKeyLocked(final long key) {
        try (CriticalSection ignored = newCriticalSection()) {
            return tryKey(key);
        }
    }

    public abstract void clear();

    public abstract void lock();

    public abstract void unlock();

    public abstract V cacheObject(final long key, @NotNull final V x);

    // returns value pushed out of the cache
    public abstract V remove(final long key);

    public abstract V tryKey(final long key);

    public abstract V getObject(final long key);

    public boolean isCached(final long key) {
        return getObject(key) != null;
    }

    public abstract int count();

    public int size() {
        return size;
    }

    @Override
    public void adjustHitRate() {
        try (CriticalSection ignored = newCriticalSection()) {
            super.adjustHitRate();
        }
    }

    public CriticalSection newCriticalSection() {
        lock();
        return criticalSection;
    }

    public interface CriticalSection extends AutoCloseable {

        @Override
        void close();
    }
}