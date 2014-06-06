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

import org.jetbrains.annotations.NotNull;

public abstract class ObjectCacheBase<K, V> {

    public static final int DEFAULT_SIZE = 8192;
    public static final int MIN_SIZE = 4;

    protected final int size;
    protected long attempts;
    protected long hits;

    protected ObjectCacheBase(final int size) {
        this.size = Math.max(MIN_SIZE, size);
    }

    public boolean isEmpty() {
        return count() == 0;
    }

    public int size() {
        return size;
    }

    public boolean containsKey(final K key) {
        return isCached(key);
    }

    public V get(final K key) {
        return tryKey(key);
    }

    public boolean isCached(final K key) {
        return getObject(key) != null;
    }

    @SuppressWarnings({"VariableNotUsedInsideIf"})
    public V put(final K key, final V value) {
        final V oldValue = tryKey(key);
        if (oldValue != null) {
            remove(key);
        }
        cacheObject(key, value);
        return oldValue;
    }

    public V tryKeyLocked(@NotNull final K key) {
        lock();
        try {
            return tryKey(key);
        } finally {
            unlock();
        }
    }

    public long getAttempts() {
        return attempts;
    }

    public long getHits() {
        return hits;
    }

    public double hitRate() {
        return attempts > 0 ? (double) hits / (double) attempts : 0;
    }

    public void fillWith(@NotNull final ObjectCacheBase<K, V> source, int maxCount) {
        // by default do nothing
    }

    public abstract void clear();

    public abstract void lock();

    public abstract void unlock();

    public abstract V cacheObject(@NotNull final K key, @NotNull final V x);

    // returns value pushed out of the cache
    public abstract V remove(@NotNull final K key);

    public abstract V tryKey(@NotNull final K key);

    public abstract V getObject(@NotNull final K key);

    public abstract int count();
}
