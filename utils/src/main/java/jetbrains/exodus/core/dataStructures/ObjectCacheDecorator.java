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

import jetbrains.exodus.core.execution.SharedTimer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache decorator for lazy cache creation.
 */
public abstract class ObjectCacheDecorator<K, V> extends ObjectCacheBase<K, V> {

    private static final ObjectCacheBase FAKE_CACHE = new FakeObjectCache<>();

    @Nullable
    private ObjectCacheBase<K, V> decorated;

    public ObjectCacheDecorator() {
        this(DEFAULT_SIZE);
    }

    public ObjectCacheDecorator(final int size) {
        super(size);
    }

    public void clear() {
        if (decorated != null) {
            decorated.close();
            decorated = null;
        }
    }

    @Override
    public void lock() {
        getCache(true).lock();
    }

    @Override
    public void unlock() {
        getCache(false).unlock();
    }

    public V cacheObject(@NotNull K key, @NotNull V x) {
        return getCache(true).cacheObject(key, x);
    }

    public V remove(@NotNull K key) {
        return getCache(false).remove(key);
    }

    public V tryKey(@NotNull K key) {
        return getCache(false).tryKey(key);
    }

    public V getObject(@NotNull K key) {
        return getCache(false).getObject(key);
    }

    public int count() {
        return getCache(false).count();
    }

    @Override
    public void close() {
        getCache(false).close();
    }

    @Override
    public int getAttempts() {
        return getCache(false).getAttempts();
    }

    @Override
    public int getHits() {
        return getCache(false).getHits();
    }

    @Override
    public float hitRate() {
        return getCache(false).hitRate();
    }

    @Override
    public CriticalSection newCriticalSection() {
        return getCache(true).newCriticalSection();
    }

    @Override
    protected void incAttempts() {
        getCache(false).incAttempts();
    }

    @Override
    protected void incHits() {
        getCache(false).incHits();
    }

    @Nullable
    @Override
    protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
        return null;
    }

    protected abstract ObjectCacheBase<K, V> createdDecorated();

    @NotNull
    private ObjectCacheBase<K, V> getCache(final boolean create) {
        if (decorated == null) {
            if (!create) {
                //noinspection unchecked
                return FAKE_CACHE;
            }
            decorated = createdDecorated();

        }
        return decorated;
    }
}
