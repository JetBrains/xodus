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

public class FakeObjectCache<K, V> extends ObjectCacheBase<K, V> {

    public FakeObjectCache() {
        this(0);
    }

    private FakeObjectCache(int size) {
        super(size);
    }

    @Override
    public void clear() {
        // do nothing
    }

    @Override
    public void lock() {
        // do nothing
    }

    @Override
    public void unlock() {
        // do nothing
    }

    @Override
    public V cacheObject(@NotNull K key, @NotNull V x) {
        return null;
    }

    @Override
    public V remove(@NotNull K key) {
        return null;
    }

    @Override
    public V tryKey(@NotNull K key) {
        return null;
    }

    @Override
    public V getObject(@NotNull K key) {
        return null;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public int getAttempts() {
        return 0;
    }

    @Override
    public int getHits() {
        return 0;
    }

    @Override
    public float hitRate() {
        return 0;
    }

    @Override
    public CriticalSection newCriticalSection() {
        return TRIVIAL_CRITICAL_SECTION;
    }

    @Override
    protected void incAttempts() {
        // do nothing
    }

    @Override
    protected void incHits() {
        // do nothing
    }

    @Nullable
    @Override
    protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
        return null;
    }
}
