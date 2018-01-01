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

import java.lang.ref.WeakReference;

public abstract class CacheHitRateable {

    @Nullable
    private final SharedTimer.ExpirablePeriodicTask cacheAdjuster;
    private int attempts;
    private int hits;

    protected CacheHitRateable() {
        attempts = hits = 0;
        cacheAdjuster = getCacheAdjuster();
        if (cacheAdjuster != null) {
            SharedTimer.registerPeriodicTask(cacheAdjuster);
        }
    }

    public void close() {
        if (cacheAdjuster != null) {
            SharedTimer.unregisterPeriodicTask(cacheAdjuster);
        }
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(final int attempts) {
        this.attempts = attempts;
    }

    public int getHits() {
        return hits;
    }

    public void setHits(final int hits) {
        this.hits = hits;
    }

    public float hitRate() {
        final int hits = this.hits;
        int attempts = this.attempts;
        // due to lack of thread-safety there can appear not that consistent results
        if (hits > attempts) {
            attempts = hits;
        }
        return attempts > 0 ? (float) hits / (float) attempts : 0;
    }

    protected void incAttempts() {
        ++attempts;
    }

    protected void incHits() {
        ++hits;
    }

    public void adjustHitRate() {
        final int hits = this.hits;
        int attempts = this.attempts;
        // due to lack of thread-safety there can appear not that consistent results
        if (hits > attempts) {
            attempts = hits;
        }
        if (attempts > 16) {
            this.attempts = attempts >> 1;
            this.hits = hits >> 1;
        }
    }

    @Nullable
    protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
        return new CacheAdjuster(this);
    }

    private static class CacheAdjuster implements SharedTimer.ExpirablePeriodicTask {

        private final WeakReference<CacheHitRateable> cacheRef;

        public CacheAdjuster(@NotNull final CacheHitRateable cache) {
            cacheRef = new WeakReference<>(cache);
        }

        @Override
        public boolean isExpired() {
            return cacheRef.get() == null;
        }

        @Override
        public void run() {
            final CacheHitRateable cache = cacheRef.get();
            if (cache != null) {
                cache.adjustHitRate();
            }
        }
    }
}
