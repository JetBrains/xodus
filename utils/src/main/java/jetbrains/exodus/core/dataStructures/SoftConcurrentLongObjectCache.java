/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import static jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache.DEFAULT_NUMBER_OF_GENERATIONS;

public class SoftConcurrentLongObjectCache<V> extends SoftLongObjectCacheBase<V> {

    private final int generationCount;

    public SoftConcurrentLongObjectCache(final int cacheSize) {
        this(cacheSize, DEFAULT_NUMBER_OF_GENERATIONS);
    }

    public SoftConcurrentLongObjectCache(final int cacheSize, final int generationCount) {
        super(cacheSize);
        this.generationCount = generationCount;
    }

    @NotNull
    @Override
    protected ConcurrentLongObjectCache<V> newChunk(final int chunkSize) {
        return new ConcurrentLongObjectCache<V>(chunkSize, generationCount) {
            @Override
            protected SharedTimer.ExpirablePeriodicTask getCacheAdjuster() {
                return null;
            }
        };
    }
}
