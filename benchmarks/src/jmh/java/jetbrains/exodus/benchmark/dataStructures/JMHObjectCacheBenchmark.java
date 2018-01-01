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
package jetbrains.exodus.benchmark.dataStructures;

import jetbrains.exodus.core.dataStructures.ObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "UnusedDeclaration"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHObjectCacheBenchmark {

    protected static final int CACHE_SIZE = 100000;

    final ObjectCacheBase<Integer, String> cache = createCache();
    int existingKey = 0;
    int missingKey = CACHE_SIZE;

    @Setup
    public void prepare() {
        for (int i = 0; i < CACHE_SIZE; ++i) {
            cache.cacheObject(i, Integer.toString(i));
        }
    }

    @Setup(Level.Invocation)
    public void changeIndex() {
        if (++existingKey == CACHE_SIZE) {
            existingKey = 0;
        }
        ++missingKey;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String cacheTryKey() {
        return cache.tryKey(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String cacheTryMissingKey() {
        return cache.tryKey(missingKey);
    }

    protected ObjectCacheBase<Integer, String> createCache() {
        return new ObjectCache<>(CACHE_SIZE);
    }
}
