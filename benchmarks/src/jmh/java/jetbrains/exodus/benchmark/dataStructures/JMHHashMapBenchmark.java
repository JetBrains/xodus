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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "UnusedDeclaration"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHHashMapBenchmark {

    private static final int MAP_SIZE = 100000;

    final Map<Integer, String> map = createHashMap();
    int existingKey = 0;
    int missingKey = MAP_SIZE;

    @Setup
    public void prepare() {
        for (int i = 0; i < MAP_SIZE; ++i) {
            map.put(i, Integer.toString(i));
        }
    }

    @Setup(Level.Invocation)
    public void changeIndex() {
        if (++existingKey == MAP_SIZE) {
            existingKey = 0;
        }
        ++missingKey;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String hashMapGet() {
        return map.get(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public String hashMapGetMissingKey() {
        return map.get(missingKey);
    }

    protected Map<Integer, String> createHashMap() {
        return new HashMap<>();
    }
}
