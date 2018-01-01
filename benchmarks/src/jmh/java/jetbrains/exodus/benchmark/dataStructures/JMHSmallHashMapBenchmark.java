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

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHSmallHashMapBenchmark {
    private static final Object VALUE = new Object();

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public HashMap<String, Object> newXodusMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("foo", VALUE);
        map.put("bar", true);
        return map;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public java.util.HashMap<String, Object> newJdkMap() {
        java.util.HashMap<String, Object> map = new java.util.HashMap<>();
        map.put("foo", VALUE);
        map.put("bar", true);
        return map;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public HashMap<String, Object> newEmptyXodusMap() {
        return new HashMap<>();
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public java.util.HashMap<String, Object> newEmptyJdkMap() {
        return new java.util.HashMap<>();
    }
}
