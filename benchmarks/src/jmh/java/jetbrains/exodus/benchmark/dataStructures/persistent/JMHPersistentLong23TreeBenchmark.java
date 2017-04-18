/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeMap;
import org.openjdk.jmh.annotations.*;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHPersistentLong23TreeBenchmark {

    private static final int MAP_SIZE = 100000;

    private final PersistentLong23TreeMap<Object> tree = new PersistentLong23TreeMap();
    private final TreeMap<Long, Object> juTree = new TreeMap<>();
    private final Object value = new Object();
    private long existingKey = 0;
    private long missingKey = MAP_SIZE;

    @Setup
    public void prepare() {
        final PersistentLong23TreeMap<Object>.MutableMap mutableMap = tree.beginWrite();
        for (int i = 0; i < MAP_SIZE; ++i) {
            // the keys are even
            mutableMap.put((long) (i * 2), value);
            juTree.put((long) (i * 2), value);
        }
        mutableMap.endWrite();
    }

    @Setup(Level.Invocation)
    public void prepareKeys() {
        // the even key exists in the map, the odd one doesn't
        existingKey = (long) ((Math.random() * MAP_SIZE) * 2);
        missingKey = existingKey + 1;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object getExisting() {
        return tree.beginRead().get(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object getMissing() {
        return tree.beginRead().get(missingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object treeMapGetExisting() {
        return juTree.get(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object treeMapGetMissing() {
        return juTree.get(missingKey);
    }
}
