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
package jetbrains.exodus.benchmark.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet;
import org.openjdk.jmh.annotations.*;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHPersistentLongSetBenchmark {
    static final long MAP_SIZE = 100000;

    private final PersistentLongSet treeSet = new PersistentLong23TreeSet();
    private final PersistentLongSet bitTreeSet = new PersistentBitTreeLongSet();

    private final TreeMap<Long, Object> juTree = new TreeMap<>();
    private final Object value = new Object();

    private long existingKey = 0;
    private long missingKey = MAP_SIZE;

    @Setup
    public void prepare() {
        final PersistentLongSet.MutableSet mutableTreeSet = treeSet.beginWrite();
        final PersistentLongSet.MutableSet mutableBitSet = bitTreeSet.beginWrite();
        for (long i = 0; i < MAP_SIZE; ++i) {
            // the keys are even
            mutableTreeSet.add(i * 2);
            mutableBitSet.add(i * 2);
            juTree.put(i * 2, value);
        }
        mutableTreeSet.endWrite();
        mutableBitSet.endWrite();
    }

    @Setup(Level.Invocation)
    public void prepareKeys() {
        // the even key exists in the map, the odd one doesn't
        existingKey = (long) ((Math.random() * MAP_SIZE) * 2);
        missingKey = existingKey + 1;
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean get23TreeExisting() {
        return treeSet.beginRead().contains(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean get23TreeMissing() {
        return treeSet.beginRead().contains(missingKey);
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean getBitTreeExisting() {
        return bitTreeSet.beginRead().contains(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean getBitTreeMissing() {
        return bitTreeSet.beginRead().contains(missingKey);
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean treeMapGetExisting() {
        return juTree.containsKey(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public boolean treeMapGetMissing() {
        return juTree.containsKey(missingKey);
    }
}
