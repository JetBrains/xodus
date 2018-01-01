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

import static jetbrains.exodus.benchmark.dataStructures.persistent.JMHPersistentLongSetBenchmark.MAP_SIZE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHPersistentLongSetFillBenchmark {
    private final PersistentLongSet treeSet = new PersistentLong23TreeSet();
    private final PersistentLongSet bitTreeSet = new PersistentBitTreeLongSet();

    private final TreeMap<Long, Object> juTree = new TreeMap<>();
    private final Object value = new Object();

    private final long[] keys = new long[(int) MAP_SIZE];

    @Setup
    public void prepare() {
        for (int i = 0; i < MAP_SIZE; ++i) {
            keys[i] = (long) (i * 2);
        }
    }


    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object fillTreeMap() {
        for (int i = 0; i < MAP_SIZE; ++i) {
            juTree.put(keys[i], value);
        }
        return juTree;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object fill23Tree() {
        final PersistentLongSet.MutableSet mutableTreeSet = treeSet.beginWrite();
        for (int i = 0; i < MAP_SIZE; ++i) {
            mutableTreeSet.add(keys[i]);
        }
        return mutableTreeSet;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object fillBitTree() {
        final PersistentLongSet.MutableSet mutableTreeSet = bitTreeSet.beginWrite();
        for (int i = 0; i < MAP_SIZE; ++i) {
            mutableTreeSet.add(keys[i]);
        }
        return mutableTreeSet;
    }
}
