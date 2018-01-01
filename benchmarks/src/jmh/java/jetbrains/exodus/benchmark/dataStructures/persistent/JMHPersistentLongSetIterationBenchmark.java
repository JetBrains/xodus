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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet;
import org.openjdk.jmh.annotations.*;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.dataStructures.persistent.JMHPersistentLongSetBenchmark.MAP_SIZE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JMHPersistentLongSetIterationBenchmark {
    private final PersistentLongSet treeSet = new PersistentLong23TreeSet();
    private final PersistentLongSet bitTreeSet = new PersistentBitTreeLongSet();

    private final TreeMap<Long, Object> juTree = new TreeMap<>();
    private final Object value = new Object();

    @Setup
    public void prepare() {
        final PersistentLongSet.MutableSet mutableTreeSet = treeSet.beginWrite();
        final PersistentLongSet.MutableSet mutableBitSet = bitTreeSet.beginWrite();
        for (int i = 0; i < MAP_SIZE; ++i) {
            // the keys are even
            mutableTreeSet.add((long) (i * 2));
            mutableBitSet.add((long) (i * 2));
            juTree.put((long) (i * 2), value);
        }
        mutableTreeSet.endWrite();
        mutableBitSet.endWrite();
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public long iterate23Tree() {
        LongIterator iterator = treeSet.beginRead().longIterator();
        long result = 0;
        while (iterator.hasNext()) {
            result += iterator.nextLong();
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public long iterateBitTree() {
        LongIterator iterator = bitTreeSet.beginRead().longIterator();
        long result = 0;
        while (iterator.hasNext()) {
            result += iterator.nextLong();
        }
        return result;
    }

    @Benchmark
    @Warmup(iterations = 6, time = 1)
    @Measurement(iterations = 8, time = 1)
    @Fork(5)
    public long iterateTreeMap() {
        Iterator<Long> iterator = juTree.keySet().iterator();
        long result = 0;
        while (iterator.hasNext()) {
            result += iterator.next();
        }
        return result;
    }
}
