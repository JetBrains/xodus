/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.benchmark.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"UnusedDeclaration"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JMHPackedLongHashSetBenchmark {

    protected static final int SET_SIZE = 100000;

    final PackedLongHashSet setForContains = new PackedLongHashSet();


    final long[] testData = new long[10000];

    @Setup
    public void prepare() {
        Random r = new Random();
        for (int i = 0; i < SET_SIZE; i++) {
            setForContains.add(r.nextLong());
        }

        for (int i = 0; i < testData.length; i++) {
            testData[i] = r.nextLong();
        }
    }


    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public Object tryAdd() {
        final PackedLongHashSet setForAdd = new PackedLongHashSet();
        for (long testDatum : testData) {
            setForAdd.add(testDatum);
        }
        return setForAdd;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean tryContains() {
        boolean c = true;
        for (long testDatum : testData) {
            c ^= setForContains.contains(testDatum);
        }
        return c;
    }
}
