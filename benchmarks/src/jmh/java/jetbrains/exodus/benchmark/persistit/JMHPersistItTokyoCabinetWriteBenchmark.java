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
package jetbrains.exodus.benchmark.persistit;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.FORKS;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.MEASUREMENT_ITERATIONS;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.WARMUP_ITERATIONS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHPersistItTokyoCabinetWriteBenchmark extends JMHPersistItTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException, PersistitException {
        setup();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveWrite() throws PersistitException {
        writeSuccessiveKeys(createTestStore());
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomWrite() throws PersistitException {
        final Exchange store = createTestStore();
        for (final ByteIterable key : randomKeys) {
            store.clear();
            ByteIterator it = key.iterator();
            while (it.hasNext()) {
                store.append(it.next());
            }
            store.getValue().put(key.getBytesUnsafe());
            store.store();
        }
        persistit.flush();
    }
}
