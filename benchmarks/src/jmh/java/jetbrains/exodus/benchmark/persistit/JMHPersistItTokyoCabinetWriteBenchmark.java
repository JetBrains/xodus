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
package jetbrains.exodus.benchmark.persistit;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import jetbrains.exodus.ByteIterable;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;

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
        final Exchange exchange = createTestStore();
        for (final ByteIterable key : randomKeys) {
            exchange.clear();
            for (int i = 0; i < key.getLength(); i++) {
                exchange.append(key.getBytesUnsafe()[i]);
            }
            exchange.getValue().put(key.getBytesUnsafe());
            exchange.store();
        }
        persistit.flush();
    }
}
