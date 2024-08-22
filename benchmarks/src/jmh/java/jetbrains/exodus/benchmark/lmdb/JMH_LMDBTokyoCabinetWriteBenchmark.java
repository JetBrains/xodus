/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.lmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Transaction;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_LMDBTokyoCabinetWriteBenchmark extends JMH_LMDBTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveWrite() {
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomWrite() {
        try (Transaction txn = env.createWriteTransaction()) {
            try (BufferCursor c = db.bufferCursor(txn)) {
                for (final byte[] key : randomKeys) {
                    c.keyWriteBytes(key);
                    c.valWriteBytes(key);
                    c.put();
                }
            }
            txn.commit();
        }
    }
}
