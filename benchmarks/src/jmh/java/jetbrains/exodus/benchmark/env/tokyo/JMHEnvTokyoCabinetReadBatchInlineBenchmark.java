/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.benchmark.env.tokyo;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.StoreConfig;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.FORKS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvTokyoCabinetReadBatchInlineBenchmark extends JMHEnvTokyoCabinetBenchmarkBase {
    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveRead(final Blackhole bh) {
        env.executeInReadonlyTransaction(txn -> {
            try (Cursor c = store.openCursor(txn)) {
                while (c.getNext()) {
                    consumeBytes(bh, c.getKey());
                    consumeBytes(bh, c.getValue());
                }
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomRead(final Blackhole bh) {
        env.executeInReadonlyTransaction(txn -> {
            try (Cursor c = store.openCursor(txn)) {
                for (final ByteIterable key : randomKeys) {
                    c.getSearchKey(key);
                    consumeBytes(bh, c.getValue());
                }
            }
        });
    }

    @Override
    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_INLINE;
    }

    private static void consumeBytes(final Blackhole bh, final ByteIterable it) {
        final ByteIterator iterator = it.iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }
}
