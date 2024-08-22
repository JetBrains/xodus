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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.StoreConfig;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.env.JMHEnvWithPrefixingRandomAccessWriteBenchmarkV1.*;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)

public class JMHEnvWithPrefixingRandomAccessReadBenchmarkV1 extends JMHEnvBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        shuffleKeys();
        setup();
        writeChunked(env, store);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void read() {
        env.executeInReadonlyTransaction(txn -> {
            for (int key : keys) {
                store.get(txn, IntegerBinding.intToEntry(key));
            }
        });
    }

    @Override
    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
    }

    @Override
    protected EnvironmentConfig adjustEnvironmentConfig(@NotNull EnvironmentConfig ec) {
        return ec.setGcEnabled(false).setUseVersion1Format(true);
    }
}
