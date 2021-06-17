/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvWithPrefixingRandomAccessWriteBenchmarkV1 extends JMHEnvBenchmarkBase {

    static final int WARMUP_ITERATIONS = 5;
    static final int MEASUREMENT_ITERATIONS = 5;
    static final int FORKS = 1;
    static final int KEYS_COUNT = 10_000_000;
    static final int CHUNK_SIZE = 10_000;
    static final int[] keys = new int[KEYS_COUNT];

    static {
        for (int i = 0; i < keys.length; i++) {
            keys[i] = i;
        }
    }

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        shuffleKeys();
        setup();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void write() {
        writeChunked(env, store);
    }

    @Override
    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
    }

    @Override
    protected EnvironmentConfig adjustEnvironmentConfig(@NotNull EnvironmentConfig ec) {
        return ec.setGcEnabled(false).setUseVersion1Format(true);
    }

    static void shuffleKeys() {
        Collections.shuffle(Arrays.asList(keys));
    }

    static void writeChunked(Environment env, Store store) {
        for (int i = 0; i < KEYS_COUNT / CHUNK_SIZE; ++i) {
            final int base = i * CHUNK_SIZE;
            env.executeInTransaction(txn -> {
                for (int j = 0; j < CHUNK_SIZE; ++j) {
                    final ArrayByteIterable entry = IntegerBinding.intToEntry(keys[base + j]);
                    store.put(txn, entry, entry);
                }
            });
        }
    }
}
