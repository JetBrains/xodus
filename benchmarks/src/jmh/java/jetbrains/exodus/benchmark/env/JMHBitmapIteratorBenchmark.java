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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.env.*;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Thread)
public class JMHBitmapIteratorBenchmark {

    public TemporaryFolder temporaryFolder;
    public Environment env;
    public BitmapImpl store;

    @SuppressWarnings("deprecation") // Log.invalidateSharedCache is test-only
    @Setup(Level.Iteration)
    public void setup() throws IOException {
        Log.invalidateSharedCache();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        env = Environments.newInstance(testsDirectory,
                adjustEnvironmentConfig(new EnvironmentConfig().setLogFileSize(32768)));
        store = (BitmapImpl) env.computeInTransaction(txn -> env.openBitmap("JHMBitMapBench", StoreConfig.WITHOUT_DUPLICATES, txn));

        env.computeInTransaction(txn -> {
            for (int i = 0; i < 100000; i++) {
                store.set(txn, i * ThreadLocalRandom.current().nextInt(3), ThreadLocalRandom.current().nextBoolean());
            }
            return null;
        });

    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        if (env != null) {
            env.close();
            env = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    @SuppressWarnings("unused")
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public long benchIterate() {
        long v = 0;
        Transaction txn = this.env.beginReadonlyTransaction();
        try {

            try (BitmapIterator iter = store.iterator(txn)) {
                while (iter.hasNext()) {
                    v += iter.nextLong();
                }
            }
        } finally {
            txn.abort();
        }
        return v;
    }

    private EnvironmentConfig adjustEnvironmentConfig(@NotNull EnvironmentConfig ec) {
        ec.setUseVersion1Format(false);

        return ec;
    }
}
