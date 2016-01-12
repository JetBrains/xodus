/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.je;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.*;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;

public abstract class JMH_JETokyoCabinetBenchmarkBase extends BenchmarkTestBase {

    protected static final DatabaseEntry[] successiveKeys;
    protected static final DatabaseEntry[] randomKeys;

    static {
        final DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        successiveKeys = new DatabaseEntry[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            final DatabaseEntry key = new DatabaseEntry();
            StringBinding.stringToEntry(FORMAT.format(i), key);
            successiveKeys[i] = key;
        }
        randomKeys = Arrays.copyOf(successiveKeys, successiveKeys.length);
        shuffleKeys();
    }

    protected Environment env;
    protected Database store;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        start();
        shuffleKeys();
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        env = new Environment(temporaryFolder.newFolder("data"), environmentConfig);
        store = computeInTransaction(new TransactionalComputable<Database>() {
            @Override
            public Database compute(@NotNull Transaction txn) {
                final DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbConfig.setKeyPrefixing(isKeyPrefixing());
                return env.openDatabase(null, "testTokyoCabinet", dbConfig);
            }
        });
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        store.close();
        env.close();
        end();
    }

    protected void writeSuccessiveKeys() {
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull Transaction txn) {
                for (final DatabaseEntry key : successiveKeys) {
                    store.put(txn, key, key);
                }
                return null;
            }
        });
    }

    protected static void shuffleKeys() {
        Collections.shuffle(Arrays.asList(randomKeys));
    }

    protected interface TransactionalComputable<T> {

        T compute(@NotNull final Transaction txn);
    }

    protected <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        final Transaction txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        try {
            return computable.compute(txn);
        } finally {
            txn.commit();
        }
    }

    protected abstract boolean isKeyPrefixing();
}
