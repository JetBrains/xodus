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
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;

public class JETokyoCabinetLikeBenchmarkTest extends BenchmarkTestBase {

    private DatabaseEntry[] keys;
    private Environment env;

    @Before
    public void generateKeys() throws IOException {
        createEnvironment();
        DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        keys = new DatabaseEntry[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            final DatabaseEntry entry = new DatabaseEntry();
            StringBinding.stringToEntry(FORMAT.format(i), entry);
            keys[i] = entry;
        }
    }

    @Test
    public void testWrite() {
        final Database store = createTestStore();
        long time = TestUtil.time("Write", new Runnable() {
            @Override
            public void run() {
                fillStore(store);
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("JEWriteTokyoTest", time);
        }
    }

    @Test
    public void testRandomWrite() {
        shuffleKeys();
        final Database store = createTestStore();
        long time = TestUtil.time("Random write", new Runnable() {
            @Override
            public void run() {
                fillStore(store);
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("JERandomWriteTokyoTest", time);
        }
    }

    @Test
    public void testRead() {
        final Database store = createTestStore();
        fillStore(store);
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Read", new Runnable() {
                    @Override
                    public void run() {
                        doRead(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("JEReadTokyoTest", time);
                }
                return null;
            }
        });
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Read again", new Runnable() {
                    @Override
                    public void run() {
                        doRead(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("JEReadTokyoTest2", time);
                }
                return null;
            }
        });
    }

    @Test
    public void testReadRandom() {
        final Database store = createTestStore();
        fillStore(store);
        shuffleKeys();
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Random read", new Runnable() {
                    @Override
                    public void run() {
                        doReadRandom(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("JERandomReadTokyoTest", time);
                }
                return null;
            }
        });
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Random read again", new Runnable() {
                    @Override
                    public void run() {
                        doReadRandom(store, txn);
                    }
                });
                final String envRandomReadTokyoTest2 = "JERandomReadTokyoTest2";
                if (myMessenger != null) {
                    myMessenger.putValue(envRandomReadTokyoTest2, time);
                }
                return null;
            }
        });
    }

    private void doRead(@NotNull final Database store, @NotNull final Transaction txn) {
        try (Cursor c = store.openCursor(txn, null)) {
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();
            while (c.getNext(key, value, null) == OperationStatus.SUCCESS) {
                key.getData();
                value.getData();
            }
        }
    }

    private void doReadRandom(@NotNull final Database store, @NotNull final Transaction txn) {
        try (Cursor c = store.openCursor(txn, null)) {
            for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                final DatabaseEntry key = new DatabaseEntry(keys[i].getData());
                final DatabaseEntry value = new DatabaseEntry();
                c.getSearchKey(key, value, null);
                key.getData();
                value.getData();
            }
        }
    }

    private void createEnvironment() throws IOException {
        if (env != null) {
            env.close();
        }
        final EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        env = new Environment(temporaryFolder.newFolder("data"), environmentConfig);
    }

    private void shuffleKeys() {
        Collections.shuffle(Arrays.asList(keys));
    }

    private Database createTestStore() {
        return computeInTransaction(new TransactionalComputable<Database>() {
            @Override
            public Database compute(@NotNull Transaction txn) {
                final DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                return env.openDatabase(null, "testTokyoCabinet", dbConfig);
            }
        });
    }

    private void fillStore(@NotNull final Database store) {
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull Transaction txn) {
                for (final DatabaseEntry key : keys) {
                    store.put(txn, key, key);
                }
                return null;
            }
        });
    }

    private interface TransactionalComputable<T> {

        T compute(@NotNull final Transaction txn);
    }

    private <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        final Transaction txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        try {
            return computable.compute(txn);
        } finally {
            txn.commit();
        }
    }
}
