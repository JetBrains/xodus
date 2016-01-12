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
package jetbrains.exodus.benchmark.mapdb;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class MapDBTokyoCabinetLikeBenchmarkTest extends BenchmarkTestBase {

    private String[] keys;
    private TxMaker txMaker;

    @Before
    public void generateKeys() throws IOException {
        createEnvironment();
        DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        keys = new String[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            keys[i] = FORMAT.format(i);
        }
    }

    @Test
    public void testWrite() {
        long time = TestUtil.time("Write", new Runnable() {
            @Override
            public void run() {
                fillStore();
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBWriteTokyoTest", time);
        }
    }

    @Test
    public void testRandomWrite() {
        shuffleKeys();
        long time = TestUtil.time("Random write", new Runnable() {
            @Override
            public void run() {
                fillStore();
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBRandomWriteTokyoTest", time);
        }
    }

    @Test
    public void testRead() {
        fillStore();
        long time = TestUtil.time("Read", new Runnable() {
            @Override
            public void run() {
                computeInTransaction(new TransactionalComputable() {
                    @Override
                    public Object compute(@NotNull DB db) {
                        doRead(createTestStore(db));
                        return null;
                    }
                });
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBReadTokyoTest", time);
        }
        time = TestUtil.time("Read again", new Runnable() {
            @Override
            public void run() {
                computeInTransaction(new TransactionalComputable() {
                    @Override
                    public Object compute(@NotNull DB db) {
                        doRead(createTestStore(db));
                        return null;
                    }
                });
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBReadTokyoTest2", time);
        }
    }

    @Test
    public void testReadRandom() {
        fillStore();
        long time = TestUtil.time("Random read", new Runnable() {
            @Override
            public void run() {
                computeInTransaction(new TransactionalComputable() {
                    @Override
                    public Object compute(@NotNull DB db) {
                        doReadRandom(createTestStore(db));
                        return null;
                    }
                });
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBRandomReadTokyoTest", time);
        }
        time = TestUtil.time("Random read again", new Runnable() {
            @Override
            public void run() {
                computeInTransaction(new TransactionalComputable() {
                    @Override
                    public Object compute(@NotNull DB db) {
                        doReadRandom(createTestStore(db));
                        return null;
                    }
                });
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("MapDBRandomReadTokyoTest2", time);
        }
    }

    private void doRead(@NotNull final Map<Object, Object> store) {
        for (Map.Entry entry : store.entrySet()) {
            Assert.assertEquals(entry.getKey(), entry.getValue());
        }
    }

    private void doReadRandom(@NotNull final Map<Object, Object> store) {
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            final String key = keys[i];
            Assert.assertEquals(key, store.get(key));
        }
    }

    private void createEnvironment() throws IOException {
        if (txMaker != null) {
            txMaker.close();
        }
        txMaker = DBMaker.newFileDB(temporaryFolder.newFile("data")).makeTxMaker();
    }

    private void shuffleKeys() {
        Collections.shuffle(Arrays.asList(keys));
    }

    private Map<Object, Object> createTestStore(@NotNull final DB db) {
        return db.getTreeMap("testTokyoCabinet");
    }

    private void fillStore(@NotNull final Map<Object, Object> store) {
        for (final String key : keys) {
            store.put(key, key);
        }
    }

    private void fillStore() {
        computeInTransaction(new TransactionalComputable() {
            @Override
            public Object compute(@NotNull DB db) {
                fillStore(createTestStore(db));
                return null;
            }
        });
    }

    private interface TransactionalComputable<T> {

        T compute(@NotNull final DB db);
    }

    private <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        final DB db = txMaker.makeTx();
        try {
            return computable.compute(db);
        } finally {
            db.commit();
        }
    }
}

