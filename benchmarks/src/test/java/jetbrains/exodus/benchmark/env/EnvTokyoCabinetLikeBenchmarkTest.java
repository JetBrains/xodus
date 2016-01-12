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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.*;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;

public class EnvTokyoCabinetLikeBenchmarkTest extends BenchmarkTestBase {

    private ByteIterable[] keys;
    private Environment env;

    @Before
    public void generateKeys() throws IOException {
        Log.invalidateSharedCache();
        createEnvironment();
        DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        keys = new ByteIterable[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            keys[i] = StringBinding.stringToEntry(FORMAT.format(i));
        }
    }

    protected void shuffleKeys() {
        Collections.shuffle(Arrays.asList(keys));
    }

    @Test
    public void testWrite() {
        final Store store = openStoreAutoCommit("testTokyoCabinet", getStoreConfiguration());
        long time = TestUtil.time("Write", new Runnable() {
            @Override
            public void run() {
                fillStore(store);
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("EnvWriteTokyoTest", time);
        }
    }

    protected Store openStoreAutoCommit(final String storeName, final StoreConfig configuration) {
        return env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore(storeName, configuration, txn);
            }
        });
    }

    @Test
    public void testRandomWrite() {
        shuffleKeys();
        final Store store = openStoreAutoCommit("testTokyoCabinet", getStoreConfiguration());
        long time = TestUtil.time("Random write", new Runnable() {
            @Override
            public void run() {
                fillStore(store);
            }
        });
        if (myMessenger != null) {
            myMessenger.putValue("EnvRandomWriteTokyoTest", time);
        }
    }

    @Test
    public void testRead() {
        final Store store = openStoreAutoCommit("testTokyoCabinet", getStoreConfiguration());
        fillStore(store);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Read", new Runnable() {
                    @Override
                    public void run() {
                        doRead(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("EnvReadTokyoTest", time);
                }
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Read again", new Runnable() {
                    @Override
                    public void run() {
                        doRead(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("EnvReadTokyoTest2", time);
                }
            }
        });
    }

    private void doRead(@NotNull final Store store, @NotNull final Transaction txn) {
        final Cursor c = store.openCursor(txn);
        while (c.getNext()) {
            iterate(c.getKey());
            iterate(c.getValue());
        }
        c.close();
    }

    @Test
    public void testReadRandom() {
        final Store store = openStoreAutoCommit("testTokyoCabinet", getStoreConfiguration());
        fillStore(store);
        shuffleKeys();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Random read", new Runnable() {
                    @Override
                    public void run() {
                        doReadRandom(store, txn);
                    }
                });
                if (myMessenger != null) {
                    myMessenger.putValue("EnvRandomReadTokyoTest", time);
                }
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long time = TestUtil.time("Random read again", new Runnable() {
                    @Override
                    public void run() {
                        doReadRandom(store, txn);
                    }
                });
                final String envRandomReadTokyoTest2 = "EnvRandomReadTokyoTest2";
                if (myMessenger != null) {
                    myMessenger.putValue(envRandomReadTokyoTest2, time);
                }
            }
        });
    }

    private void doReadRandom(@NotNull final Store store, @NotNull final Transaction txn) {
        final Cursor c = store.openCursor(txn);
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            c.getSearchKey(keys[i]);
            iterate(c.getKey());
            iterate(c.getValue());
        }
        c.close();
    }

    private void fillStore(@NotNull final Store store) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                    store.add(txn, keys[i], keys[i]);
                }
            }
        });
    }

    private static void iterate(final ByteIterable iter) {
        final ByteIterator i = iter.iterator();
        while (i.hasNext()) i.next();
    }

    protected Pair<DataReader, DataWriter> createRW() throws IOException {
        final File testsDirectory = temporaryFolder.newFolder("data");

        return new Pair<DataReader, DataWriter>(
                new FileDataReader(testsDirectory, 16),
                new FileDataWriter(testsDirectory)
        );
    }

    protected void createEnvironment() throws IOException {
        if (env != null) {
            env.close();
        }
        final Pair<DataReader, DataWriter> rw = createRW();

        env = Environments.newInstance(LogConfig.create(rw.getFirst(), rw.getSecond()));
    }

    protected StoreConfig getStoreConfiguration() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }
}
