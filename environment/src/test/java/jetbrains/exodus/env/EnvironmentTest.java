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
package jetbrains.exodus.env;

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.execution.locks.Latch;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.tree.btree.BTreeBase;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.*;

public class EnvironmentTest extends EnvironmentTestsBase {

    private final Map<String, File> subfolders = new HashMap<>();

    @Test
    public void testEmptyEnvironment() {
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testStatisticsBytesWritten() {
        testEmptyEnvironment();
        Assert.assertTrue(env.getStatistics().getStatisticsItem(BYTES_WRITTEN).getTotal() > 0L);
    }

    @Test
    public void testCreateSingleStore() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT,
            DatabaseRoot.DATABASE_ROOT_TYPE, BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF,
            BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testStatisticsTransactions() {
        testCreateSingleStore();
        final EnvironmentStatistics statistics = env.getStatistics();
        Assert.assertTrue(statistics.getStatisticsItem(TRANSACTIONS).getTotal() > 0L);
        Assert.assertTrue(statistics.getStatisticsItem(FLUSHED_TRANSACTIONS).getTotal() > 0L);
    }

    @Test
    public void testStatisticsItemNames() {
        testStatisticsTransactions();
        final EnvironmentStatistics statistics = env.getStatistics();
        Assert.assertNotNull(statistics.getStatisticsItem(BYTES_WRITTEN));
        Assert.assertNotNull(statistics.getStatisticsItem(BYTES_READ));
        Assert.assertNotNull(statistics.getStatisticsItem(BYTES_MOVED_BY_GC));
        Assert.assertNotNull(statistics.getStatisticsItem(TRANSACTIONS));
        Assert.assertNotNull(statistics.getStatisticsItem(READONLY_TRANSACTIONS));
        Assert.assertNotNull(statistics.getStatisticsItem(ACTIVE_TRANSACTIONS));
        Assert.assertNotNull(statistics.getStatisticsItem(FLUSHED_TRANSACTIONS));
        Assert.assertNotNull(statistics.getStatisticsItem(DISK_USAGE));
        Assert.assertNotNull(statistics.getStatisticsItem(UTILIZATION_PERCENT));
        Assert.assertNotNull(statistics.getStatisticsItem(LOG_CACHE_HIT_RATE));
        Assert.assertNotNull(statistics.getStatisticsItem(STORE_GET_CACHE_HIT_RATE));
    }

    @Test
    public void testFirstLastLoggables() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        final Log log = getLog();
        Loggable l = log.getFirstLoggableOfType(BTreeBase.BOTTOM_ROOT);
        Assert.assertNotNull(l);
        Assert.assertEquals(0L, l.getAddress());
        l = log.getLastLoggableOfType(BTreeBase.BOTTOM_ROOT);
        Assert.assertNotNull(l);
        Assert.assertEquals(40L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress());
        Assert.assertNotNull(l);
        Assert.assertEquals(12L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress());
        Assert.assertNotNull(l);
        Assert.assertEquals(0L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress());
        Assert.assertNull(l);
        l = log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, Long.MAX_VALUE);
        Assert.assertNotNull(l);
        Assert.assertEquals(48L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, l.getAddress());
        Assert.assertNotNull(l);
        l = log.getFirstLoggableOfType(DatabaseRoot.DATABASE_ROOT_TYPE);
        Assert.assertNotNull(l);
    }

    @Test
    public void testClear() {
        env.getEnvironmentConfig().setGcEnabled(false);
        testCreateSingleStore();
        getEnvironment().clear();
        testEmptyEnvironment();
        getEnvironment().clear();
        testFirstLastLoggables();
        getEnvironment().clear();
        testEmptyEnvironment();
    }

    @Test
    public void testClearMoreThanOneFile() {
        setLogFileSize(1);
        env.getEnvironmentConfig().setGcEnabled(false);
        testGetAllStoreNames();
        getEnvironment().clear();
        testEmptyEnvironment();
    }

    @Test
    @TestFor(issues = "XD-457")
    public void testClearWithTransaction_XD_457() throws InterruptedException {
        final Latch latch = Latch.create();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final StoreImpl store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                store.put(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0"));
                Assert.assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
                final Throwable[] th = {null};
                // asynchronously clear the environment
                try {
                    latch.acquire();
                    runParallelRunnable(new Runnable() {
                        @Override
                        public void run() {
                            latch.release();
                            try {
                                env.clear();
                            } catch (Throwable t) {
                                th[0] = t;
                            }
                            latch.release();
                        }
                    });
                    latch.acquire();
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                    Assert.assertTrue(false);
                }
                Assert.assertNull(th[0]);
                Assert.assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
            }
        });
        latch.acquire();
        env.executeInExclusiveTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final StoreImpl store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                Assert.assertFalse(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
            }
        });
    }

    @Test
    public void testCloseTwice() {
        final int count = 100;
        final Transaction txn = env.beginTransaction();
        for (int i = 0; i < count; ++i) {
            env.openStore("new_store" + i, StoreConfig.WITHOUT_DUPLICATES, txn);
        }
        txn.commit();
        final EnvironmentConfig envConfig = env.getEnvironmentConfig();
        env.close();
        try {
            Assert.assertFalse(env.isOpen());
            env.close();
        } finally {
            // forget old env anyway to prevent tearDown fail
            env = newEnvironmentInstance(LogConfig.create(reader, writer), envConfig);
        }
    }

    @Test
    public void testGetAllStoreNames() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final int count = 100;
        for (int i = 0; i < count; ++i) {
            env.openStore("new_store" + i, StoreConfig.WITHOUT_DUPLICATES, txn);
        }
        final List<String> stores = env.getAllStoreNames(txn);
        txn.commit();
        Assert.assertEquals(count, stores.size());
        // list is sorted by name in lexicographical order
        final Set<String> names = new TreeSet<>();
        for (int i = 0; i < count; ++i) {
            names.add("new_store" + i);
        }
        int i = 0;
        for (final String name : names) {
            Assert.assertEquals(name, stores.get(i++));
        }
    }

    @Test
    public void testUpdateBalancePolicy() {
        EnvironmentConfig envConfig = env.getEnvironmentConfig();
        final BTreeBalancePolicy bs = env.getBTreeBalancePolicy();
        final ArrayByteIterable wtf = StringBinding.stringToEntry("wtf");
        final int count = bs.getPageMaxSize() - 1;
        {
            final Transaction txn = env.beginTransaction();
            try {
                final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                for (int i = 0; i < count; ++i) {
                    store.put(txn, IntegerBinding.intToEntry(i), wtf);
                }
            } finally {
                txn.commit();
            }
        }
        env.close();
        env = newEnvironmentInstance(LogConfig.create(reader, writer), envConfig.setTreeMaxPageSize(count / 2));
        final Transaction txn = env.beginTransaction();
        try {
            Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
            store.put(txn, IntegerBinding.intToEntry(count), wtf);
        } finally {
            txn.commit();
        }
    }

    @Test
    public void testReopenEnvironment() {
        testGetAllStoreNames();
        reopenEnvironment();
        testGetAllStoreNames();
    }

    @Test
    public void testReopenEnvironment2() {
        testGetAllStoreNames();
        final EnvironmentConfig envConfig = env.getEnvironmentConfig();
        env.close();
        final long highAddress = env.getLog().getHighAddress();
        env = newEnvironmentInstance(LogConfig.create(reader, writer), envConfig);
        Assert.assertEquals(highAddress, env.getLog().getHighAddress());
        testGetAllStoreNames();
    }

    @Test
    public void testBreakSavingMetaTree() {
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        if (ec.getLogCachePageSize() > 1024) {
            ec.setLogCachePageSize(1024);
        }
        ec.setTreeMaxPageSize(16);
        Log.invalidateSharedCache();
        reopenEnvironment();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final StoreImpl store1 = env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
                final StoreImpl store2 = env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
                final StoreImpl store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
                final StoreImpl store4 = env.openStore("store4", StoreConfig.WITHOUT_DUPLICATES, txn);
                store4.put(txn, IntegerBinding.intToCompressedEntry(0), IntegerBinding.intToCompressedEntry(0));
                for (int i = 0; i < 16; ++i) {
                    store1.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                    store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                    store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                }
            }
        });
        reopenEnvironment();
        final LogTestConfig testConfig = new LogTestConfig();
        testConfig.setMaxHighAddress(1024 * 10 + 3);
        testConfig.setSettingHighAddressDenied(true);
        env.getLog().setLogTestConfig(testConfig);
        try {
            for (int i = 0; i < 23; ++i) {
                env.executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        final StoreImpl store1 = env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
                        final StoreImpl store2 = env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
                        final StoreImpl store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
                        for (int i = 0; i < 13; ++i) {
                            store1.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                            store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                            store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                        }
                    }
                });
            }
            TestUtil.runWithExpectedException(new Runnable() {
                @Override
                public void run() {
                    env.executeInTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final Transaction txn) {
                            final StoreImpl store1 = env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
                            final StoreImpl store2 = env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
                            final StoreImpl store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
                            for (int i = 0; i < 13; ++i) {
                                store1.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                                store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                                store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                            }
                        }
                    });
                }
            }, ExodusException.class);
            env.getLog().setLogTestConfig(null);
            AbstractConfig.suppressConfigChangeListenersForThread();
            try {
                ec.setEnvIsReadonly(true);
                reopenEnvironment();
            } finally {
                AbstractConfig.resumeConfigChangeListenersForThread();
            }
            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    env.getAllStoreNames(txn);
                }
            });
        } finally {
            env.getLog().setLogTestConfig(null);
        }
    }

    @Test
    public void testUseExistingConfig() {
        final StoreConfig expectedConfig = StoreConfig.WITHOUT_DUPLICATES;
        final String name = "testDatabase";

        Transaction txn = env.beginTransaction();
        env.openStore(name, expectedConfig, txn);
        txn.commit();

        txn = env.beginTransaction();
        final Store store = env.openStore(name, StoreConfig.USE_EXISTING, txn);
        Assert.assertEquals(expectedConfig, store.getConfig());
        txn.commit();
    }

    @Test
    public void testWriteDataToSeveralFiles() throws Exception {
        setLogFileSize(16);
        env.getEnvironmentConfig().setGcEnabled(true);

        final StoreConfig expectedConfig = StoreConfig.WITHOUT_DUPLICATES;

        for (int j = 0; j < 100; j++) {
            System.out.println("Cycle " + j);
            for (int i = 0; i < 100; i++) {
                Transaction txn = env.beginTransaction();
                try {
                    while (true) {
                        final String name = "testDatabase" + j % 10;
                        final Store store = env.openStore(name, expectedConfig, txn);
                        store.put(txn, StringBinding.stringToEntry("key" + i), StringBinding.stringToEntry("value" + i));
                        if (txn.flush()) {
                            break;
                        } else {
                            txn.revert();
                        }
                    }
                } finally {
                    txn.abort();
                }
            }
            Thread.yield();
        }
    }

    @Test
    public void testSetHighAddress() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value1"));
            }
        });
        final long highAddress = env.getLog().getHighAddress();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value2"));
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                Assert.assertEquals(StringBinding.stringToEntry("value2"), store.get(txn, StringBinding.stringToEntry("key")));
            }
        });
        env.setHighAddress(highAddress);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                Assert.assertEquals(StringBinding.stringToEntry("value1"), store.get(txn, StringBinding.stringToEntry("key")));
            }
        });
    }

    @Test
    @TestFor(issues = "XD-590")
    public void issueXD_590_reported() {
        // 1) open store
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
        // 2) store(put) a key 1 , value A1
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("key1"), StringBinding.stringToEntry("A1"));
            }
        });
        // 3) using second transaction : store(put) key 2 value A2,  update key 1 with B1. inside transaction reload ke1 (value=B1 OK)
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("key2"), StringBinding.stringToEntry("A2"));
                store.put(txn, StringBinding.stringToEntry("key1"), StringBinding.stringToEntry("B1"));
                final ByteIterable value1 = store.get(txn, StringBinding.stringToEntry("key1"));
                Assert.assertNotNull(value1);
                Assert.assertEquals("B1", StringBinding.entryToString(value1));
            }
        });
        // 4) using third transaction : reload key 1 , value is A1 !=B1   !!!!! Error.
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final ByteIterable value1 = store.get(txn, StringBinding.stringToEntry("key1"));
                Assert.assertNotNull(value1);
                Assert.assertEquals("B1", StringBinding.entryToString(value1));
            }
        });
    }

    @Test
    @TestFor(issues = "XD-594")
    public void leakingEnvironment() throws Exception {
        tearDown();
        final WeakReference<Environment> envRef = new WeakReference<Environment>(createAndCloseEnvironment());
        waitForPendingFinalizers(10000);
        Assert.assertNull(envRef.get());
    }

    @Test
    @TestFor(issues = "XD-606")
    public void mappedFileNotUnmapped() {
        File tempDir = TestUtil.createTempDir();
        try {
            final Environment env = Environments.newInstance(tempDir, new EnvironmentConfig().setLogFileSize(1).setLogCachePageSize(1024).setLogCacheShared(false));
            final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull Transaction txn) {
                    return env.openStore("0", StoreConfig.WITHOUT_DUPLICATES, txn);
                }
            });
            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull Transaction txn) {
                    store.put(txn, StringBinding.stringToEntry("k"), StringBinding.stringToEntry("v"));
                    for (int i = 0; i < 200; ++i) {
                        store.put(txn, StringBinding.stringToEntry("k" + i), StringBinding.stringToEntry("v" + i));
                    }
                }
            });
            Assert.assertEquals("v", env.computeInTransaction(new TransactionalComputable<String>() {
                @Override
                public String compute(@NotNull Transaction txn) {
                    return StringBinding.entryToString(store.get(txn, StringBinding.stringToEntry("k")));
                }
            }));
            env.close();
            final Environment reopenedEnv = Environments.newInstance(tempDir, env.getEnvironmentConfig());
            final Store reopenedStore = reopenedEnv.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull Transaction txn) {
                    return reopenedEnv.openStore("0", StoreConfig.USE_EXISTING, txn);
                }
            });
            Assert.assertEquals("v", reopenedEnv.computeInTransaction(new TransactionalComputable<String>() {
                @Override
                public String compute(@NotNull Transaction txn) {
                    return StringBinding.entryToString(reopenedStore.get(txn, StringBinding.stringToEntry("k")));
                }
            }));
            reopenedEnv.close();
            Assert.assertTrue(new File(tempDir, LogUtil.getLogFilename(0)).renameTo(new File(tempDir, LogUtil.getLogFilename(0x1000000))));
        } finally {
            IOUtil.deleteRecursively(tempDir);
        }
    }

    @Test(expected = IllegalStateException.class)
    @TestFor(issues = "XD-628")
    public void readCloseRace() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                for (int i = 0; i < 10000; ++i) {
                    store.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(Integer.toString(i)));
                }
            }
        });
        env.getEnvironmentConfig().setEnvCloseForcedly(true);
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try (Cursor cursor = store.openCursor(txn)) {
                    final Latch latch = Latch.create();
                    try {
                        latch.acquire();
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                env.close();
                                latch.release();
                            }
                        }).run();
                        latch.acquire();
                    } catch (InterruptedException ignore) {
                    }
                    while (cursor.getNext()) {
                        Assert.assertNotNull(cursor.getKey());
                        Assert.assertNotNull(cursor.getValue());
                    }
                }
            }
        });
    }

    @Test
    public void testSharedCache() throws InterruptedException, IOException {
        env.getEnvironmentConfig().setLogCacheShared(true);
        reopenEnvironment();
        final Set<Environment> additionalEnvironments = new HashSet<>();
        try {
            env.getEnvironmentConfig().setGcEnabled(false);
            final int numberOfEnvironments = 200;
            for (int i = 0; i < numberOfEnvironments; ++i) {
                final Pair<DataReader, DataWriter> rwPair = createReaderWriter("sub" + i);
                additionalEnvironments.add(newEnvironmentInstance(LogConfig.create(rwPair.getFirst(), rwPair.getSecond()), EnvironmentConfig.DEFAULT));
            }
            final Thread[] threads = new Thread[numberOfEnvironments];
            System.out.println("create data concurrently");
            // create data concurrently
            int i = 0;
            for (final Environment env : additionalEnvironments) {
                threads[i++] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        final Transaction txn = env.beginTransaction();
                        try {
                            final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                            for (int i = 0; i < 10000; ++i) {
                                final ArrayByteIterable kv = IntegerBinding.intToEntry(i);
                                store.put(txn, kv, kv);
                            }
                        } catch (Exception e) {
                            txn.abort();
                        }
                        txn.commit();
                    }
                });
            }
            for (final Thread thread : threads) {
                thread.start();
            }
            for (final Thread thread : threads) {
                thread.join();
            }
            System.out.println("read data concurrently");
            // read data concurrently
            i = 0;
            for (final Environment env : additionalEnvironments) {
                threads[i++] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        final Transaction txn = env.beginTransaction();
                        try {
                            final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                            for (int i = 0; i < 10000; ++i) {
                                final ByteIterable bi = store.get(txn, IntegerBinding.intToEntry(i));
                                Assert.assertNotNull(bi);
                                Assert.assertEquals(i, IntegerBinding.entryToInt(bi));
                            }
                        } finally {
                            txn.abort();
                        }
                    }
                });
            }
            for (final Thread thread : threads) {
                thread.start();
            }
            for (final Thread thread : threads) {
                thread.join();
            }
            System.out.println("closing environments");
        } finally {
            for (final Environment env : additionalEnvironments) {
                env.close();
            }
        }
    }

    private Pair<DataReader, DataWriter> createReaderWriter(String subfolder) throws IOException {
        final File parent = getEnvDirectory();
        File child = subfolders.get(subfolder);
        if (child == null) {
            child = new File(parent, subfolder);
            if (child.exists()) throw new IOException("SubDirectory already exists " + subfolder);
            if (!child.mkdirs()) {
                throw new IOException("Failed to create directory " + subfolder + " for tests.");
            }
            subfolders.put(subfolder, child);
        }
        return new Pair<DataReader, DataWriter>(
            new FileDataReader(child, 16),
            new FileDataWriter(child)
        );
    }

    @Override
    public void tearDown() throws Exception {
        cleanSubfolders();
        super.tearDown();
    }

    private void cleanSubfolders() {
        for (final Map.Entry<String, File> stringFileEntry : subfolders.entrySet()) {
            File file = stringFileEntry.getValue();
            IOUtil.deleteRecursively(file);
            IOUtil.deleteFile(file);
        }
    }

    private EnvironmentImpl createAndCloseEnvironment() throws IOException {
        final Pair<DataReader, DataWriter> rw = createRW();
        final EnvironmentImpl env = newEnvironmentInstance(
            LogConfig.create(rw.getFirst(), rw.getSecond()), new EnvironmentConfig().setGcUtilizationFromScratch(true));
        env.close();
        return env;
    }

    private void waitForPendingFinalizers(final long timeoutMillis) {
        final long started = System.currentTimeMillis();
        final WeakReference ref = new WeakReference<>(new Object());
        while (ref.get() != null && System.currentTimeMillis() - started < timeoutMillis) {
            System.gc();
            Thread.yield();
        }
    }
}
