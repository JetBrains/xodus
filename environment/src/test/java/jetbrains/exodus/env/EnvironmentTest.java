/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.*;
import static org.junit.Assert.*;

public class EnvironmentTest extends EnvironmentTestsBase {

    private final Map<String, File> subfolders = new HashMap<>();

    @Test
    public void testEmptyEnvironment() {
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testStatisticsBytesWritten() {
        testEmptyEnvironment();
        assertTrue(env.getStatistics().getStatisticsItem(BYTES_WRITTEN).getTotal() > 0L);
    }

    @Test
    public void testCreateSingleStore() {
        openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT,
                DatabaseRoot.DATABASE_ROOT_TYPE, BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF,
                BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testStatisticsTransactions() {
        testCreateSingleStore();
        final EnvironmentStatistics statistics = env.getStatistics();
        assertTrue(statistics.getStatisticsItem(TRANSACTIONS).getTotal() > 0L);
        assertTrue(statistics.getStatisticsItem(FLUSHED_TRANSACTIONS).getTotal() > 0L);
    }

    @Test
    public void testStatisticsItemNames() {
        testStatisticsTransactions();
        final EnvironmentStatistics statistics = env.getStatistics();
        assertNotNull(statistics.getStatisticsItem(BYTES_WRITTEN));
        assertNotNull(statistics.getStatisticsItem(BYTES_READ));
        assertNotNull(statistics.getStatisticsItem(BYTES_MOVED_BY_GC));
        assertNotNull(statistics.getStatisticsItem(TRANSACTIONS));
        assertNotNull(statistics.getStatisticsItem(READONLY_TRANSACTIONS));
        assertNotNull(statistics.getStatisticsItem(GC_TRANSACTIONS));
        assertNotNull(statistics.getStatisticsItem(ACTIVE_TRANSACTIONS));
        assertNotNull(statistics.getStatisticsItem(FLUSHED_TRANSACTIONS));
        assertNotNull(statistics.getStatisticsItem(TRANSACTIONS_DURATION));
        assertNotNull(statistics.getStatisticsItem(READONLY_TRANSACTIONS_DURATION));
        assertNotNull(statistics.getStatisticsItem(GC_TRANSACTIONS_DURATION));
        assertNotNull(statistics.getStatisticsItem(DISK_USAGE));
        assertNotNull(statistics.getStatisticsItem(UTILIZATION_PERCENT));
    }

    @Test
    public void testFirstLastLoggables() {
        openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        final Log log = getLog();
        Loggable l = log.getFirstLoggableOfType(BTreeBase.BOTTOM_ROOT);
        assertNotNull(l);
        Assert.assertEquals(0L, l.getAddress());
        l = log.getLastLoggableOfType(BTreeBase.BOTTOM_ROOT);
        assertNotNull(l);
        Assert.assertEquals(40L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress(), log.getTip());
        assertNotNull(l);
        Assert.assertEquals(12L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress(), log.getTip());
        assertNotNull(l);
        Assert.assertEquals(0L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(BTreeBase.BOTTOM_ROOT, l.getAddress(), log.getTip());
        Assert.assertNull(l);
        l = log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, Long.MAX_VALUE, log.getTip());
        assertNotNull(l);
        Assert.assertEquals(48L, l.getAddress());
        l = log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, l.getAddress(), log.getTip());
        assertNotNull(l);
        l = log.getFirstLoggableOfType(DatabaseRoot.DATABASE_ROOT_TYPE);
        assertNotNull(l);
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
    @TestFor(issue = "XD-457")
    public void testClearWithTransaction_XD_457() throws InterruptedException {
        final Latch latch = Latch.create();
        env.executeInTransaction(txn -> {
            final StoreImpl store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
            store.put(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0"));
            assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
            final Throwable[] th = {null};
            // asynchronously clear the environment
            try {
                latch.acquire();
                runParallelRunnable(() -> {
                    latch.release();
                    try {
                        env.clear();
                    } catch (Throwable t) {
                        th[0] = t;
                    }
                    latch.release();
                });
                latch.acquire();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
                fail();
            }
            Assert.assertNull(th[0]);
            assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
        });
        latch.acquire();
        env.executeInExclusiveTransaction(txn -> {
            final StoreImpl store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
            Assert.assertFalse(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
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
    public void testBreakSavingMetaTree() {
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        if (ec.getLogCachePageSize() > 1024) {
            ec.setLogCachePageSize(1024);
        }
        ec.setTreeMaxPageSize(16);

        recreateEnvinronment(ec);

        env.executeInTransaction(txn -> {
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
        });
        reopenEnvironment();
        final LogTestConfig testConfig = new LogTestConfig();
        testConfig.setMaxHighAddress(10401);
        testConfig.setSettingHighAddressDenied(true);
        //noinspection deprecation
        env.getLog().setLogTestConfig(testConfig);
        try {
            for (int i = 0; i < 23; ++i) {
                env.executeInTransaction(txn -> {
                    final StoreImpl store1 = env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
                    final StoreImpl store2 = env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
                    final StoreImpl store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
                    for (int i1 = 0; i1 < 13; ++i1) {
                        store1.put(txn, IntegerBinding.intToCompressedEntry(i1), IntegerBinding.intToCompressedEntry(i1));
                        store2.put(txn, IntegerBinding.intToCompressedEntry(i1), IntegerBinding.intToCompressedEntry(i1));
                        store3.put(txn, IntegerBinding.intToCompressedEntry(i1), IntegerBinding.intToCompressedEntry(i1));
                    }
                });
            }
            TestUtil.runWithExpectedException(() -> env.executeInTransaction(txn -> {
                final StoreImpl store1 = env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
                final StoreImpl store2 = env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
                final StoreImpl store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
                for (int i = 0; i < 13; ++i) {
                    store1.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                    store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                    store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
                }
            }), ExodusException.class);
            //noinspection deprecation
            env.getLog().setLogTestConfig(null);
            AbstractConfig.suppressConfigChangeListenersForThread();
            try {
                ec.setEnvIsReadonly(true);
                reopenEnvironment();
            } finally {
                AbstractConfig.resumeConfigChangeListenersForThread();
            }
            env.executeInTransaction(txn -> env.getAllStoreNames(txn));
        } finally {
            //noinspection deprecation
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
    public void testWriteDataToSeveralFiles() {
        setLogFileSize(16);
        env.getEnvironmentConfig().setGcEnabled(true);

        final StoreConfig expectedConfig = StoreConfig.WITHOUT_DUPLICATES;

        final long started = System.currentTimeMillis();
        for (int j = 0; j < 100; j++) {
            if (System.currentTimeMillis() - started > 30000) break;
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
    @TestFor(issue = "XD-590")
    public void issueXD_590_reported() {
        // 1) open store
        final Store store = env.computeInTransaction((TransactionalComputable<Store>) txn -> env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn));
        // 2) store(put) a key 1 , value A1
        env.executeInTransaction(txn -> store.put(txn, StringBinding.stringToEntry("key1"), StringBinding.stringToEntry("A1")));
        // 3) using second transaction : store(put) key 2 value A2,  update key 1 with B1. inside transaction reload ke1 (value=B1 OK)
        env.executeInTransaction(txn -> {
            store.put(txn, StringBinding.stringToEntry("key2"), StringBinding.stringToEntry("A2"));
            store.put(txn, StringBinding.stringToEntry("key1"), StringBinding.stringToEntry("B1"));
            final ByteIterable value1 = store.get(txn, StringBinding.stringToEntry("key1"));
            assertNotNull(value1);
            Assert.assertEquals("B1", StringBinding.entryToString(value1));
        });
        // 4) using third transaction : reload key 1 , value is A1 !=B1   !!!!! Error.
        env.executeInTransaction(txn -> {
            final ByteIterable value1 = store.get(txn, StringBinding.stringToEntry("key1"));
            assertNotNull(value1);
            Assert.assertEquals("B1", StringBinding.entryToString(value1));
        });
    }

    @Test
    @TestFor(issues = {"XD-594", "XD-717"})
    public void leakingEnvironment() throws Exception {
        cleanSubfolders();
        super.tearDown();
        final WeakReference<Environment> envRef = new WeakReference<>(createAndCloseEnvironment());
        waitForPendingFinalizers(10000);
        Assert.assertNull(envRef.get());
    }

    @Test
    @TestFor(issue = "XD-606")
    public void mappedFileNotUnmapped() {
        File tempDir = TestUtil.createTempDir();
        try {
            final Environment env = Environments.newInstance(tempDir, new EnvironmentConfig().setLogFileSize(1).setLogCachePageSize(1024).setLogCacheShared(false));
            final Store store = env.computeInTransaction(txn -> env.openStore("0", StoreConfig.WITHOUT_DUPLICATES, txn));
            env.executeInTransaction(txn -> {
                store.put(txn, StringBinding.stringToEntry("k"), StringBinding.stringToEntry("v"));
                for (int i = 0; i < 200; ++i) {
                    store.put(txn, StringBinding.stringToEntry("k" + i), StringBinding.stringToEntry("v" + i));
                }
            });
            Assert.assertEquals("v", env.computeInTransaction(txn ->
                    StringBinding.entryToString(
                            Objects.requireNonNull(store.get(txn, StringBinding.stringToEntry("k"))))));
            env.close();
            final Environment reopenedEnv = Environments.newInstance(tempDir, env.getEnvironmentConfig());
            final Store reopenedStore = reopenedEnv.computeInTransaction(txn -> reopenedEnv.openStore("0", StoreConfig.USE_EXISTING, txn));
            Assert.assertEquals("v", reopenedEnv.computeInTransaction(txn -> StringBinding.entryToString(
                    Objects.requireNonNull(reopenedStore.get(txn, StringBinding.stringToEntry("k"))))));
            reopenedEnv.close();
            assertTrue(new File(tempDir, LogUtil.getLogFilename(0)).renameTo(new File(tempDir, LogUtil.getLogFilename(0x1000000))));
        } finally {
            IOUtil.deleteRecursively(tempDir);
        }
    }

    @SuppressWarnings("deprecation")
    @Test(expected = IllegalStateException.class)
    @TestFor(issue = "XD-628")
    public void readCloseRace() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(txn -> {
            for (int i = 0; i < 10000; ++i) {
                store.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(Integer.toString(i)));
            }
        });

        env.getEnvironmentConfig().setEnvCloseForcedly(true);
        //noinspection deprecation
        env.getLog().clearCache();

        env.executeInReadonlyTransaction(txn -> {
            try (Cursor cursor = store.openCursor(txn)) {
                final Latch latch = Latch.create();
                try {
                    latch.acquire();
                    new Thread(() -> {
                        env.close();
                        latch.release();
                    }).start();
                    latch.acquire();
                } catch (InterruptedException ignore) {
                }
                while (cursor.getNext()) {
                    assertNotNull(cursor.getKey());
                    assertNotNull(cursor.getValue());
                }
            }
        });
    }

    @Test
    @TestFor(issue = "XD-682")
    public void cursorOnFlushedTxn() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(txn -> {
            store.put(txn, IntegerBinding.intToEntry(0), StringBinding.stringToEntry(Integer.toString(0)));
            store.put(txn, IntegerBinding.intToEntry(1), StringBinding.stringToEntry(Integer.toString(1)));
        });
        env.executeInTransaction(txn -> {
            try (Cursor cursor = store.openCursor(txn)) {
                assertTrue(cursor.getNext());
                Assert.assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
                store.put(txn, IntegerBinding.intToEntry(2), StringBinding.stringToEntry(Integer.toString(2)));
                assertTrue(txn.flush());
                TestUtil.runWithExpectedException(cursor::getNext, ExodusException.class);
                TestUtil.runWithExpectedException(cursor::getPrev, ExodusException.class);
                TestUtil.runWithExpectedException(cursor::getKey, ExodusException.class);
                TestUtil.runWithExpectedException(cursor::getValue, ExodusException.class);
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
                additionalEnvironments.add(newEnvironmentInstance(LogConfig.create(rwPair.getFirst(), rwPair.getSecond()), new EnvironmentConfig()));
            }
            final Thread[] threads = new Thread[numberOfEnvironments];
            System.out.println("create data concurrently");
            // create data concurrently
            int i = 0;
            for (final Environment env : additionalEnvironments) {
                threads[i++] = new Thread(() -> {
                    final Transaction txn = env.beginTransaction();
                    try {
                        final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                        for (int i12 = 0; i12 < 10000; ++i12) {
                            final ArrayByteIterable kv = IntegerBinding.intToEntry(i12);
                            store.put(txn, kv, kv);
                        }
                    } catch (Exception e) {
                        txn.abort();
                    }
                    txn.commit();
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
                threads[i++] = new Thread(() -> {
                    final Transaction txn = env.beginTransaction();
                    try {
                        final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                        for (int i1 = 0; i1 < 10000; ++i1) {
                            final ByteIterable bi = store.get(txn, IntegerBinding.intToEntry(i1));
                            assertNotNull(bi);
                            Assert.assertEquals(i1, IntegerBinding.entryToInt(bi));
                        }
                    } finally {
                        txn.abort();
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
//
//    @Test
//    public void testMigration() throws Exception {
//        final String envHome = "/home/andrey/old-db";
//        final String dataFile = "/home/andrey/old-db/data-file";
//
//        EnvironmentConfig config = new EnvironmentConfig();
//        config.removeSetting(EnvironmentConfig.CIPHER_KEY);
//        config.removeSetting(EnvironmentConfig.CIPHER_ID);
//
//        try (final FileInputStream fileInputStream = new FileInputStream(dataFile)) {
//            try (final DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {
//                try (final Environment environment = Environments.newInstance(envHome, config)) {
//                    environment.executeInReadonlyTransaction(txn -> {
//                        System.out.println("Pre-check of storages");
//
//                        var storeNames = environment.getAllStoreNames(txn);
//                        for (var storeName : storeNames) {
//                            var store = environment.openStore(storeName, StoreConfig.USE_EXISTING, txn);
//
//                            try (var cursor = store.openCursor(txn)) {
//                                while (cursor.getNext()) {
//                                    cursor.getKey();
//                                    cursor.getValue();
//                                }
//                            }
//
//                            System.out.printf("Store %s was processed%n", storeName);
//                        }
//
//                        System.out.println("Check stored data");
//
//                        int count = 0;
//                        while (true) {
//                            try {
//                                final int storeId = dataInputStream.read();
//                                if (storeId == -1) {
//                                    break;
//                                }
//
//                                final int rem = storeId % 4;
//                                final Store store;
//                                if (rem == 0) {
//                                    store = environment.openStore("store " + storeId,
//                                            StoreConfig.WITHOUT_DUPLICATES,
//                                            txn);
//                                } else if (rem == 1) {
//                                    store = environment.openStore("store " + storeId,
//                                            StoreConfig.WITH_DUPLICATES,
//                                            txn);
//                                } else if (rem == 2) {
//                                    store = environment.openStore("store " + storeId,
//                                            StoreConfig.WITH_DUPLICATES_WITH_PREFIXING,
//                                            txn);
//                                } else {
//                                    store = environment.openStore("store " + storeId,
//                                            StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
//                                            txn);
//                                }
//
//                                final int keySize = dataInputStream.readInt();
//                                final byte[] key = new byte[keySize];
//
//                                dataInputStream.readFully(key);
//
//                                final int valueSize = dataInputStream.readInt();
//                                final byte[] value = new byte[valueSize];
//
//                                dataInputStream.readFully(value);
//
//                                ByteIterable storedValue = store.get(txn, new ArrayByteIterable(key));
//                                Assert.assertEquals(new ArrayByteIterable(value), storedValue);
//                            } catch (IOException e) {
//                                throw new RuntimeException(e);
//                            }
//
//                            count++;
//                            if (count % 10_000 == 0) {
//                                System.out.printf("%d records were processed %n", count);
//                            }
//                        }
//                    });
//                }
//            }
//        }
//    }

    @Test
    @TestFor(issue = "XD-770")
    public void alterBalancePolicy() {
        final Store[] store = {openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)};
        env.executeInTransaction(txn -> {
            for (int i = 0; i < 10000; i += 2) {
                store[0].put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(Integer.toString(i)));
            }
        });
        env.executeInReadonlyTransaction(txn -> {
            for (int i = 0; i < 10000; i += 2) {
                Assert.assertEquals(StringBinding.stringToEntry(Integer.toString(i)), store[0].get(txn, IntegerBinding.intToEntry(i)));
            }
        });
        final EnvironmentConfig config = env.getEnvironmentConfig();
        config.setTreeMaxPageSize(config.getTreeMaxPageSize() / 4);
        reopenEnvironment();
        store[0] = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(txn -> {
            for (int i = 1; i < 10000; i += 2) {
                store[0].put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(Integer.toString(i)));
            }
        });
        env.executeInReadonlyTransaction(txn -> {
            for (int i = 0; i < 10000; ++i) {
                Assert.assertEquals(StringBinding.stringToEntry(Integer.toString(i)), store[0].get(txn, IntegerBinding.intToEntry(i)));
            }
        });
    }

    @Test
    @TestFor(issue = "XD-770")
    public void alterBalancePolicy2() {
        alterBalancePolicy(0.25f);
    }

    @Test
    @TestFor(issue = "XD-770")
    public void alterBalancePolicy3() {
        alterBalancePolicy(4);
    }

    @Test
    @TestFor(issue = "XD-770")
    public void avoidEmptyPages() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITH_DUPLICATES);
        final int count = 1000;
        env.executeInTransaction(txn -> {
            for (int i = 0; i < count; ++i) {
                store.put(txn, IntegerBinding.intToEntry(i % 600), StringBinding.stringToEntry(Integer.toString(i)));
            }
        });
        env.executeInTransaction(txn -> {
            final Random rnd = new Random();
            for (int i = 0; i < 2000; ++i) {
                for (int j = 0; j < count / 2; ++j) {
                    store.put(txn, IntegerBinding.intToEntry(rnd.nextInt(600)), StringBinding.stringToEntry(Integer.toString(i)));
                }
                for (int j = 0; j < count / 2; ++j) {
                    store.delete(txn, IntegerBinding.intToEntry(rnd.nextInt(600)));
                }
                try (Cursor cursor = store.openCursor(txn)) {
                    cursor.getNext();
                    int prev = IntegerBinding.entryToInt(cursor.getKey());
                    try (Cursor c = store.openCursor(txn)) {
                        Assert.assertNotNull(c.getSearchKey(cursor.getKey()));
                    }
                    while (cursor.getNext()) {
                        final int next = IntegerBinding.entryToInt(cursor.getKey());
                        Assert.assertTrue(prev <= next);
                        prev = next;
                        try (Cursor c = store.openCursor(txn)) {
                            Assert.assertNotNull(c.getSearchKey(cursor.getKey()));
                        }
                    }
                }
            }
        });
    }

    private void alterBalancePolicy(float pageSizeMultiple) {
        final Store[] store = {openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)};
        final int count = 20000;
        env.executeInTransaction(txn -> {
            for (int i = 0; i < count; ++i) {
                store[0].put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(Integer.toString(i)));
            }
        });
        env.executeInReadonlyTransaction(txn -> {
            for (int i = 0; i < count; ++i) {
                Assert.assertEquals(StringBinding.stringToEntry(Integer.toString(i)), store[0].get(txn, IntegerBinding.intToEntry(i)));
            }
        });
        final EnvironmentConfig config = env.getEnvironmentConfig();
        config.setTreeMaxPageSize((int) (config.getTreeMaxPageSize() * pageSizeMultiple));
        reopenEnvironment();
        store[0] = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        env.executeInTransaction(txn -> {
            for (int i = 0; i < count / 5; ++i) {
                store[0].delete(txn, IntegerBinding.intToEntry(i));
            }
            for (int i = 0; i < count / 5; ++i) {
                store[0].put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(""));
            }
            Random rnd = new Random();
            for (int i = 0; i < count / 3; ++i) {
                store[0].delete(txn, IntegerBinding.intToEntry(rnd.nextInt(count / 4)));
                store[0].put(txn, IntegerBinding.intToEntry(rnd.nextInt(count / 4)), StringBinding.stringToEntry(""));
            }
            for (int i = 0; i < count / 2; ++i) {
                store[0].delete(txn, IntegerBinding.intToEntry(i));
            }
        });
        env.executeInReadonlyTransaction(txn -> {
            try (Cursor cursor = store[0].openCursor(txn)) {
                int i;
                for (i = count / 2; i < count; ++i) {
                    if (!cursor.getNext()) break;
                    Assert.assertEquals("" + i, IntegerBinding.intToEntry(i), cursor.getKey());
                }
                Assert.assertEquals(count, i);
            }
        });
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
        FileDataReader reader = new FileDataReader(child);
        return new Pair<>(reader, new FileDataWriter(reader)
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

    protected EnvironmentImpl createAndCloseEnvironment() throws Exception {
        final Pair<DataReader, DataWriter> rw = createRW();
        final EnvironmentImpl env = newEnvironmentInstance(
                LogConfig.create(rw.getFirst(), rw.getSecond()), new EnvironmentConfig().setGcUtilizationFromScratch(true));
        env.close();
        return env;
    }

    private void waitForPendingFinalizers(@SuppressWarnings("SameParameterValue") final long timeoutMillis) {
        final long started = System.currentTimeMillis();
        final WeakReference<Object> ref = new WeakReference<>(new Object());
        while (ref.get() != null && System.currentTimeMillis() - started < timeoutMillis) {
            System.gc();
            Thread.yield();
        }
    }
}
