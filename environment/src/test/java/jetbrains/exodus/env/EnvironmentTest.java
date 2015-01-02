/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.tree.btree.BTreeBase;
import jetbrains.exodus.util.IOUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class EnvironmentTest extends EnvironmentTestsBase {

    private final Map<String, File> subfolders = new HashMap<String, File>();

    @Test
    public void testEmptyEnvironment() {
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testCreateSingleStore() {
        final Store store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT,
                DatabaseRoot.DATABASE_ROOT_TYPE, BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF,
                BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
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
        Assert.assertEquals(l.getAddress(), l.getAddress());
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
    public void testCloseTwice() {
        final int count = 100;
        final Transaction txn = env.beginTransaction();
        for (int i = 0; i < count; ++i) {
            env.openStore("new_store" + i, StoreConfig.WITHOUT_DUPLICATES, txn);
        }
        txn.commit();
        getEnvironment().close();
        final EnvironmentConfig envConfig = env.getEnvironmentConfig();
        try {
            TestUtil.runWithExpectedException(new Runnable() {
                @Override
                public void run() {
                    env.close();
                }
            }, IllegalStateException.class);
        } finally {
            // forget old env anyway to prevent tearDown fail
            LogConfig config = new LogConfig();
            config.setReader(reader);
            config.setWriter(writer);
            env = newEnvironmentInstance(config, envConfig);
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
        final Set<String> names = new TreeSet<String>();
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
        LogConfig config = new LogConfig();
        config.setReader(reader);
        config.setWriter(writer);
        envConfig.setTreeMaxPageSize(count / 2);
        env = newEnvironmentInstance(config, envConfig);
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
    public void testUseExistingConfig() {
        final StoreConfig expectedConfig = StoreConfig.WITHOUT_DUPLICATES;
        final String name = "testDatabase";

        Transaction txn = env.beginTransaction();
        env.openStore(name, expectedConfig, txn);
        txn.commit();

        txn = env.beginTransaction();
        final Store store = env.openStore(name, StoreConfig.USE_EXISTING_READONLY, txn);
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
    public void testSharedCache() throws InterruptedException, IOException {
        env.getEnvironmentConfig().setLogCacheShared(true);
        reopenEnvironment();
        final Set<Environment> additionalEnvironments = new HashSet<Environment>();
        try {
            env.getEnvironmentConfig().setGcEnabled(false);
            final int numberOfEnvironments = 200;
            for (int i = 0; i < numberOfEnvironments; ++i) {
                final Pair<DataReader, DataWriter> readerWriterPair = createReaderWriter("sub" + i);
                final LogConfig logConfig = new LogConfig();
                logConfig.setReader(readerWriterPair.getFirst());
                logConfig.setWriter(readerWriterPair.getSecond());
                additionalEnvironments.add(newEnvironmentInstance(logConfig, EnvironmentConfig.DEFAULT));
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

}
