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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Arrays;

public class StoreTest extends EnvironmentTestsBase {

    private static final long STORE_ROOT_ADDRESS = 12L;

    @Test
    public void testPutWithoutDuplicates() {
        putWithoutDuplicates(StoreConfig.WITHOUT_DUPLICATES);
    }

    @Test
    public void testPutWithoutDuplicatesWithPrefixing() {
        putWithoutDuplicates(StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
    }

    @Test
    public void testPutRightWithoutDuplicates() {
        successivePutRightWithoutDuplicates(StoreConfig.WITHOUT_DUPLICATES);
    }

    @Test
    public void testPutRightWithoutDuplicatesWithPrefixing() {
        successivePutRightWithoutDuplicates(StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
    }

    @Test
    public void testTruncateWithinTxn() {
        truncateWithinTxn(StoreConfig.WITHOUT_DUPLICATES);
    }

    @Test
    public void testTruncateWithinTxnWithPrefixing() {
        truncateWithinTxn(StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
    }

    @Test
    public void testRemoveWithoutTransaction() {
        final Store store = openStoreAutoCommit("store", StoreConfig.WITHOUT_DUPLICATES);
        Transaction txn = env.beginTransaction();
        store.put(txn, getKey(), getValue());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");

        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.removeStore("store", txn);
            }
        });
        try {
            openStoreAutoCommit("store", StoreConfig.USE_EXISTING);
            Assert.fail("Exception on open removed db is not thrown!");
        } catch (Exception ex) {
            // ignore
        }
    }

    @Test
    public void testRemoveWithinTransaction() {
        final Store store = openStoreAutoCommit("store", StoreConfig.WITHOUT_DUPLICATES);
        Transaction txn = env.beginTransaction();
        store.put(txn, getKey(), getValue());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        txn = env.beginTransaction();
        store.put(txn, getKey(), getValue2());
        env.removeStore("store", txn);
        txn.commit();
        assertEmptyValue(store, getKey());

        try {
            openStoreAutoCommit("store", StoreConfig.USE_EXISTING);
            Assert.fail("Exception on open removed db is not thrown!");
        } catch (Exception ex) {
            // ignore
        }
    }

    @Test
    public void testPutWithDuplicates() {
        final Store store = openStoreAutoCommit("store", StoreConfig.WITH_DUPLICATES);
        Transaction txn = env.beginTransaction();
        store.put(txn, getKey(), getValue());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        txn = env.beginTransaction();
        store.put(txn, getKey(), getValue2());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        assertNotNullStringValues(store, "value", "value2");
    }

    @Test
    public void testCloseCursorTwice() {
        final Store store = openStoreAutoCommit("store", StoreConfig.WITH_DUPLICATES);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final Cursor cursor = store.openCursor(txn);
                cursor.close();
                TestUtil.runWithExpectedException(new Runnable() {
                    @Override
                    public void run() {
                        cursor.close();
                    }
                }, ExodusException.class);
            }
        });
    }

    @Test
    public void testCreateThreeStoresWithoutAutoCommit() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn);
        env.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn);
        final Store store3 = env.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn);
        store3.put(txn, getKey(), getValue());
        txn.commit();
    }

    @Test
    public void testCreateTwiceInTransaction_XD_394() {
        final Environment env = getEnvironment();
        final Store[] store = {null};
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                store[0] = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                store[0].put(txn, getKey(), getValue());
                final Store sameNameStore = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                sameNameStore.put(txn, getKey2(), getValue2());
            }
        });
        Assert.assertNotNull(store[0]);
        assertNotNullStringValue(store[0], getKey(), "value");
        assertNotNullStringValue(store[0], getKey2(), "value2");
    }

    @Test
    public void testNewlyCreatedStoreExists_XD_394() {
        final Environment env = getEnvironment();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
                Assert.assertTrue(env.storeExists(store.getName(), txn));
            }
        });
    }

    @Test
    public void test_XD_459() {
        final Environment env = getEnvironment();
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("Store", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0"));
                store.put(txn, StringBinding.stringToEntry("1"), StringBinding.stringToEntry("1"));
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try (Cursor cursor = store.openCursor(txn)) {
                    Assert.assertTrue(cursor.getSearchBoth(StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
                    Assert.assertTrue(cursor.deleteCurrent());
                    Assert.assertFalse(cursor.getSearchBoth(StringBinding.stringToEntry("x"), StringBinding.stringToEntry("x")));
                    Assert.assertFalse(cursor.deleteCurrent());
                    Assert.assertTrue(cursor.getSearchBoth(StringBinding.stringToEntry("1"), StringBinding.stringToEntry("1")));
                    Assert.assertTrue(cursor.deleteCurrent());
                }
            }
        });
    }

    @Test
    public void testConcurrentPutLikeJetPassBTree() {
        concurrentPutLikeJetPass(StoreConfig.WITHOUT_DUPLICATES);
    }

    @Test
    public void testConcurrentPutLikeJetPassPatricia() {
        concurrentPutLikeJetPass(StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
    }

    private void putWithoutDuplicates(final StoreConfig config) {
        final EnvironmentImpl env = getEnvironment();
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", config, txn);
        store.put(txn, getKey(), getValue());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        txn = env.beginTransaction();
        store.put(txn, getKey(), getValue2());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value2");
        Assert.assertTrue(env.getGC().isExpired(STORE_ROOT_ADDRESS, 1)); // is root of store obsolete
    }

    private void successivePutRightWithoutDuplicates(final StoreConfig config) {
        final Environment env = getEnvironment();
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", config, txn);
        final ArrayByteIterable kv0 = new ArrayByteIterable(new byte[]{0});
        store.putRight(txn, kv0, StringBinding.stringToEntry("0"));
        final ArrayByteIterable kv10 = new ArrayByteIterable(new byte[]{1, 0});
        store.putRight(txn, kv10, StringBinding.stringToEntry("10"));
        final ArrayByteIterable kv11 = new ArrayByteIterable(new byte[]{1, 1});
        store.putRight(txn, kv11, StringBinding.stringToEntry("11"));
        txn.commit();
        assertNotNullStringValue(store, kv0, "0");
        assertNotNullStringValue(store, kv10, "10");
        assertNotNullStringValue(store, kv11, "11");
    }

    private void truncateWithinTxn(final StoreConfig config) {
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", config, txn);
        store.put(txn, getKey(), getValue());
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        txn = env.beginTransaction();
        store.put(txn, getKey(), getValue2());
        env.truncateStore("store", txn);
        txn.commit();
        assertEmptyValue(store, getKey());
        Assert.assertTrue(env.getGC().isExpired(STORE_ROOT_ADDRESS, 1)); // root of store should be obsolete
        openStoreAutoCommit("store", StoreConfig.USE_EXISTING);
        assertEmptyValue(store, getKey());
    }

    private void concurrentPutLikeJetPass(@NotNull final StoreConfig config) {
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = openStoreAutoCommit("store", config);
        final JobProcessor processor = new MultiThreadDelegatingJobProcessor("ConcurrentPutProcessor", 8) {
        };
        processor.setExceptionHandler(new JobProcessorExceptionHandler() {
            @Override
            public void handle(JobProcessor processor, Job job, Throwable t) {
                t.printStackTrace(System.out);
            }
        });
        processor.start();
        final int count = 50000;
        final LongSet keys = new LongHashSet();
        for (int i = 0; i < count; ++i) {
            processor.queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    env.executeInTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final Transaction txn) {
                            final long key = randomLong();
                            store.put(txn, LongBinding.longToCompressedEntry(key), getValue());
                            if (txn.flush()) {
                                final boolean added;
                                synchronized (keys) {
                                    added = keys.add(key);
                                }
                                if (!added) {
                                    System.out.println("Happy birthday paradox!");
                                }
                            }
                        }
                    });
                }
            });
        }
        processor.waitForJobs(10);
        processor.finish();
        Assert.assertEquals(count, keys.size());
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long[] longs = keys.toLongArray();
                for (long key : longs) {
                    Assert.assertEquals(getValue(), store.get(txn, LongBinding.longToCompressedEntry(key)));
                }
                Arrays.sort(longs);
                try (Cursor cursor = store.openCursor(txn)) {
                    int i = 0;
                    while (cursor.getNext()) {
                        Assert.assertEquals(longs[i++], LongBinding.compressedEntryToLong(cursor.getKey()));
                    }
                    Assert.assertEquals(count, i);
                }
            }
        });
    }

    private static ByteIterable getKey() {
        return StringBinding.stringToEntry("key");
    }

    private static ByteIterable getKey2() {
        return StringBinding.stringToEntry("key2");
    }

    private static ByteIterable getValue() {
        return StringBinding.stringToEntry("value");
    }

    private static ByteIterable getValue2() {
        return StringBinding.stringToEntry("value2");
    }

    private static final SecureRandom rnd = new SecureRandom();
    //private static final AtomicLong longRef = new AtomicLong();

    private static long randomLong() {
        //return longRef.incrementAndGet();
        return Math.abs(rnd.nextLong());
    }
}
