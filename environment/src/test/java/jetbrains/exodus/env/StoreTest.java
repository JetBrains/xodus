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
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Arrays;

import static org.junit.Assert.*;

public class StoreTest extends EnvironmentTestsBase {

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
            fail("Exception on open removed db is not thrown!");
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
            fail("Exception on open removed db is not thrown!");
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
        assertNotNull(store[0]);
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
                assertTrue(env.storeExists(store.getName(), txn));
            }
        });
    }

    @Test
    public void test_XD_459() {
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
                    assertTrue(cursor.getSearchBoth(StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")));
                    assertTrue(cursor.deleteCurrent());
                    assertFalse(cursor.getSearchBoth(StringBinding.stringToEntry("x"), StringBinding.stringToEntry("x")));
                    assertFalse(cursor.deleteCurrent());
                    assertTrue(cursor.getSearchBoth(StringBinding.stringToEntry("1"), StringBinding.stringToEntry("1")));
                    assertTrue(cursor.deleteCurrent());
                }
            }
        });
    }

    @Test
    public void testFileByteIterable() throws IOException {
        final String content = "quod non habet principium, non habet finem";
        final File file = File.createTempFile("FileByteIterable", null, TestUtil.createTempDir());
        try (FileOutputStream output = new FileOutputStream(file)) {
            output.write(content.getBytes("UTF-8"));
        }
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("Store", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
        final FileByteIterable fbi = new FileByteIterable(file);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("winged"), fbi);
            }
        });
        try {
            assertEquals(content, env.computeInReadonlyTransaction(new TransactionalComputable<String>() {
                @Override
                public String compute(@NotNull Transaction txn) {
                    final ByteIterable value = store.get(txn, StringBinding.stringToEntry("winged"));
                    assertNotNull(value);
                    try {
                        return new String(value.getBytesUnsafe(), 0, value.getLength(), "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        return null;
                    }
                }
            }));
        } finally {
            file.delete();
        }
    }

    @Test
    public void testConcurrentPutLikeJetPassBTree() {
        concurrentPutLikeJetPass(StoreConfig.WITHOUT_DUPLICATES);
    }

    @Test
    public void testConcurrentPutLikeJetPassPatricia() {
        concurrentPutLikeJetPass(StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
    }

    @Test
    @TestFor(issues = "XD-601")
    public void testXD_601() {
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Messages", StoreConfig.WITHOUT_DUPLICATES, txn, true);
            }
        });
        assertNotNull(store);
        final int cachePageSize = env.getEnvironmentConfig().getLogCachePageSize();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < cachePageSize; ++i) {
            builder.append('0');
        }
        final String key = builder.toString();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry(key), StringBinding.stringToEntry(""));
            }
        });
        assertNull(env.computeInTransaction(new TransactionalComputable<ByteIterable>() {
            @Override
            public ByteIterable compute(@NotNull final Transaction txn) {
                return store.get(txn, StringBinding.stringToEntry(key.substring(0, cachePageSize - 1) + '1'));
            }
        }));
    }

    @Test
    @TestFor(issues = "XD-608")
    public void testXD_608_by_Thorsten_Schemm() {
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Whatever", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn, true);
            }
        });
        assertNotNull(store);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
                store.put(txn, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(1));
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                assert_XD_608_1_0(txn, store);
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                assert_XD_608_0_0_1(txn, store);
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                assert_XD_608_0_1(txn, store);
            }
        });
    }

    @Test
    @TestFor(issues = "XD-608")
    public void testXD_608_Mutable() {
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Whatever", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn, true);
            }
        });
        assertNotNull(store);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
                store.put(txn, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(1));
                assert_XD_608_1_0(txn, store);
                assert_XD_608_0_0_1(txn, store);
                assert_XD_608_0_1(txn, store);
            }
        });
    }

    private void assert_XD_608_1_0(@NotNull Transaction txn, Store store) {
        try (Cursor cursor = store.openCursor(txn)) {
            assertTrue(cursor.getPrev());
            assertEquals(1, IntegerBinding.entryToInt(cursor.getKey()));
            assertTrue(cursor.getPrev());
            assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
        }
    }

    private void assert_XD_608_0_1(@NotNull Transaction txn, Store store) {
        try (Cursor cursor = store.openCursor(txn)) {
            assertNotNull(cursor.getSearchKey(IntegerBinding.intToEntry(1)));
            assertEquals(1, IntegerBinding.entryToInt(cursor.getKey()));
            assertTrue(cursor.getPrev());
            assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
        }
    }

    private void assert_XD_608_0_0_1(@NotNull Transaction txn, Store store) {
        try (Cursor cursor = store.openCursor(txn)) {
            assertNotNull(cursor.getSearchKey(IntegerBinding.intToEntry(0)));
            assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
            assertTrue(cursor.getNext());
            assertEquals(1, IntegerBinding.entryToInt(cursor.getKey()));
        }
    }

    @Test
    @TestFor(issues = "XD-614")
    public void testXD_614_by_Thorsten_Schemm() {
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Whatever", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn, true);
            }
        });
        assertNotNull(store);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
                store.put(txn, IntegerBinding.intToEntry(256), IntegerBinding.intToEntry(256));
                store.put(txn, IntegerBinding.intToEntry(257), IntegerBinding.intToEntry(257));
                store.put(txn, IntegerBinding.intToEntry(512), IntegerBinding.intToEntry(512));
                store.put(txn, IntegerBinding.intToEntry(521), IntegerBinding.intToEntry(521));
                try (Cursor cursor = store.openCursor(txn)) {
                    assertNotNull(cursor.getSearchKey(IntegerBinding.intToEntry(256)));
                    assertTrue(cursor.getPrev());
                    assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
                }
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try (Cursor cursor = store.openCursor(txn)) {
                    assertNotNull(cursor.getSearchKey(IntegerBinding.intToEntry(256)));
                    assertTrue(cursor.getPrev());
                    assertEquals(0, IntegerBinding.entryToInt(cursor.getKey()));
                }
            }
        });
    }

    @Test
    @TestFor(issues = "XD-614")
    public void testXD_614_next_prev() {
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Whatever", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn, true);
            }
        });
        assertNotNull(store);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                for (int i = 0; i < 512; ++i) {
                    store.put(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
                }
                assert_XD_614(txn, store);
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                assert_XD_614(txn, store);
            }
        });
    }

    private void assert_XD_614(@NotNull Transaction txn, Store store) {
        try (Cursor cursor = store.openCursor(txn)) {
            for (int i = 0; i < 511; ++i) {
                assertTrue(cursor.getNext());
                assertEquals(i, IntegerBinding.entryToInt(cursor.getKey()));
                assertTrue(cursor.getNext());
                assertTrue(cursor.getPrev());
            }
        }
    }

    @Test
    @TestFor(issues = "XD-601")
    public void testXD_601_by_Thorsten_Schemm() {
        env.getEnvironmentConfig().setGcEnabled(false);
        assertTrue(new HashSet<>(Arrays.asList(XD_601_KEYS)).size() == XD_601_KEYS.length);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("Messages", StoreConfig.WITHOUT_DUPLICATES, txn, true);
            }
        });
        assertNotNull(store);
        for (int i = 0; i < XD_601_KEYS.length; i++) {
            final ByteIterable nextKey = StringBinding.stringToEntry(XD_601_KEYS[i]);
            final ByteIterable nextValue = StringBinding.stringToEntry(Integer.toString(i));
            long storeCount = env.computeInTransaction(new TransactionalComputable<Long>() {
                @Override
                public Long compute(@NotNull final Transaction txn) {
                    return store.count(txn);
                }
            });
            assertEquals(storeCount, i);
            if (storeCount != i) {
                System.out.println("unexpected store count:  " + storeCount + " at " + i);
            }
            ByteIterable currentValue = env.computeInReadonlyTransaction(new TransactionalComputable<ByteIterable>() {
                @Override
                public ByteIterable compute(@NotNull Transaction txn) {
                    return store.get(txn, nextKey);
                }
            });
            if (currentValue != null) {
                System.out.println("unexpected value: " + StringBinding.entryToString(currentValue) + " at " + i);
                env.executeInReadonlyTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        assertNotNull(store.get(txn, nextKey));
                    }
                });
            }
            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    assertTrue(store.put(txn, nextKey, nextValue));
                }
            });
        }
    }

    private static String[] XD_601_KEYS = new String[]{
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com]]502,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2103429320,test@example.com]]503,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2134650811,test@example.com]]504,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2145406178,test@example.com]]505,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147059744,test@example.com]]506,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147384965,test@example.com]]507,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147450898,test@example.com]]508,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147479264,test@example.com]]509,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147480602,test@example.com]]510,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147480889,test@example.com]]511,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483388,test@example.com]]512,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483585,test@example.com]]513,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483644,test@example.com]]514,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483645,test@example.com]]515,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com]]516,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1011115154,test@example.com]]517,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1584076859,test@example.com]]518,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2142239416,test@example.com]]519,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2145528569,test@example.com]]520,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147005689,test@example.com]]521,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147398315,test@example.com]]522,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147468281,test@example.com]]523,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147481424,test@example.com]]524,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147482206,test@example.com]]525,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147482668,test@example.com]]526,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147482677,test@example.com]]527,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483616,test@example.com]]528,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483622,test@example.com]]529,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483629,test@example.com]]530,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483638,test@example.com]]531,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483643,test@example.com]]532,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483645,test@example.com]]533,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com]]534,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1983389444,test@example.com]]535,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1988121287,test@example.com]]536,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2117166309,test@example.com]]537,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2138359900,test@example.com]]538,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2143453507,test@example.com]]539,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2144452650,test@example.com]]540,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2146904126,test@example.com]]541,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147429187,test@example.com]]542,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147445430,test@example.com]]543,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147475608,test@example.com]]544,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147482216,test@example.com]]545,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483147,test@example.com]]546,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483360,test@example.com]]547,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483636,test@example.com]]548,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483637,test@example.com]]549,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483638,test@example.com]]550,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483640,test@example.com]]551,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483644,test@example.com]]552,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483645,test@example.com]]553,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com]]554,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][999029592,test@example.com]]555,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1311687065,test@example.com]]556,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1341841370,test@example.com]]557,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1693281641,test@example.com]]558,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1908697104,test@example.com]]559,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][1968567927,test@example.com]]560,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2053101057,test@example.com]]561,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2093936241,test@example.com]]562,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2134716187,test@example.com]]563,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2143830702,test@example.com]]564,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2146764466,test@example.com]]565,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147012566,test@example.com]]566,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147272263,test@example.com]]567,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147455665,test@example.com]]568,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147480723,test@example.com]]569,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147481629,test@example.com]]570,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483128,test@example.com]]571,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483396,test@example.com]]572,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483404,test@example.com]]573,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483599,test@example.com]]574,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483633,test@example.com]]575,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483635,test@example.com]]576,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483640,test@example.com]]577,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483641,test@example.com]]578,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483645,test@example.com]]579,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com]]580,0]",
        "[[[2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][2147483646,test@example.com][798189424,test@example.com]]581,0]"
    };

    private void putWithoutDuplicates(final StoreConfig config) {
        final EnvironmentImpl env = getEnvironment();
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", config, txn);
        assertTrue(store.put(txn, getKey(), getValue()));
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value");
        txn = env.beginTransaction();
        assertTrue(store.put(txn, getKey(), getValue2()));
        txn.commit();
        txn = env.beginTransaction();
        // TODO: review the following when we no longer need meta-tree cloning
        assertEquals(!config.prefixing, store.put(txn, getKey(), getValue2()));
        txn.commit();
        assertNotNullStringValue(store, getKey(), "value2");
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
                protected void execute() {
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
        assertEquals(count, keys.size());
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final long[] longs = keys.toLongArray();
                for (long key : longs) {
                    assertEquals(getValue(), store.get(txn, LongBinding.longToCompressedEntry(key)));
                }
                Arrays.sort(longs);
                try (Cursor cursor = store.openCursor(txn)) {
                    int i = 0;
                    while (cursor.getNext()) {
                        assertEquals(longs[i++], LongBinding.compressedEntryToLong(cursor.getKey()));
                    }
                    assertEquals(count, i);
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

    private static long randomLong() {
        return Math.abs(rnd.nextLong());
    }
}
