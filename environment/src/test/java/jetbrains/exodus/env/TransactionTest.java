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
import jetbrains.exodus.core.execution.LatchJob;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.tree.btree.BTreeBase;
import jetbrains.exodus.util.DeferredIO;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TransactionTest extends EnvironmentTestsBase {

    @Rule
    public TestName name = new TestName();

    @Override
    protected void createEnvironment() {
        final String methodName = name.getMethodName();
        env = methodName.contains("XD_471") || methodName.contains("XD_478") || methodName.contains("testAfter") ?
                newEnvironmentInstance(LogConfig.create(reader, writer)) :
                newContextualEnvironmentInstance(LogConfig.create(reader, writer));
    }

    @Test
    public void testCurrentTransaction() {
        final ContextualEnvironment env = (ContextualEnvironment) getEnvironment();
        Transaction txn = env.beginTransaction();
        Assert.assertEquals(txn, env.getCurrentTransaction());
        txn.abort();
        Assert.assertEquals(null, env.getCurrentTransaction());
        txn = env.beginTransaction();
        Assert.assertEquals(txn, env.getCurrentTransaction());
        Transaction txn1 = env.beginTransaction();
        Assert.assertEquals(txn1, env.getCurrentTransaction());
        txn1.commit();
        txn.commit();
        Assert.assertEquals(null, env.getCurrentTransaction());
    }

    @Test
    public void testCommitTwice() {
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                Transaction txn = env.beginTransaction();
                txn.commit();
                txn.commit();
            }
        }, ExodusException.class);
    }

    @Test
    public void testAbortTwice() {
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                Transaction txn = env.beginTransaction();
                txn.abort();
                txn.abort();
            }
        }, ExodusException.class);
    }

    @Test
    public void testNestedTransactions() {
        final Transaction txn = env.beginTransaction();
        final Transaction txn1 = env.beginTransaction();
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                txn.commit();
            }
        }, ExodusException.class);
        txn1.commit();
        txn.commit();
    }

    @Test
    public void testAtomicity() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        final ArrayByteIterable entry1 = StringBinding.stringToEntry("1");
        store.put(txn, entry1, entry1);
        final ArrayByteIterable entry2 = StringBinding.stringToEntry("2");
        store.put(txn, entry2, entry2);
        txn.commit();
        // all changes should be placed in single snapshot
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE,
                BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF,
                BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testAbort() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        final ArrayByteIterable entry1 = StringBinding.stringToEntry("1");
        store.put(txn, entry1, entry1);
        final ArrayByteIterable entry2 = StringBinding.stringToEntry("2");
        store.put(txn, entry2, entry2);
        txn.abort();
        // no changes should be written since transaction was not committed
        assertLoggableTypes(getLog(), 0, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE);
    }

    @Test
    public void testReadCommitted() {
        final Environment env = getEnvironment();
        final ByteIterable key = StringBinding.stringToEntry("key");
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, key, StringBinding.stringToEntry("value"));
        Transaction t = env.beginTransaction();
        Assert.assertNull(store.get(t, key));
        t.commit();
        txn.commit();
        txn = env.beginTransaction();
        store.put(txn, key, StringBinding.stringToEntry("value1"));
        t = env.beginTransaction();
        assertNotNullStringValue(store, key, "value");
        t.commit();
        txn.commit();
        assertNotNullStringValue(store, key, "value1");
    }

    @Test
    public void testReadUncommitted() {
        final Environment env = getEnvironment();
        final ByteIterable key = StringBinding.stringToEntry("key");
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.commit();
        txn = env.beginTransaction();
        store.put(txn, key, StringBinding.stringToEntry("value"));
        assertNotNullStringValue(store, key, "value");
        txn.commit();
    }

    @Test
    public void testRepeatableRead() {
        final Environment env = getEnvironment();
        final ByteIterable key = StringBinding.stringToEntry("key");
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, key, StringBinding.stringToEntry("value"));
        assertNotNullStringValue(store, key, "value");
        txn.commit();
        assertNotNullStringValue(store, key, "value");
        txn = env.beginTransaction();
        assertNotNullStringValue(store, key, "value");
        executeParallelTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, key, StringBinding.stringToEntry("value1"));
            }
        });
        assertNotNullStringValue(store, key, "value");
        txn.abort();
        assertNotNullStringValue(store, key, "value1");
    }

    @Test
    public void testTransactionSafeJob() throws InterruptedException {
        final boolean[] bTrue = new boolean[]{false};
        final boolean[] bFalse = new boolean[]{true};
        final Transaction txn = env.beginTransaction();
        final Transaction txn1 = env.beginTransaction();
        env.executeTransactionSafeTask(new Runnable() {
            @Override
            public void run() {
                bTrue[0] = true;
            }
        });
        env.executeTransactionSafeTask(new Runnable() {
            @Override
            public void run() {
                bFalse[0] = false;
            }
        });
        Thread.sleep(500);
        Assert.assertFalse(bTrue[0]);
        Assert.assertTrue(bFalse[0]);
        txn1.abort();
        Thread.sleep(500);
        Assert.assertFalse(bTrue[0]);
        Assert.assertTrue(bFalse[0]);
        txn.abort();
        Thread.sleep(500);
        Assert.assertTrue(bTrue[0]);
        Assert.assertFalse(bFalse[0]);
    }

    @Test
    public void testFlush() {
        final boolean[] ok = {true};
        final Environment env = getEnvironment();
        final ByteIterable key1 = StringBinding.stringToEntry("key1");
        final ByteIterable key2 = StringBinding.stringToEntry("key2");
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.commit();
        txn = env.beginTransaction();
        store.put(txn, key1, StringBinding.stringToEntry("value1"));
        executeParallelTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try {
                    assertEmptyValue(txn, store, key1);
                    assertEmptyValue(txn, store, key2);
                } catch (Throwable t) {
                    ok[0] = false;
                }
            }
        });
        txn.flush();
        store.put(txn, key2, StringBinding.stringToEntry("value2"));
        executeParallelTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try {
                    assertNotNullStringValue(txn, store, key1, "value1");
                    assertEmptyValue(txn, store, key2);
                } catch (Throwable t) {
                    ok[0] = false;
                }
            }
        });
        txn.flush();
        txn.abort();
        Assert.assertTrue(ok[0]);
        assertNotNullStringValue(store, key1, "value1");
        assertNotNullStringValue(store, key2, "value2");
    }

    @Test
    public void testRevert() {
        final Environment env = getEnvironment();
        final ByteIterable key1 = StringBinding.stringToEntry("key1");
        final ByteIterable key2 = StringBinding.stringToEntry("key2");
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.commit();
        txn = env.beginTransaction();
        store.put(txn, key1, StringBinding.stringToEntry("value1"));
        executeParallelTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, key2, StringBinding.stringToEntry("value2"));
            }
        });
        assertNotNullStringValue(store, key1, "value1");
        assertEmptyValue(store, key2);
        txn.revert();
        assertEmptyValue(store, key1);
        assertNotNullStringValue(store, key2, "value2");
        txn.abort();
    }

    @Test(expected = ReadonlyTransactionException.class)
    public void testExecuteInReadonlyTransaction() {
        final EnvironmentImpl env = getEnvironment();
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
    }

    @Test(expected = ReadonlyTransactionException.class)
    public void test_XD_447() {
        final EnvironmentImpl env = getEnvironment();
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        ec.setEnvIsReadonly(true);
        ec.setEnvReadonlyEmptyStores(true);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final StoreImpl store = env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn);
                final ArrayByteIterable wtfEntry = StringBinding.stringToEntry("WTF");
                store.put(txn, wtfEntry, wtfEntry);
            }
        });
    }

    @Test(expected = ReadonlyTransactionException.class)
    public void test_XD_447_() {
        final EnvironmentImpl env = getEnvironment();
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        ec.setEnvIsReadonly(true);
        ec.setEnvReadonlyEmptyStores(true);
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final StoreImpl store = env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn);
                store.delete(txn, StringBinding.stringToEntry("WTF"));
            }
        });
    }

    @Test
    public void test_XD_401() throws Exception {
        final Environment env = getEnvironment();
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("store", StoreConfig.WITH_DUPLICATES, txn);
            }
        });
        final Transaction txn = env.beginTransaction();
        final long started = txn.getStartTime();
        store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value"));
        Thread.sleep(200);
        try {
            Assert.assertTrue(txn.flush());
            Assert.assertTrue(txn.getStartTime() > started + 150);
            store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("new value"));
            Thread.sleep(200);
            txn.revert();
            Assert.assertTrue(txn.getStartTime() > started + 350);
        } finally {
            txn.abort();
        }
    }

    @Test
    public void test_XD_471() {
        final Environment env = getEnvironment();
        final Transaction[] txn = {null};
        DeferredIO.getJobProcessor().waitForLatchJob(new LatchJob() {
            @Override
            protected void execute() throws Throwable {
                txn[0] = env.beginTransaction();
                release();
            }
        }, 100);
        final Transaction tx = txn[0];
        Assert.assertNotNull(tx);
        tx.abort();
    }

    @Test
    public void test_XD_471_() {
        final Environment env = getEnvironment();
        final Transaction[] txn = {null};
        DeferredIO.getJobProcessor().waitForLatchJob(new LatchJob() {
            @Override
            protected void execute() throws Throwable {
                txn[0] = env.beginReadonlyTransaction();
                release();
            }
        }, 100);
        final Transaction tx = txn[0];
        Assert.assertNotNull(tx);
        tx.abort();
    }

    @Test(expected = ExodusException.class)
    public void testAfterCommit() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn).put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.commit();
        env.openStore("new store1", StoreConfig.WITHOUT_DUPLICATES, txn);
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterCommit2() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.commit();
        store.delete(txn, IntegerBinding.intToEntry(0));
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterCommit3() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.commit();
        txn.commit();
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterCommit4() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.commit();
        txn.abort();
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterAbort() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn).put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.abort();
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterAbort2() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.flush();
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.abort();
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterAbort3() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.flush();
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.abort();
        txn.abort();
        Assert.fail();
    }

    @Test(expected = ExodusException.class)
    public void testAfterAbort4() {
        final Environment env = getEnvironment();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.flush();
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        txn.abort();
        txn.commit();
        Assert.fail();
    }

    @Test
    @TestFor(issues = "XD-477")
    public void test_XD_477() {
        getEnvironment().getEnvironmentConfig().setEnvTxnReplayTimeout(500L);
        getEnvironment().executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
                getEnvironment().executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull Transaction txn) {
                        env.openStore("new store 2", StoreConfig.WITHOUT_DUPLICATES, txn);
                    }
                });
                txn.flush();
                Assert.assertFalse(txn.isExclusive());
                txn.revert();
                Assert.assertFalse(txn.isExclusive());
                // here transaction is idempotent and not exclusive
                try {
                    Thread.sleep(600);
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
                txn.revert();
                Assert.assertFalse(txn.isExclusive());
            }
        });
    }

    @Test
    @TestFor(issues = "XD-478")
    public void test_XD_478() {
        final EnvironmentImpl env = getEnvironment();
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
        final TransactionBase txn = env.beginTransaction();
        try {
            Assert.assertFalse(store.exists(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value")));
            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull Transaction tx) {
                    store.put(tx, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value"));
                }
            });
            txn.revert();
            Assert.assertTrue(store.exists(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value")));
        } finally {
            txn.abort();
        }
    }

    @Test
    @TestFor(issues = "XD-480") // the test will hang if XD-480 is not fixed
    public void testSuspendGCInTxn() {
        set1KbFileWithoutGC();
        final EnvironmentImpl env = getEnvironment();
        env.getEnvironmentConfig().setGcEnabled(true);
        env.getEnvironmentConfig().setGcMinUtilization(90);
        env.getEnvironmentConfig().setGcStartIn(0);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction t) {
                for (int i = 0; i < 30; ++i) {
                    final int j = i;
                    env.executeInTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull Transaction txn) {
                            store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(j));
                            store.put(txn, IntegerBinding.intToEntry(j / 2), IntegerBinding.intToEntry(j));
                        }
                    });
                }
            }
        });
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
                env.suspendGC();
            }
        });
    }
}
