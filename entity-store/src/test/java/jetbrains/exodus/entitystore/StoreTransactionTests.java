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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.env.ReadonlyTransactionException;
import jetbrains.exodus.util.UTFUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;

@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class StoreTransactionTests extends EntityStoreTestBase {

    public void testAutomaticClosingOpenCursorsInTransaction() throws Exception {
        final StoreTransaction txn = getStoreTransactionSafe();
        final Entity issue = txn.newEntity("Issue");
        for (int i = 0; i < 10; ++i) {
            issue.addLink("comment", txn.newEntity("Comment"));
        }
        boolean wasException = false;
        try {
            for (final Entity comment : issue.getLinks("comment")) {
                txn.flush();
                break;
            }
        } catch (Throwable t) {
            wasException = true;
        }
        Assert.assertFalse(wasException);
    }

    public void testAbort() {
        final StoreTransaction txn = getStoreTransactionSafe();
        txn.newEntity("Issue");
        Assert.assertTrue(txn.getAll("Issue").size() == 1);
        txn.flush();
        txn.newEntity("Issue");
        Assert.assertTrue(txn.getAll("Issue").size() == 2);
        txn.revert();
        Assert.assertTrue(txn.getAll("Issue").size() == 1);
    }

    public void testAbort2() {
        final StoreTransaction txn = getStoreTransactionSafe();
        txn.newEntity("Issue");
        Assert.assertTrue(txn.getAll("Issue").size() == 1);
        txn.revert();
        Assert.assertTrue(txn.getAll("Issue").size() == 0);
        txn.newEntity("Issue");
        Assert.assertTrue(txn.getAll("Issue").size() == 1);
        txn.flush();
        Assert.assertTrue(txn.getAll("Issue").size() == 1);
    }

    public void testAbortLinks() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        Entity issue = txn.newEntity("Issue");
        PersistentEntity issue_duplicated = txn.newEntity("Issue");
        issue_duplicated.addLink("duplicated", issue);
        txn.flush();
        Assert.assertTrue(issue_duplicated.getLinks("duplicated").size() == 1);
        issue_duplicated.deleteLink("duplicated", issue);
        txn.revert();
        Assert.assertTrue(issue_duplicated.getLinks("duplicated").size() == 1);
    }

    public void testIsolatedBlobs() throws InterruptedException, IOException {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        Entity issue = txn.newEntity("Issue");
        issue.setBlobString("desc", "Happy New Year");
        txn.flush();
        Assert.assertEquals("Happy New Year", issue.getBlobString("desc"));
        final PersistentStoreTransaction newTxn = getEntityStore().beginTransaction();
        Assert.assertEquals("Happy New Year", issue.getBlobString("desc"));
        issue.deleteBlob("desc");
        newTxn.commit();
        Thread.sleep(500);
        final InputStream s = issue.getBlob("desc");
        Assert.assertNotNull(s);
        Assert.assertEquals("Happy New Year", UTFUtil.readUTF(s));
    }

    public void testEmptyStoresRelatedTo_XD_439() {
        final PersistentEntityStoreImpl entityStore = getEntityStore();
        entityStore.getEnvironment().getEnvironmentConfig().setEnvIsReadonly(true);
        entityStore.getEnvironment().getEnvironmentConfig().setEnvReadonlyEmptyStores(true);
        Assert.assertEquals(0L, (long) entityStore.computeInReadonlyTransaction(new StoreTransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull StoreTransaction txn) {
                return txn.getAll("Issue").size();
            }
        }));
        entityStore.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction txn) {
                entityStore.getLastVersion((PersistentStoreTransaction) txn, new PersistentEntityId(0, 0));
            }
        });
        entityStore.getEnvironment().getEnvironmentConfig().setEnvIsReadonly(false);
        entityStore.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction txn) {
                final Entity issue = txn.newEntity("Issue");
                issue.setBlobString("desc", "Happy New Year");
            }
        });
        Assert.assertEquals(1L, (long) entityStore.computeInReadonlyTransaction(new StoreTransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull StoreTransaction txn) {
                return txn.getAll("Issue").size();
            }
        }));
    }

    public void testExecuteInTransaction() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("summary", "Do nothing");
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                issue.setProperty("summary", "Release Xodus");
            }
        });
        Assert.assertFalse(txn.flush());
        Assert.assertEquals("Release Xodus", issue.getProperty("summary"));
    }

    public void testExecuteInReadonlyTransaction() {
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getEntityStore().executeInReadonlyTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        txn.newEntity("Issue");
                    }
                });
            }
        }, ReadonlyTransactionException.class);
    }

    public void testComputeInReadonlyTransaction() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("summary", "it's ok");
        txn.flush();
        Assert.assertEquals("it's ok", getEntityStore().computeInReadonlyTransaction(new StoreTransactionalComputable<Comparable>() {
            @Override
            public Comparable compute(@NotNull StoreTransaction txn) {
                return issue.getProperty("summary");
            }
        }));
    }

    @TestFor(issues = "XD-495")
    public void testNewEntityInReadonlyTransaction() {
        setReadonly();
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        txn.newEntity("Issue");
                    }
                });
            }
        }, ReadonlyTransactionException.class);
    }

    @TestFor(issues = "XD-495")
    public void testNewEntityInReadonlyTransaction2() {
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                txn.newEntity("Issue");
            }
        });
        setReadonly();
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        txn.newEntity("Issue");
                    }
                });
            }
        }, ReadonlyTransactionException.class);
    }

    @TestFor(issues = "XD-495")
    public void testNewEntityInReadonlyTransaction3() {
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                txn.newEntity("Issue");
            }
        });
        setReadonly();
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction txn) {
                TestUtil.runWithExpectedException(new Runnable() {
                    @Override
                    public void run() {
                        txn.newEntity("Issue");
                    }
                }, ReadonlyTransactionException.class);
            }
        });
    }

    private void setReadonly() {
        getEntityStore().getEnvironment().getEnvironmentConfig().setEnvIsReadonly(true);
    }
}
