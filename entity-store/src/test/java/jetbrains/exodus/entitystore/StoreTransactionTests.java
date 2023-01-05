/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;

public class StoreTransactionTests extends EntityStoreTestBase {

    public void testAutomaticClosingOpenCursorsInTransaction() {
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
        Assert.assertEquals(1, txn.getAll("Issue").size());
        txn.flush();
        txn.newEntity("Issue");
        Assert.assertEquals(2, txn.getAll("Issue").size());
        txn.revert();
        Assert.assertEquals(1, txn.getAll("Issue").size());
    }

    public void testAbort2() {
        final StoreTransaction txn = getStoreTransactionSafe();
        txn.newEntity("Issue");
        Assert.assertEquals(1, txn.getAll("Issue").size());
        txn.revert();
        Assert.assertEquals(0, txn.getAll("Issue").size());
        txn.newEntity("Issue");
        Assert.assertEquals(1, txn.getAll("Issue").size());
        txn.flush();
        Assert.assertEquals(1, txn.getAll("Issue").size());
    }

    public void testAbortLinks() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        Entity issue = txn.newEntity("Issue");
        PersistentEntity issue_duplicated = txn.newEntity("Issue");
        issue_duplicated.addLink("duplicated", issue);
        txn.flush();
        Assert.assertEquals(1, issue_duplicated.getLinks("duplicated").size());
        issue_duplicated.deleteLink("duplicated", issue);
        txn.revert();
        Assert.assertEquals(1, issue_duplicated.getLinks("duplicated").size());
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
        Assert.assertEquals(0L, (long) entityStore.computeInReadonlyTransaction(txn -> txn.getAll("Issue").size()));
        entityStore.executeInTransaction((StoreTransactionalExecutable) txn -> entityStore.getLastVersion((PersistentStoreTransaction) txn, new PersistentEntityId(0, 0)));
        entityStore.getEnvironment().getEnvironmentConfig().setEnvIsReadonly(false);
        entityStore.executeInTransaction(txn -> {
            final Entity issue = txn.newEntity("Issue");
            issue.setBlobString("desc", "Happy New Year");
        });
        Assert.assertEquals(1L, (long) entityStore.computeInReadonlyTransaction(txn -> txn.getAll("Issue").size()));
    }

    public void testExecuteInTransaction() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("summary", "Do nothing");
        getEntityStore().executeInTransaction(txn1 -> issue.setProperty("summary", "Release Xodus"));
        Assert.assertFalse(txn.flush());
        Assert.assertEquals("Release Xodus", issue.getProperty("summary"));
    }

    public void testExecuteInReadonlyTransaction() {
        final PersistentEntityStoreImpl store = getEntityStore();
        store.executeInTransaction(txn -> txn.newEntity("Issue"));
        TestUtil.runWithExpectedException(() -> store.executeInReadonlyTransaction(txn -> txn.newEntity("Issue")), ReadonlyTransactionException.class);
    }

    public void testComputeInReadonlyTransaction() {
        final PersistentStoreTransaction txn = getStoreTransactionSafe();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("summary", "it's ok");
        txn.flush();
        Assert.assertEquals("it's ok", getEntityStore().computeInReadonlyTransaction(txn1 -> issue.getProperty("summary")));
    }

    @TestFor(issue = "XD-495")
    public void testNewEntityInReadonlyTransaction() {
        setReadonly();
        TestUtil.runWithExpectedException(() -> getEntityStore().executeInTransaction(txn -> txn.newEntity("Issue")), ReadonlyTransactionException.class);
    }

    @TestFor(issue = "XD-495")
    public void testNewEntityInReadonlyTransaction2() {
        getEntityStore().executeInTransaction(txn -> txn.newEntity("Issue"));
        setReadonly();
        TestUtil.runWithExpectedException(() -> getEntityStore().executeInTransaction(txn -> txn.newEntity("Issue")), ReadonlyTransactionException.class);
    }

    @TestFor(issue = "XD-495")
    public void testNewEntityInReadonlyTransaction3() {
        getEntityStore().executeInTransaction(txn -> txn.newEntity("Issue"));
        setReadonly();
        getEntityStore().executeInTransaction(txn -> TestUtil.runWithExpectedException(() -> txn.newEntity("Issue"), ReadonlyTransactionException.class));
    }

    private void setReadonly() {
        getEntityStore().getEnvironment().getEnvironmentConfig().setEnvIsReadonly(true);
    }
}
