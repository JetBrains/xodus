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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.*;
import java.net.URL;

@SuppressWarnings("ConstantConditions")
public class EntityBlobTests extends EntityStoreTestBase {
    @Override
    protected String[] casesThatDontNeedExplicitTxn() {
        return new String[]{"testEntityStoreClear"};
    }

    public void testInPlaceBlobs() throws Exception {
        checkBlobs(getStoreTransaction());
    }

    public void testBlobs() throws Exception {
        getEntityStore().getConfig().setMaxInPlaceBlobSize(0);
        checkBlobs(getStoreTransaction());
    }

    public void testBlobFiles() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        Assert.assertNull(entity.getBlob("body"));
        entity.setBlob("body", createTempFile("my body"));
        entity.setBlob("body2", createTempFile("my body2"));
        Assert.assertEquals("my body", entity.getBlobString("body"));
        Assert.assertEquals("my body2", entity.getBlobString("body2"));
        txn.flush();
        Assert.assertEquals("my body", entity.getBlobString("body"));
        Assert.assertEquals("my body2", entity.getBlobString("body2"));
    }

    @TestFor(issues = "XD-675")
    public void testBlobFiles2() throws Exception {
        getEntityStore().getConfig().setMaxInPlaceBlobSize(0);
        testBlobFiles();
    }

    public void testDeleteBlobs() throws IOException {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        issue.setBlobString("description", "This is a test issue");
        issue.setBlob("body", createTempFile("my body"));
        txn.flush();
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"));
        Assert.assertEquals("my body", issue.getBlobString("body"));
        issue.deleteBlob("description");
        issue.deleteBlob("body");
        txn.flush();
        Assert.assertNull(issue.getBlob("description"));
        Assert.assertNull(issue.getBlobString("description"));
        Assert.assertNull(issue.getBlob("body"));
        Assert.assertNull(issue.getBlobString("body"));
    }

    public void testDeleteBlobsWithinTxn() throws IOException {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        issue.setBlobString("description", "This is a test issue");
        issue.setBlob("body", createTempFile("my body"));
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"));
        Assert.assertEquals("my body", issue.getBlobString("body"));
        issue.deleteBlob("description");
        issue.deleteBlob("body");
        Assert.assertNull(issue.getBlob("description"));
        Assert.assertNull(issue.getBlobString("description"));
        Assert.assertNull(issue.getBlob("body"));
        Assert.assertNull(issue.getBlobString("body"));
        issue.setBlobString("description", "This is a test issue");
        issue.setBlob("body", createTempFile("my body"));
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"));
        Assert.assertEquals("my body", issue.getBlobString("body"));
        issue.deleteBlob("description");
        issue.deleteBlob("body");
        txn.flush();
        Assert.assertNull(issue.getBlob("description"));
        Assert.assertNull(issue.getBlobString("description"));
        Assert.assertNull(issue.getBlob("body"));
        Assert.assertNull(issue.getBlobString("body"));
    }

    public void testEmptyBlobString_XD_365() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        issue.setBlobString("description", "");
        Assert.assertNotNull(issue.getBlobString("description"));
        Assert.assertEquals("", issue.getBlobString("description"));
        txn.flush();
        Assert.assertNotNull(issue.getBlobString("description"));
        Assert.assertEquals("", issue.getBlobString("description"));
    }

    public void testNonAsciiBlobString() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity issue = txn.newEntity("Issue");
        issue.setBlobString("description", "абвгдеёжзийклмнопрстуфхкцчшщъыьэюя");
        txn.flush();
        Assert.assertEquals("абвгдеёжзийклмнопрстуфхкцчшщъыьэюя", issue.getBlobString("description"));
    }

    @TestFor(issues = "JT-44824")
    public void testLargeBlobString() {
        StringBuilder builder = new StringBuilder();
        final int blobStringSize = 80000;
        for (int i = 0; i < blobStringSize; ++i) {
            builder.append(' ');
        }
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity issue = txn.newEntity("Issue");
        issue.setBlobString("blank", builder.toString());
        txn.flush();
        final String blank = issue.getBlobString("blank");
        for (int i = 0; i < blobStringSize; ++i) {
            Assert.assertEquals(' ', blank.charAt(i));
        }
    }

    public void testXD_362() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        Assert.assertNull(entity.getBlob("body"));
        final URL data = this.getClass().getClassLoader().getResource("testXD_362.data");

        entity.setBlob("body", data.openStream());
        Assert.assertTrue(entity.getBlobSize("body") > 0L);
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream(), false));
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream(), false));
        txn.flush();
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream()));
    }

    public void testBlobOverwrite() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        txn.flush();
        final Entity entity = txn.newEntity("Issue");
        Assert.assertNull(entity.getBlob("body"));
        entity.setBlob("body", string2Stream("body"));
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")));
        entity.setBlob("body", string2Stream("body1"));
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body1")));
        txn.flush();
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body1")));
        entity.setBlob("body", string2Stream("body2"));
        txn.flush();
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body2")));
    }

    public void testSingleNameBlobAndProperty() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        Assert.assertNull(entity.getBlob("body"));
        entity.setBlob("body", string2Stream("stream body"));
        entity.setProperty("body", "string body");
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("stream body")));
        txn.flush();
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("stream body")));
        Assert.assertEquals(entity.getProperty("body"), "string body");
    }

    public void testMultipleBlobs() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        for (int i = 0; i < 2000; ++i) {
            entity.setBlob("body" + i, string2Stream("body" + i));
        }
        txn.flush();
        for (int i = 0; i < 2000; ++i) {
            Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body" + i), string2Stream("body" + i)));
        }
    }

    @SuppressWarnings({"ObjectAllocationInLoop"})
    public void testConcurrentMultipleBlobs() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        for (int i = 0; i < 2000; ++i) {
            entity.setBlob("body" + i, string2Stream("body" + i));
        }
        txn.flush();
        final boolean[] wereExceptions = {false};
        Thread[] threads = new Thread[10];
        for (int t = 0; t < threads.length; ++t) {
            final Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    final PersistentStoreTransaction txn = getEntityStore().beginTransaction();
                    for (int i = 0; i < 2000; ++i) {
                        try {
                            if (!TestUtil.streamsEqual(entity.getBlob("body" + i), string2Stream("body" + i))) {
                                wereExceptions[0] = true;
                                break;
                            }
                        } catch (Exception e) {
                            wereExceptions[0] = true;
                            break;
                        }
                    }
                    txn.commit();
                }
            });
            thread.start();
            threads[t] = thread;
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertFalse(wereExceptions[0]);
    }

    @TestFor(issues = "XD-531")
    public void testEntityStoreClear() {
        final PersistentEntityStoreImpl store = getEntityStore();
        store.getConfig().setMaxInPlaceBlobSize(0);
        store.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                txn.newEntity("User").setBlobString("bio", "I was born");
            }
        });
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                final InputStream content = store.getBlobVault().getContent(0, ((PersistentStoreTransaction) txn).getEnvironmentTransaction());
                assertNotNull(content);
                try {
                    content.close();
                } catch (IOException e) {
                    throw ExodusException.toExodusException(e);
                }
            }
        });
        store.clear();
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                assertNull(store.getBlobVault().getContent(0, ((PersistentStoreTransaction) txn).getEnvironmentTransaction()));
            }
        });
    }

    private static void checkBlobs(StoreTransaction txn) throws IOException {
        final Entity entity = txn.newEntity("Issue");
        Assert.assertNull(entity.getBlob("body"));
        final int length = "body".getBytes().length;
        entity.setBlob("body", string2Stream("body"));
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")));
        Assert.assertEquals(length, entity.getBlobSize("body"));
        txn.flush();
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")));
        Assert.assertEquals(length, entity.getBlobSize("body"));
    }

    private static File createTempFile(String content) throws IOException {
        final File tempFile = File.createTempFile("test", null);
        DataOutputStream output = new DataOutputStream(new FileOutputStream(tempFile));
        output.writeUTF(content);
        output.close();
        return tempFile;
    }

    private static InputStream string2Stream(String s) {
        return new ByteArrayInputStream(s.getBytes());
    }
}
