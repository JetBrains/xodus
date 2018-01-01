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
package jetbrains.exodus.vfs;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.TreeSet;

public class VfsFileTests extends VfsTestsBase {

    @Test
    public void testFileCreation() {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        txn.commit();
        Assert.assertEquals(0L, file0.getDescriptor());
    }

    @Test
    public void testFileCreation1() {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.openFile(txn, "file0", true);
        txn.commit();
        Assert.assertNotNull(file0);
        Assert.assertEquals(0L, file0.getDescriptor());
    }

    @Test
    public void testCreateExistingFile() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final File file0 = vfs.createFile(txn, "file0");
                txn.flush();
                Assert.assertEquals(0L, file0.getDescriptor());
                TestUtil.runWithExpectedException(new Runnable() {
                    @Override
                    public void run() {
                        vfs.createFile(txn, "file0");
                    }
                }, FileExistsException.class);
            }
        });
    }

    @Test
    public void testCreateExistingFile2() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final File file0 = vfs.openFile(txn, "file0", true);
                txn.flush();
                Assert.assertNotNull(file0);
                Assert.assertEquals(0L, file0.getDescriptor());
                TestUtil.runWithExpectedException(new Runnable() {
                    @Override
                    public void run() {
                        vfs.createFile(txn, "file0");
                    }
                }, FileExistsException.class);
            }
        });
    }

    @Test
    public void testCreateUniqueFile() {
        final Transaction txn = env.beginTransaction();
        final File file = vfs.createUniqueFile(txn, "file");
        txn.commit();
        Assert.assertEquals(0L, file.getDescriptor());
        Assert.assertTrue(file.getPath().startsWith("file"));
    }

    @Test
    public void testOpenFile() {
        testFileCreation();
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.openFile(txn, "file0", false);
        txn.commit();
        Assert.assertNotNull(file0);
        Assert.assertEquals(0L, file0.getDescriptor());
        Assert.assertTrue(file0.getCreated() > 0);
        Assert.assertTrue(file0.getLastModified() > 0);
    }

    @Test
    public void testRenameFile() throws InterruptedException {
        testFileCreation();
        final Transaction txn = env.beginTransaction();
        File file0 = vfs.openFile(txn, "file0", false);
        txn.revert();
        Assert.assertNotNull(file0);
        final long fd = file0.getDescriptor();
        final long created = file0.getCreated();
        final long lastModified = file0.getLastModified();
        Thread.sleep(50);
        vfs.renameFile(txn, file0, "file1");
        txn.flush();
        file0 = vfs.openFile(txn, "file0", false);
        File file1 = vfs.openFile(txn, "file1", false);
        txn.abort();
        Assert.assertNull(file0);
        Assert.assertNotNull(file1);
        Assert.assertEquals(fd, file1.getDescriptor());
        Assert.assertEquals(created, file1.getCreated());
        Assert.assertTrue(file1.getLastModified() > lastModified);
    }

    @Test
    public void testDeleteFile() {
        Transaction txn = env.beginTransaction();
        vfs.createFile(txn, "file0");
        txn.commit();
        txn = env.beginTransaction();
        final File file0 = vfs.deleteFile(txn, "file0");
        txn.commit();
        Assert.assertNotNull(file0);
        Assert.assertEquals("file0", file0.getPath());
    }

    @Test
    public void testDeleteFile2() {
        Transaction txn = env.beginTransaction();
        vfs.createFile(txn, "file0");
        txn.flush();
        vfs.deleteFile(txn, "file0");
        txn.flush();
        Assert.assertNull(vfs.openFile(txn, "file0", false));
        final File file0 = vfs.openFile(txn, "file0", true);
        txn.commit();
        Assert.assertNotNull(file0);
        Assert.assertEquals("file0", file0.getPath());
    }

    @Test
    public void testDeleteFile3() throws IOException {
        final Transaction txn = env.beginTransaction();
        for (int i = 0; i < 10; ++i) {
            final File file0 = vfs.createFile(txn, "file0");
            final OutputStream outputStream = vfs.writeFile(txn, file0);
            outputStream.write("vain bytes to be deleted".getBytes());
            outputStream.close();
            txn.flush();
            Assert.assertNotNull(vfs.deleteFile(txn, "file0"));
            txn.flush();
        }
        Assert.assertEquals(0L, vfs.getContents().count(txn));
        txn.commit();
    }

    @Test
    public void testDeleteFile4() throws IOException {
        final Transaction txn = env.beginTransaction();
        for (int i = 0; i < 50; ++i) {
            final File file0 = vfs.createFile(txn, "file0");
            final OutputStream outputStream = vfs.writeFile(txn, file0);
            for (int j = 0; j < i; ++j) {
                outputStream.write(new byte[vfs.getConfig().getClusteringStrategy().getFirstClusterSize()]);
            }
            outputStream.write("vain bytes to be deleted".getBytes());
            for (int j = 0; j < i + 1; ++j) {
                outputStream.write(new byte[vfs.getConfig().getClusteringStrategy().getFirstClusterSize()]);
            }
            outputStream.close();
            txn.flush();
            Assert.assertNotNull(vfs.deleteFile(txn, "file0"));
            txn.flush();
        }
        Assert.assertEquals(0L, vfs.getContents().count(txn));
        txn.commit();
    }

    @Test
    public void testNumberOfFiles() {
        final Transaction txn = env.beginTransaction();
        final HashSet<String> files = new HashSet<>();
        final int numberOfFiles = (int) (Math.random() * 1000 + 5000);
        for (int i = 0; i < numberOfFiles; ++i) {
            final String file = "file" + Math.random();
            if (!files.contains(file)) {
                files.add(file);
                vfs.createFile(txn, file);
            }
        }
        Assert.assertEquals(files.size(), vfs.getNumberOfFiles(txn));
        txn.commit();
    }

    @Test
    public void testTouchFile() throws InterruptedException {
        final Transaction txn = env.beginTransaction();
        vfs.createFile(txn, "file0");
        txn.flush();
        File file0 = vfs.openFile(txn, "file0", false);
        Assert.assertNotNull(file0);
        Assert.assertEquals(file0.getCreated(), file0.getLastModified());
        Thread.sleep(100);
        vfs.touchFile(txn, file0);
        txn.flush();
        file0 = vfs.openFile(txn, "file0", false);
        Assert.assertNotNull(file0);
        Assert.assertTrue(file0.getCreated() + 50 < file0.getLastModified());
        txn.commit();
    }

    @Test
    public void testFileEnumeration() {
        final Transaction txn = env.beginTransaction();
        final TreeSet<String> files = new TreeSet<>();
        final int numberOfFiles = (int) (Math.random() * 1000 + 5000);
        for (int i = 0; i < numberOfFiles; ++i) {
            final String file = "file" + Math.random();
            if (!files.contains(file)) {
                files.add(file);
                vfs.createFile(txn, file);
            }
        }
        final Iterator<String> it = files.iterator();
        for (final File file : vfs.getFiles(txn)) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(file.getPath(), it.next());
        }
        Assert.assertFalse(it.hasNext());
        txn.commit();
    }
}