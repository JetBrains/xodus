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

import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.EnvironmentTestsBase;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.log.LogConfig;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class VfsStressTests extends EnvironmentTestsBase {

    private VirtualFileSystem vfs;

    @Override
    public void setUp() throws Exception {
        System.setProperty(EnvironmentConfig.TREE_MAX_PAGE_SIZE, "8");
        super.setUp();
    }

    @Override
    protected void createEnvironment() {
        env = newContextualEnvironmentInstance(LogConfig.create(reader, writer));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        vfs.shutdown();
        super.tearDown();
    }

    @Test
    public void testLuceneDirectoryLike() throws IOException {
        final VfsConfig config = new VfsConfig();
        config.setClusteringStrategy(new ClusteringStrategy.QuadraticClusteringStrategy(4));
        vfs = new VirtualFileSystem(getEnvironment(), config, StoreConfig.WITHOUT_DUPLICATES);
        vfs.setClusterConverter(getClusterConverter());
        Transaction txn = env.beginTransaction();
        File bigFile = vfs.createFile(txn, "big_file");
        txn.commit();
        for (int i = 0; i < 1000; ++i) {
            txn = env.beginTransaction();
            String temp = String.valueOf(i) + Math.random();
            OutputStream outputStream = vfs.writeFile(txn, vfs.createFile(txn, temp));
            outputStream.write(("testLuceneDirectoryLike" + i).getBytes());
            outputStream.close();
            txn.commit();
            txn = env.beginTransaction();
            final File tempFile = vfs.openFile(txn, temp, false);
            Assert.assertNotNull(tempFile);
            final long sourceLen = vfs.getFileLength(txn, bigFile);
            outputStream = vfs.appendFile(txn, bigFile);
            InputStream inputStream = vfs.readFile(txn, tempFile);
            int count = 0;
            while (true) {
                int c = inputStream.read();
                if (c < 0) {
                    break;
                }
                ++count;
                outputStream.write(c);
            }
            outputStream.close();
            Assert.assertEquals(("testLuceneDirectoryLike" + i).getBytes().length, count);
            Assert.assertEquals(count, vfs.getFileLength(txn, tempFile));
            Assert.assertEquals(vfs.getFileLength(txn, bigFile), sourceLen + vfs.getFileLength(txn, tempFile));
            vfs.deleteFile(txn, temp);
            txn.commit();
        }
    }

    @Nullable
    ClusterConverter getClusterConverter() {
        return null;
    }
}
