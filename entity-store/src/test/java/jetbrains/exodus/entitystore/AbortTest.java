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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.env.*;
import jetbrains.exodus.util.IOUtil;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.File;

public class AbortTest extends TestCase {

    private Environment env;
    private final File dbPath = new File(TestUtil.createTempDir(), Double.toString(Math.random()));

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        //noinspection ResultOfMethodCallIgnored
        dbPath.mkdir();
        env = Environments.newInstance(dbPath.getPath());
    }

    public void testAbort() {
        final StoreConfig dbConfig = StoreConfig.WITHOUT_DUPLICATES;
        Transaction txn = env.beginTransaction();
        final Store store = env.openStore("testDatabase", dbConfig, txn);
        final ArrayByteIterable dbEntry = new ArrayByteIterable(new byte[4]);
        store.put(txn, dbEntry, dbEntry);
        txn.revert();
        Assert.assertTrue(store.count(txn) == 0);
        //env.setThreadTransaction(txn);
        store.put(txn, dbEntry, dbEntry);
        txn.flush();
        Assert.assertTrue(store.count(txn) == 1);
        txn.abort();
    }

    @Override
    protected void tearDown() throws Exception {
        env.close();
        deleteRecursively(dbPath);
        super.tearDown();
    }

    private static void deleteRecursively(final File dir) {
        for (final File file : IOUtil.listFiles(dir)) {
            if (file.isDirectory()) {
                deleteRecursively(file);
            }
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }
        if (!dir.delete()) {
            dir.deleteOnExit();
        }
    }
}
