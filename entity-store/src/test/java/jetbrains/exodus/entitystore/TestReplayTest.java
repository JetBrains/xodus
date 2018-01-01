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

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.util.IOUtil;
import junit.framework.TestCase;

import java.io.File;

public class TestReplayTest extends TestCase {

    private TestTransactionReplayPersistentEntityStoreImpl store;
    private final File dbPath = new File(TestUtil.createTempDir(), Double.toString(Math.random()));

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        //noinspection ResultOfMethodCallIgnored
        dbPath.mkdir();
        final Environment env = Environments.newInstance(dbPath.getPath());
        store = new TestTransactionReplayPersistentEntityStoreImpl(env, "test");
    }

    public void testReplayFlush() {
        final PersistentStoreTransaction txn = store.beginTransaction();
        try {
            txn.newEntity("whatever");
            assertFalse(txn.flush());
            txn.newEntity("whatever");
            txn.flush();
        } finally {
            txn.abort();
        }
    }

    public void testReplayFlushFactor2() {
        store.setReplayFactor(2);
        final PersistentStoreTransaction txn = store.beginTransaction();
        try {
            txn.newEntity("whatever");
            assertFalse(txn.flush());
            txn.newEntity("whatever");
            assertFalse(txn.flush());
            txn.newEntity("whatever");
            txn.flush();
        } finally {
            txn.abort();
        }
    }

    public void testReplayCommit() {
        final PersistentStoreTransaction txn = store.beginTransaction();
        txn.newEntity("whatever");
        assertFalse(txn.commit());
        txn.newEntity("whatever");
        txn.commit();
    }

    public void testReplayCommitFactor2() {
        store.setReplayFactor(2);
        final PersistentStoreTransaction txn = store.beginTransaction();
        txn.newEntity("whatever");
        assertFalse(txn.commit());
        txn.newEntity("whatever");
        assertFalse(txn.commit());
        txn.newEntity("whatever");
        txn.commit();
    }

    @Override
    protected void tearDown() throws Exception {
        store.close();
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
