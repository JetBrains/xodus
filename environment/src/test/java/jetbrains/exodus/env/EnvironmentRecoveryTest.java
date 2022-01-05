/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.NullLoggable;
import jetbrains.exodus.tree.btree.BTreeBase;
import org.junit.Test;

public class EnvironmentRecoveryTest extends EnvironmentTestsBase {

    private int[] SEQ;
    private int A;
    private int B;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env.getEnvironmentConfig().setGcEnabled(false);
        SEQ = new int[]{BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE,
                BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE,
                BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE};
        A = SEQ.length - 5;
        B = SEQ.length - 10;
    }

    @Test
    public void testLastLoggableIncomplete0() {
        cutAndCheckLastLoggableIncomplete(106, SEQ.length);
    }

    @Test
    public void testLastLoggableIncomplete1() {
        cutAndCheckLastLoggableIncomplete(99, A);
    }

    @Test
    public void testLastLoggableIncomplete2() {
        cutAndCheckLastLoggableIncomplete(89, A);
    }

    @Test
    public void testLastLoggableIncomplete3() {
        cutAndCheckLastLoggableIncomplete(67, A);
    }

    @Test
    public void testLastLoggableIncomplete4() {
        cutAndCheckLastLoggableIncomplete(61, A);
    }

    @Test
    public void testLastLoggableIncomplete5() {
        cutAndCheckLastLoggableIncomplete(56, A);
    }

    @Test
    public void testLastLoggableIncomplete6() {
        cutAndCheckLastLoggableIncomplete(49, B);
    }

    @Test
    public void testLastLoggableIncomplete7() {
        cutAndCheckLastLoggableIncomplete(41, B);
    }

    @Test
    public void testLastLoggableIncomplete8() {
        cutAndCheckLastLoggableIncomplete(23, B);
    }

    @Test
    public void testLastLoggableIncomplete9() {
        cutAndCheckLastLoggableIncomplete(17, B);
    }

    @Test
    public void testLastLoggableIncomplete10() {
        cutAndCheckLastLoggableIncomplete(12, B);
    }

    @Test
    public void testLastLoggableIncomplete11() {
        cutAndCheckLastLoggableIncomplete(5, B); // recovery should create same empty environment
    }

    @Test
    public void testLastLoggableIncomplete12() {
        cutAndCheckLastLoggableIncomplete(0, B); // recovery should create same empty environment
    }

    @Test
    public void testLastLoggableIncomplete13() {
        Log log = env.getLog();
        final long fileSize = env.getEnvironmentConfig().getLogFileSize() * 1024;
        log.beginWrite();
        for (int i = 0; i < fileSize; ++i) {
            log.write(NullLoggable.create());
        }
        log.endWrite();
        env.executeInTransaction(txn -> env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn));
        env.executeInTransaction(txn -> env.openStore("another_store", StoreConfig.WITHOUT_DUPLICATES, txn));
        env.close();
        writer.openOrCreateBlock(fileSize, 0);
        writer.close();
        env = newEnvironmentInstance(LogConfig.create(reader, writer).setFileSize(env.getEnvironmentConfig().getLogFileSize())); // recovery pending
        assertLoggableTypes(B, env.getLog().getLoggableIterator(0), SEQ);
    }

    private void cutAndCheckLastLoggableIncomplete(int cutAt, int max) {
        env.executeInTransaction(txn -> env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn));
        env.executeInTransaction(txn -> env.openStore("another_store", StoreConfig.WITHOUT_DUPLICATES, txn));
        assertLoggableTypes(getLog(), 0, SEQ);
        env.close();

        writer.openOrCreateBlock(0, cutAt);
        writer.close();

        env = newEnvironmentInstance(LogConfig.create(reader, writer)); // recovery pending
        assertLoggableTypes(max, env.getLog().getLoggableIterator(0), SEQ);
    }

}
