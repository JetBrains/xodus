/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.tree.btree.BTreeBase;
import org.junit.Test;

import java.io.IOException;

public class LogRecoveryTest extends EnvironmentTestsBase {
    private int[] seq;

    private int a;

    private int b;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env.getEnvironmentConfig().setGcEnabled(false);
        seq = new int[]{BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE, BTreeBase.BOTTOM_ROOT,
                BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE};
        a = seq.length - 1;
        b = seq.length - 2;
    }

    @Test
    public void testLastLoggableIncomplete0() throws IOException {
        cutAndCheckLastLoggableIncomplete(0, seq.length);
    }

    @Test
    public void testLastLoggableIncomplete1() throws IOException {
        cutAndCheckLastLoggableIncomplete(1, a);
    }

    @Test
    public void testLastLoggableIncomplete2() throws IOException {
        cutAndCheckLastLoggableIncomplete(2, a);
    }

    @Test
    public void testLastLoggableIncomplete3() throws IOException {
        cutAndCheckLastLoggableIncomplete(3, a);
    }

    @Test
    public void testLastLoggableIncomplete4() throws IOException {
        cutAndCheckLastLoggableIncomplete(4, a);
    }

    @Test
    public void testLastLoggableIncomplete5() throws IOException {
        cutAndCheckLastLoggableIncomplete(5, a);
    }

    @Test
    public void testLastLoggableIncomplete6() throws IOException {
        cutAndCheckLastLoggableIncomplete(6, a);
    }

    @Test
    public void testLastLoggableIncomplete7() throws IOException {
        cutAndCheckLastLoggableIncomplete(7, a);
    }

    @Test
    public void testLastLoggableIncomplete8() throws IOException {
        cutAndCheckLastLoggableIncomplete(8, b);
    }

    private void cutAndCheckLastLoggableIncomplete(int cutSize, int max) {
        openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES);

        /*final Iterator<Loggable> itr = getLog().getLoggablesIterator(0);
        while (itr.hasNext()) {
            final Loggable next = itr.next();
            System.out.println(next.getType() + " @ " + next.getAddress());
        }*/

        assertLoggableTypes(getLog(), 0, seq);
        env.close();
        env = null;

        final long size = reader.getBlock(0).length();
        writer.openOrCreateBlock(0, size - cutSize);
        writer.close();

        // only 'max' first loggables should remain
        assertLoggableTypes(max, Environments.newLogInstance(LogConfig.create(reader, writer)).getLoggableIterator(0), seq);
    }

}
