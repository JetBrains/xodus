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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.INode;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BTreeSimpleTest extends BTreeTestBase {

    @Test
    public void testEmptyTree() throws IOException {
        checkEmptyTree(t = new BTreeEmpty(log, false, 1));

        tm = new BTreeEmpty(log, false, 1).getMutableCopy();

        checkEmptyTree(tm);

        long address = tm.save();

        reopen();
        t = new BTree(log, address, true, 1);

        checkEmptyTree(t);
    }

    @Test
    public void testPutSaveGet() throws IOException {
        // put
        tm = new BTreeEmpty(log, false, 1).getMutableCopy();

        final INode ln1 = kv("1", "vadim");
        getTreeMutable().put(ln1);

        assertEquals(1, tm.getSize());
        assertEquals(true, tm.hasKey(key("1")));
        assertEquals(true, tm.hasPair(key("1"), value("vadim")));

        assertTrue(getTreeMutable().getRoot() instanceof BottomPageMutable);
        BottomPageMutable bpm = (BottomPageMutable) getTreeMutable().getRoot();
        assertEquals(1, bpm.size);
        assertEquals(ln1, bpm.keys[0]);
        assertEquals(Loggable.NULL_ADDRESS, bpm.keysAddresses[0]);
        assertMatchesIterator(tm, ln1);

        valueEquals("vadim", tm.get(key("1")));

        // save
        long newRootAddress = tm.save();
        valueEquals("vadim", tm.get(key("1")));

        // get
        t = new BTree(log, newRootAddress, false, 1);

        TreeAwareRunnable r = new TreeAwareRunnable() {
            @Override
            public void run() {
                assertEquals(1, t.getSize());
                assertEquals(true, tm.hasKey(key("1")));
                assertEquals(true, tm.hasPair(key("1"), value("vadim")));

                assertTrue(getTree().getRoot() instanceof BottomPage);
                BottomPage bp = (BottomPage) getTree().getRoot();
                assertEquals(1, bp.size);
                assertTrue(bp.getKeyAddress(0) != Loggable.NULL_ADDRESS);
                assertEquals(ln1, bp.get(key("1")));

                valueEquals("vadim", t.get(key("1")));
            }
        };

        r.run();

        // get after log reopen
        reopen();

        t = new BTree(log, newRootAddress, false, 1);

        r.run();
    }

}
