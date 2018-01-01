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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.INode;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class BTreeDeleteSpecificTest extends BTreeTestBase {

    public BTreeBalancePolicy policy = new BTreeBalancePolicy(10);

    private BTreeMutable refresh() {
        long a = tm.save();
        t = new BTree(log, policy, a, false, 1);
        tm = getTree().getMutableCopy();
        return getTreeMutable();
    }

    @Test
    public void testDeleteKeys() throws IOException {
        tm = new BTreeEmpty(log, policy, false, 1).getMutableCopy();
        for (int i = 0; i < 125; i++) {
            getTreeMutable().put(kv(i, "k" + i));
        }
        ByteIterable key = getTreeMutable().getRoot().getKey(1).getKey();
        refresh();
        tm.delete(key);
        assertTrue(getTreeMutable().getRoot().getKey(1).compareKeyTo(getTreeMutable().getRoot().getChild(1).getMinKey().getKey()) == 0);
    }

    @Test
    public void testDeleteNoDuplicatesDeleteFirstTwoPages() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), false, 1).getMutableCopy();

        for (int i = 0; i < 20; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        long a = tm.save();
        tm = new BTree(log, new BTreeBalancePolicy(4), a, false, 1).getMutableCopy();

        dump(getTreeMutable());

        for (int i = 0; i < 8; i++) {
            tm.delete(key(i));
            dump(getTreeMutable());
        }

        assertEquals(12, tm.getSize());
    }

    @Test
    public void testDeleteNotExistingKeys() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), false, 1).getMutableCopy();

        for (int i = 0; i < 20; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        long a = tm.save();
        tm = new BTree(log, new BTreeBalancePolicy(4), a, false, 1).getMutableCopy();

        dump(getTreeMutable());

        for (int i = -5; i < 8; i++) {
            tm.delete(key(i));
            dump(getTreeMutable());
        }
        assertEquals(12, tm.getSize());

        for (int i = 0; i < 20; i++) {
            tm.delete(key(i));
            dump(getTreeMutable());
        }
        assertEquals(0, tm.getSize());

    }

    @Test
    public void testDeleteNoDuplicatesBottomPage() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(16), false, 1).getMutableCopy();

        List<INode> res = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            final INode ln = kv(i, "v" + i);
            getTreeMutable().put(ln);
            res.add(ln);
        }
        dump(getTreeMutable());

        long a = tm.save();
        tm = new BTree(log, new BTreeBalancePolicy(16), a, false, 1).getMutableCopy();

        for (int i = 0; i < 64; i++) {
            tm.delete(key(i));
            res.remove(0);

            dump(getTreeMutable());

            assertMatchesIterator(tm, res);
        }
    }

    @Test
    public void testDeleteDuplicates() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv(1, "11"));
        getTreeMutable().put(kv(1, "12"));

        assertEquals(false, tm.delete(key(2)));
        assertEquals(true, tm.delete(key(1)));

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));

        long a = tm.save();

        reopen();

        t = new BTree(log, a, false, 1);

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));
    }

    @Test
    public void testDeleteDuplicates2() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv(1, "11"));
        getTreeMutable().put(kv(1, "12"));

        assertEquals(true, getTreeMutable().delete(key(1), value("11")));
        assertEquals(true, getTreeMutable().delete(key(1), value("12")));

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));

        long a = tm.save();

        reopen();

        t = new BTree(log, a, false, 1);

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));
    }

    @Test
    public void testMergeWithDefaultPolicy() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(7), true, 1).getMutableCopy();

        for (int i = 0; i < 8; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(BP(6), BP(2)));

        tm.delete(key(7));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(BP(6), BP(1)));

        tm.delete(key(6));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), BP(6));

        getTreeMutable().put(kv(6, "v6"));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), BP(7));

        getTreeMutable().put(kv(7, "v7"));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(BP(6), BP(2)));

        tm.delete(key(1));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(BP(5), BP(2)));

        tm.delete(key(2));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), BP(6));

        tm.delete(key(3));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), BP(5));

        tm.delete(key(4));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), BP(4));
    }

    @Test
    public void testRemoveFirst() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), true, 1).getMutableCopy();

        for (int i = 0; i < 14; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3), BP(3)),
                IP(BP(3), BP(2))));

        // remove first
        assertTrue(tm.delete(key(0)));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(2), BP(3), BP(3)),
                IP(BP(3), BP(2))));
    }

    @Test
    public void testMergeDuplicatesWithDefaultPolicyOnRemoveLast() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), true, 1).getMutableCopy();

        for (int i = 0; i < 14; i++) {
            getTreeMutable().put(kv(i, "v" + i));
            dump(getTreeMutable());
        }

        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3), BP(3)),
                IP(BP(3), BP(2))));

        // remove last
        assertTrue(tm.delete(key(13)));
        assertTrue(tm.delete(key(12)));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3), BP(3)),
                IP(BP(3))));

        getTreeMutable().put(kv(14, "v14"));
        getTreeMutable().put(kv(15, "v15"));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3), BP(3)),
                IP(BP(3), BP(2))));
    }

    @Test
    public void testMergeDuplicatesWithDefaultPolicyOnRemoveMiddle() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), true, 1).getMutableCopy();

        for (int i = 0; i < 14; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3), BP(3)),
                IP(BP(3), BP(2))));

        assertTrue(tm.delete(key(1)));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(2), BP(3), BP(3)),
                IP(BP(3), BP(2))));

        assertTrue(tm.delete(key(4)));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(2), BP(2), BP(3)),
                IP(BP(3), BP(2))));

        assertTrue(tm.delete(key(5)));
        dump(getTreeMutable());
        assertMatches(getTreeMutable(), IP(
                IP(BP(3), BP(3)),
                IP(BP(3), BP(2))));
    }

    @Test
    public void testGetNextEmpty() {
        tm = new BTree(log, new BTreeEmpty(log, true, 1).getMutableCopy().save(), true, 1).getMutableCopy();
        assertTrue(tm.isEmpty());
        assertEquals(0, tm.getSize());
        assertFalse(tm.openCursor().getNext());
    }

    @Test
    public void testBulkDelete() {
        prepareData(5000);
        int i = 0;

        while (!tm.isEmpty() && i <= 5000) {
            //c.getNext();
            //c.deleteCurrent();

            if (i == 103) dump(getTreeMutable());

            tm.delete(key(i));

            assertEquals(5000 - i - 1, tm.getSize());
/*
            if (i % 100 == 0) {
                long a = tm.save();
                tm = (BTreeMutable) new BTree(log, a, true).getMutableCopy();
                //c = tm.openCursor();
            }
*/

            i++;
        }

        long a = tm.save();
        tm = new BTree(log, a, true, 1).getMutableCopy();

        assertTrue(tm.isEmpty());
        assertEquals(0, tm.getSize());
        assertFalse(tm.openCursor().getNext());
    }

    private void prepareData(int size) {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        for (int i = 0; i < size; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        long a = tm.save();

        tm = new BTree(log, a, true, 1).getMutableCopy();
    }

}
