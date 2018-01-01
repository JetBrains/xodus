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

import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class BTreePutSpecificTest extends BTreeTestBase {

    @Test
    public void testPutDuplicateTreeWithDuplicates() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv("1", "1"));
        valueEquals("1", tm.get(key("1")));
        getTreeMutable().put(kv("1", "11"));
        valueEquals("1", tm.get(key("1")));

        assertEquals(true, tm.hasKey(key("1")));
        assertEquals(false, tm.hasKey(key("2")));
        assertEquals(true, tm.hasPair(key("1"), value("11")));
        assertEquals(true, tm.hasPair(key("1"), value("1")));
    }

    @Test
    public void testPutDuplicateTreeWithDuplicates2() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("1", "11"));
        getTreeMutable().put(kv("1", "11"));

        assertMatchesIterator(tm, true, kv("1", "1"), kv("1", "11"));
    }

    @Test
    public void testNextDup() {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("1", "2"));
        getTreeMutable().put(kv("1", "3"));
        getTreeMutable().put(kv("1", "4"));
        getTreeMutable().put(kv("1", "5"));

        final ITreeCursor cursor = tm.openCursor();

        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
    }

    @Test
    public void testNextDupWithSearch() {
        tm = createEmptyTreeForCursor(1).getMutableCopy();

        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("1", "2"));
        getTreeMutable().put(kv("1", "3"));
        getTreeMutable().put(kv("1", "4"));
        getTreeMutable().put(kv("1", "5"));

        final ITreeCursor cursor = tm.openCursor();

        assertNotNull(cursor.getSearchKey(key("1")));
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
        assertTrue(cursor.getNextDup());
    }

    @Test
    public void testPutNoOverwriteDuplicateTreeWithDuplicates2() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        assertEquals(true, getTreeMutable().add(kv("1", "1")));
        valueEquals("1", tm.get(key("1")));
        assertEquals(false, getTreeMutable().add(kv("1", "11")));
        valueEquals("1", tm.get(key("1")));

        assertEquals(true, tm.hasKey(key("1")));
        assertEquals(false, tm.hasKey(key("2")));
        assertEquals(false, tm.hasPair(key("1"), value("11")));
        assertEquals(true, tm.hasPair(key("1"), value("1")));
    }


    @Test
    public void testIterateOverDuplicates1() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("1", "11"));

        assertEquals(2, tm.getSize());
        assertMatchesIteratorAndExists(tm, kv("1", "1"), kv("1", "11"));

        long address = tm.save();
        t = new BTree(log, address, true, 2);

        assertEquals(2, tm.getSize());
        assertMatchesIteratorAndExists(t, kv("1", "1"), kv("1", "11"));
    }

    @Test
    public void testPutDuplicateTreeWithDuplicatesAfterSaveNoOrigDups() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), true, 1).getMutableCopy();

        // no duplicates
        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("2", "1"));

        long a = tm.save();
        reopen();

        tm = new BTree(log, new BTreeBalancePolicy(4), a, true, 2).getMutableCopy();

        getTreeMutable().put(kv("1", "11"));
        getTreeMutable().put(kv("2", "22"));

        a = tm.save();
        reopen();

        t = new BTree(log, new BTreeBalancePolicy(4), a, true, 2);

        assertEquals(true, t.hasKey(key("1")));
        assertEquals(true, t.hasKey(key("2")));
        assertEquals(false, t.hasKey(key("3")));

        assertEquals(true, t.hasPair(key("1"), value("11")));
        assertEquals(true, t.hasPair(key("2"), value("22")));
        assertEquals(false, t.hasPair(key("3"), value("1")));
    }

    @Test
    public void testPutDuplicateTreeWithDuplicatesAfterSaveOrigDupsPresent() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), true, 1).getMutableCopy();

        // dups present
        getTreeMutable().put(kv("1", "11"));
        getTreeMutable().put(kv("1", "12"));
        getTreeMutable().put(kv("2", "21"));

        long a = tm.save();
        reopen();

        tm = new BTree(log, new BTreeBalancePolicy(4), a, true, 1).getMutableCopy();

        getTreeMutable().put(kv("1", "13"));
        getTreeMutable().put(kv("2", "22"));

        a = tm.save();
        reopen();

        t = new BTree(log, new BTreeBalancePolicy(4), a, true, 1);

        assertMatchesIterator(t, kv("1", "11"), kv("1", "12"), kv("1", "13"), kv("2", "21"), kv("2", "22"));
    }

    @Test
    public void testIterateOverDuplicates2() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv("0", "0"));
        getTreeMutable().put(kv("1", "1"));
        getTreeMutable().put(kv("2", "2"));
        getTreeMutable().put(kv("1", "11"));

        assertEquals(4, tm.getSize());
        assertMatchesIteratorAndExists(tm, kv("0", "0"), kv("1", "1"), kv("1", "11"), kv("2", "2"));

        long address = tm.save();
        t = new BTree(log, address, true, 1);

        assertEquals(4, tm.getSize());
        assertMatchesIteratorAndExists(tm, kv("0", "0"), kv("1", "1"), kv("1", "11"), kv("2", "2"));
    }

    @Test
    public void testIterateOverDuplicates3() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();

        getTreeMutable().put(kv("0", "0"));
        getTreeMutable().put(kv("2", "2"));
        getTreeMutable().put(kv("1", "11"));
        getTreeMutable().put(kv("1", "1"));

        assertEquals(4, tm.getSize());
        assertMatchesIteratorAndExists(tm, kv("0", "0"), kv("1", "1"), kv("1", "11"), kv("2", "2"));

        long address = tm.save();
        t = new BTree(log, address, true, 1);

        assertEquals(4, tm.getSize());
        assertMatchesIteratorAndExists(tm, kv("0", "0"), kv("1", "1"), kv("1", "11"), kv("2", "2"));
    }

    @Test
    public void testSplitRight() throws IOException {
        tm = new BTreeEmpty(log,
                new BTreeBalancePolicy(5) {
                    @Override
                    public int getSplitPos(@NotNull BasePage page, int insertPosition) {
                        return page.getSize() - 1;
                    }
                }, true, 1
        ).getMutableCopy();

        for (int i = 0; i < 7; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        assertEquals(7, tm.getSize());

        TreeAwareRunnable r = new TreeAwareRunnable() {
            @Override
            public void run() {
                // root = internal -> bottom1, bottom2
                assertTrue(getTreeMutable().getRoot() instanceof InternalPageMutable);
                // root
                InternalPageMutable ipm = (InternalPageMutable) getTreeMutable().getRoot();
                assertEquals(2, ipm.size);
                assertEquals(key(0), ipm.keys[0].getKey());
                assertEquals(key(4), ipm.keys[1].getKey());

                // bottom1
                BottomPageMutable bp1 = (BottomPageMutable) ipm.children[0];
                assertEquals(4, bp1.size);
                for (int i = 0; i < 4; i++) {
                    assertEquals(key(i), bp1.keys[i].getKey());
                    valueEquals("v" + i, bp1.keys[i].getValue());
                }

                // bottom2
                BottomPageMutable bp2 = (BottomPageMutable) ipm.children[1];
                assertEquals(3, bp2.size);
                for (int i = 4; i < 7; i++) {
                    assertEquals(key(i), bp2.keys[i - 4].getKey());
                    valueEquals("v" + i, bp2.keys[i - 4].getValue());
                }
            }
        };

        TreeAwareRunnable r2 = checkTree(getTreeMutable(), 7);

        r.run();
        r2.run();

        //
        long rootAddress = tm.save();

        r.run();
        r2.run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        assertEquals(7, t.getSize());

        r2.setTree(getTree());
        r2.run();
    }

    @Test
    public void testSplitDefault() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(7), true, 1).getMutableCopy();

        for (int i = 0; i < 10; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        assertMatches(getTreeMutable(), IP(BP(6), BP(4)));
    }

    @Test
    public void testSplitAfterSave() throws IOException {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(4), false, 1).getMutableCopy();

        getTreeMutable().put(kv(1, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(3, "v3"));
        getTreeMutable().put(kv(4, "v4"));
        getTreeMutable().put(kv(5, "v5"));
        getTreeMutable().put(kv(6, "v6"));
        getTreeMutable().put(kv(7, "v7"));

        assertMatches(getTreeMutable(), IP(BP(3), BP(4)));

        long a = tm.save();

        tm = new BTree(log, new BTreeBalancePolicy(4), a, false, 1).getMutableCopy();

        getTreeMutable().put(kv(8, "v8"));

        assertMatches(getTreeMutable(), IP(BP(3), BP(3), BP(2)));
    }

}
