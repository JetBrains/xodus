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

import jetbrains.exodus.tree.INode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class BTreeTest extends BTreeTestBase {

    @Test
    public void testSplitRight2() throws IOException {
        int s = 1000;
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = 0; i < s; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), s).run();

        long rootAddress = tm.save();

        checkTree(getTreeMutable(), s).run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        checkTree(getTree(), s).run();
    }

    @Test
    public void testPutRightSplitRight() throws IOException {
        int s = 1000;
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = 0; i < s; i++) {
            getTreeMutable().putRight(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), s).run();

        long rootAddress = tm.save();

        checkTree(getTreeMutable(), s).run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        checkTree(getTree(), s).run();
    }

    @Test
    public void testSplitLeft() throws IOException {
        int s = 50;
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = s - 1; i >= 0; i--) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), s).run();

        long rootAddress = tm.save();

        checkTree(getTreeMutable(), s).run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        checkTree(getTree(), s).run();
    }

    @Test
    public void testSplitRandom() throws IOException {
        int s = 10000;
        List<INode> lns = createLNs(s);

        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        while (!lns.isEmpty()) {
            final int index = (int) (Math.random() * lns.size());
            INode ln = lns.get(index);
            getTreeMutable().put(ln);
            lns.remove(index);
        }

        checkTree(getTreeMutable(), s).run();

        long rootAddress = tm.save();

        checkTree(getTreeMutable(), s).run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);

        checkTree(getTree(), s).run();
    }

    @Test
    public void testPutOverwriteTreeWithoutDuplicates() throws IOException {
        // add existing key to tree that supports duplicates
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1).getMutableCopy();

        for (int i = 0; i < 100; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), 100).run();

        // put must add 100 new values
        for (int i = 0; i < 100; i++) {
            final INode ln = kv(i, "vv" + i);
            getTreeMutable().put(ln);
        }

        checkTree(getTreeMutable(), "vv", 100).run();

        long rootAddress = tm.save();

        checkTree(getTreeMutable(), "vv", 100).run();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        checkTree(getTreeMutable(), "vv", 100).run();
    }

    @Test
    public void testPutOverwriteTreeWithDuplicates() throws IOException {
        // add existing key to tree that supports duplicates
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = 0; i < 100; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), 100).run();

        // put must add 100 new values
        for (int i = 0; i < 100; i++) {
            final INode ln = kv(i, "vv" + i);
            getTreeMutable().put(ln);
        }

        // expected nodes
        List<INode> l = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            l.add(kv(i, "v" + i));
            l.add(kv(i, "vv" + i));
        }

        assertMatchesIterator(tm, l);

        long rootAddress = tm.save();

        assertMatchesIterator(tm, l);

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        assertMatchesIterator(tm, l);
    }

    @Test
    public void testPutAndDelete() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = 0; i < 100; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        long rootAddress = tm.save();
        tm = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1).getMutableCopy();

        checkTree(getTreeMutable(), 100).run();

        for (int i = 0; i < 100; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }
        Assert.assertEquals(1L, (long) tm.getExpiredLoggables().size());
        for (int i = 0; i < 100; i++) {
            final INode ln = kv(i, "v" + i);
            getTreeMutable().delete(ln.getKey(), ln.getValue());
        }

        Assert.assertEquals(0, tm.getSize());
        assertMatchesIterator(tm, Collections.<INode>emptyList());

        rootAddress = tm.save();

        reopen();

        t = new BTree(log, rootAddress, true, 1);
        assertMatchesIterator(tm, Collections.<INode>emptyList());
    }

    @Test
    public void testPutNoOverwriteTreeWithoutDuplicates() throws IOException {
        putNoOverwrite(false);
    }

    @Test
    public void testPutNoOverwriteTreeWithDuplicates() throws IOException {
        putNoOverwrite(true);
    }

    private void putNoOverwrite(boolean duplicates) {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), duplicates, 1).getMutableCopy();

        for (int i = 0; i < 100; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        checkTree(getTreeMutable(), 100).run();

        for (int i = 0; i < 100; i++) {
            final INode ln = kv(i, "vv" + i);
            assertFalse(getTreeMutable().add(ln));
        }
    }

    @Test
    public void testPutSortDuplicates() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        List<INode> expected = new ArrayList<>();
        expected.add(kv("1", "1"));
        expected.add(kv("2", "2"));
        expected.add(kv("3", "3"));
        expected.add(kv("5", "51"));
        expected.add(kv("5", "52"));
        expected.add(kv("5", "53"));
        expected.add(kv("5", "54"));
        expected.add(kv("5", "55"));
        expected.add(kv("5", "56"));
        expected.add(kv("5", "57"));
        expected.add(kv("7", "7"));

        for (INode ln : expected) {
            getTreeMutable().put(ln);
        }

        assertMatchesIterator(tm, expected);
    }

    @Test
    public void testPutRightSortDuplicates() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        List<INode> expected = new ArrayList<>();
        expected.add(kv("1", "1"));
        expected.add(kv("2", "2"));
        expected.add(kv("3", "3"));
        expected.add(kv("5", "51"));
        expected.add(kv("5", "52"));
        expected.add(kv("5", "53"));
        expected.add(kv("5", "54"));
        expected.add(kv("5", "55"));
        expected.add(kv("5", "56"));
        expected.add(kv("5", "57"));
        expected.add(kv("7", "7"));

        for (INode ln : expected) {
            getTreeMutable().putRight(ln);
        }

        assertMatchesIterator(tm, expected);
    }

    @Test
    public void testGetReturnsFirstSortedDuplicate() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        List<INode> l = new ArrayList<>();
        l.add(kv("1", "1"));
        l.add(kv("2", "2"));
        l.add(kv("3", "3"));
        l.add(kv("5", "51"));
        l.add(kv("5", "52"));
        l.add(kv("5", "53"));
        l.add(kv("5", "54"));
        l.add(kv("5", "55"));
        l.add(kv("5", "56"));
        l.add(kv("5", "57"));
        l.add(kv("7", "7"));

        for (INode ln : l) {
            getTreeMutable().add(ln);
        }

        valueEquals("51", tm.get(key("5")));
    }

}
