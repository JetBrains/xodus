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
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

@SuppressWarnings({"OverloadedMethodsWithSameNumberOfParameters", "CastToConcreteClass"})
public class BTreeReclaimTest extends BTreeTestBase {

    private long init(int p) {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1).getMutableCopy();

        for (int i = 0; i < p; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        return tm.save();
    }

    private long initDup(int p, int u) {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();

        for (int i = 0; i < p; i++) {
            for (int j = 0; j < u; j++) {
                getTreeMutable().put(kv(i, "v" + i + '#' + j));
            }
        }

        return tm.save();
    }

    @Test
    public void testLeafSimple() {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1).getMutableCopy();

        getTreeMutable().put(kv(0, "nothing"));

        long rootAddress = tm.save();
        t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1);

        final ByteIterable key = key(0);

        final ILeafNode savedLeaf = getTree().getRoot().get(key);
        Assert.assertNotNull(savedLeaf);
        final long savedLeafAddress = savedLeaf.getAddress();

        tm = getTree().getMutableCopy();

        getTreeMutable().put(kv(0, "anything"));

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeafAddress);
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            isAffected(log.read(address), key, (BTreeTraverser) addressIterator.getTraverser());
        }
    }

    @Test
    public void testLeafSimpleRemove() {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1).getMutableCopy();

        getTreeMutable().put(kv(0, "thing"));
        getTreeMutable().put(kv(1, "nothing"));
        getTreeMutable().put(kv(2, "something"));
        getTreeMutable().put(kv(3, "jumping"));
        getTreeMutable().put(kv(4, "dumping"));
        getTreeMutable().put(kv(5, "rambling"));
        getTreeMutable().put(kv(6, "plumbing"));

        long rootAddress = tm.save();
        t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1);

        final ByteIterable key = key(0);

        final ILeafNode savedLeaf = getTree().getRoot().get(key);
        Assert.assertNotNull(savedLeaf);
        final long savedLeafAddress = savedLeaf.getAddress();

        tm = getTree().getMutableCopy();

        tm.delete(key(1));
        rootAddress = tm.save();

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeafAddress);
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        System.out.println(tm.getExpiredLoggables().size());

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            isAffected(log.read(address), key, (BTreeTraverser) addressIterator.getTraverser());
        }
    }

    @Test
    public void testRootSimple() {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1).getMutableCopy();

        getTreeMutable().put(kv(0, "nothing"));
        getTreeMutable().put(kv(1, "something"));

        long rootAddress = tm.save();
        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        tm.delete(key(0));
        tm.delete(key(1));

        rootAddress = tm.save();
        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(rootAddress);
        Assert.assertTrue(tm.reclaim(iter.next(), iter)); // root should be reclaimed
    }

    @Test
    public void testLeafNoDup() {
        int p;
        long rootAddress = init(p = 1000);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        final ByteIterable key = key(0);

        final ILeafNode savedLeaf = getTree().getRoot().get(key);
        Assert.assertNotNull(savedLeaf);

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeaf.getAddress());
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            isAffected(log.read(address), key, (BTreeTraverser) addressIterator.getTraverser());
        }

        rootAddress = tm.save();

        checkTree(tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy(), p).run();
    }

    @Test
    public void testPageNoDup() {
        int p;
        long rootAddress = init(p = 1000);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy();

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(0);
        RandomAccessLoggable leaf = iter.next();
        RandomAccessLoggable next;
        while (true) {
            next = iter.next();
            if (next.getType() == BTreeBase.INTERNAL) {
                break;
            }
        }

        final BasePage page = getTree().loadPage(next.getAddress());
        Assert.assertTrue(tm.reclaim(leaf, iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            assertAffected(log.read(address), page, (BTreeTraverser) addressIterator.getTraverser());
        }

        rootAddress = tm.save();

        checkTree(tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, false, 1))).getMutableCopy(), p).run();
    }

    @Test
    public void testDupLeaf() {
        int p;
        int u;
        long rootAddress = initDup(p = 10, u = 100);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy();

        final ByteIterable key = key(0);

        final ILeafNode savedLeaf = getTree().getRoot().get(key);
        Assert.assertNotNull(savedLeaf);

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeaf.getAddress());
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            isAffected(log.read(address), key, (BTreeTraverser) addressIterator.getTraverser());
            final Loggable loggable = log.read(address);
            if (BTreeTraverser.isInDupMode(addressIterator) && isAffected(loggable, key, BTreeTraverser.getTraverserNoDup(addressIterator))) {
                assertAffected(getTreeMutable(), getTree(), loggable, key);
            }
        }

        rootAddress = tm.save();

        checkTree(tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy(), p, u).run();
    }

    @Test
    public void testSkipDupTree() {
        /* int p;
        int u; */
        long rootAddress = initDup(/* p = */10, /* u = */100);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy();

        final ByteIterable key = key(5);

        final ILeafNode savedLeaf = getTree().getRoot().get(key);
        Assert.assertNotNull(savedLeaf);
        final long oldAddress = savedLeaf.getAddress();

        tm.delete(key);
        tm.delete(key(6));
        getTreeMutable().put(key(6), value("v6#0"));
        rootAddress = tm.save();

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy();

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(oldAddress);
        /* Loggable loggable = iter.next();
        while (loggable.getType() != BTree.DUP_INTERNAL) {
            loggable = iter.next();
        }
        Assert.assertTrue(loggable.getAddress() < savedLeaf.getAddress()); // some dup tree stored before our leaf */
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            isAffected(log.read(address), key, (BTreeTraverser) addressIterator.getTraverser());
            RandomAccessLoggable loggable = log.read(address);
            if (BTreeTraverser.isInDupMode(addressIterator) && isAffected(loggable, key, BTreeTraverser.getTraverserNoDup(addressIterator))) {
                assertAffected(getTreeMutable(), getTree(), loggable, key);
            }
        }

        /* rootAddress = tm.save();
        checkTree(tm = (t = new BTree(log, rootAddress, getTreeMutable().getBalancePolicy(), true, 1)).getMutableCopy(), p, u).run(); */
    }

    @Test
    public void testLeafDup() {
        int p;
        int u;
        long rootAddress = initDup(p = 10, u = 100);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy();

        final ByteIterable key = key(0);
        final ByteIterable value = value("v0#10");

        final LeafNodeDup dupLeaf = (LeafNodeDup) getTree().getRoot().get(key);
        Assert.assertNotNull(dupLeaf);

        final BTreeDup dt = dupLeaf.tree;
        final ILeafNode savedLeaf = dt.getRoot().get(value);

        Assert.assertNotNull(savedLeaf);

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeaf.getAddress());
        Assert.assertTrue(tm.reclaim(iter.next(), iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        final LeafNodeDupMutable dupLeafMutable = (LeafNodeDupMutable) getTreeMutable().getRoot().get(key);

        Assert.assertNotNull(dupLeafMutable);
        final BTreeDupMutable dtm = dupLeafMutable.tree;

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            final Loggable loggable = log.read(address);
            if (BTreeTraverser.isInDupMode(addressIterator) && isAffected(loggable, key, BTreeTraverser.getTraverserNoDup(addressIterator))) {
                assertAffected(dtm, dt, loggable, value, (BTreeTraverser) addressIterator.getTraverser());
            }
        }

        checkTree(tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy(), p, u).run();
    }

    @Test
    public void testPageDup() {
        int p;
        int u;
        long rootAddress = initDup(p = 10, u = 100);

        tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy();

        final ByteIterable key = key(0);
        final ByteIterable value = value("v0#10");

        final LeafNodeDup dupLeaf = (LeafNodeDup) getTree().getRoot().get(key);
        Assert.assertNotNull(dupLeaf);

        final BTreeBase dt = dupLeaf.tree;
        final ILeafNode savedLeaf = dt.getRoot().get(value);

        Assert.assertNotNull(savedLeaf);

        final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(savedLeaf.getAddress());
        RandomAccessLoggable loggable = iter.next();
        while (loggable.getType() != BTreeBase.DUP_INTERNAL) {
            loggable = iter.next();
        }
        final BasePage page = dt.loadPage(loggable.getAddress());
        Assert.assertTrue(tm.reclaim(loggable, iter));

        final AddressIterator addressIterator = getTreeAddresses(getTree());

        final LeafNodeDupMutable dupLeafMutable = (LeafNodeDupMutable) getTreeMutable().getRoot().get(key);

        Assert.assertNotNull(dupLeafMutable);
        final BTreeMutable dtm = dupLeafMutable.tree;

        while (addressIterator.hasNext()) {
            final long address = addressIterator.next();
            loggable = log.read(address);
            if (BTreeTraverser.isInDupMode(addressIterator) && isAffected(loggable, value, BTreeTraverser.getTraverserNoDup(addressIterator))) {
                assertAffected(dtm, dt, loggable, page, (BTreeTraverser) addressIterator.getTraverser());
            }
        }

        checkTree(tm = ((BTree) (t = new BTree(log, getTreeMutable().getBalancePolicy(), rootAddress, true, 1))).getMutableCopy(), p, u).run();
    }

    private boolean isAffected(@NotNull final Loggable loggable, @NotNull final ByteIterable key, @NotNull final BTreeTraverser path) {
        final int type = loggable.getType();
        switch (type) {
            case BTreeBase.BOTTOM_ROOT:
            case BTreeBase.INTERNAL_ROOT:
                Assert.assertEquals(t.getRootAddress(), loggable.getAddress());
                assertAffected(getTreeMutable(), getTree().getRoot(), key, path);
                return true;
            case BTreeBase.BOTTOM:
            case BTreeBase.INTERNAL:
                assertAffected(getTreeMutable(), getTree().loadPage(loggable.getAddress()), key, path);
                return true;
            case BTreeBase.LEAF:
            case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
            case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                return assertAffected(getTreeMutable(), getTree().loadLeaf(loggable.getAddress()), key);
            default:
                return false;
        }
    }

    private void assertAffected(@NotNull final Loggable loggable, @NotNull final BasePage page, @NotNull final BTreeTraverser path) {
        final int type = loggable.getType();
        switch (type) {
            case BTreeBase.BOTTOM_ROOT:
            case BTreeBase.INTERNAL_ROOT:
                Assert.assertEquals(t.getRootAddress(), loggable.getAddress());
                assertAffected(getTreeMutable(), getTree().getRoot(), page, path);
                return;
            case BTreeBase.BOTTOM:
            case BTreeBase.INTERNAL:
                assertAffected(getTreeMutable(), getTree().loadPage(loggable.getAddress()), page, path);
                return;
            default:
                // do nothing
        }
    }

    private static void assertAffected(@NotNull BTreeMutable dtm, @NotNull BTreeBase dt, @NotNull final Loggable loggable,
                                       @NotNull final ByteIterable value, @NotNull final BTreeTraverser path) {
        final int type = loggable.getType();
        switch (type) {
            case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
            case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                assertAffected(dtm, dt.getRoot(), value, path);
                return;
            case BTreeBase.DUP_BOTTOM:
            case BTreeBase.DUP_INTERNAL:
                assertAffected(dtm, dt.loadPage(loggable.getAddress()), value, path);
                return;
            case BTreeBase.DUP_LEAF:
                assertAffected(dtm, dt.loadLeaf(loggable.getAddress()), value);
                return;
            default:
                throw new IllegalStateException("loggable type unexpected in duplicates tree");
        }
    }

    private static void assertAffected(@NotNull BTreeMutable dtm, @NotNull BTreeBase dt,
                                       @NotNull final Loggable loggable, @NotNull final ByteIterable key) {
        final int type = loggable.getType();
        switch (type) {
            case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
            case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                assertAffected(dtm, dt.loadLeaf(loggable.getAddress()), key);
                return;
            case BTreeBase.DUP_BOTTOM:
            case BTreeBase.DUP_INTERNAL:
            case BTreeBase.DUP_LEAF:
                return;
            default:
                throw new IllegalStateException("loggable type unexpected in duplicates tree");
        }
    }

    private static void assertAffected(@NotNull BTreeMutable dtm, @NotNull BTreeBase dt, @NotNull final RandomAccessLoggable loggable,
                                       @NotNull final BasePage page, @NotNull final BTreeTraverser path) {
        final int type = loggable.getType();
        switch (type) {
            /* case BTree.BOTTOM_ROOT:
            case BTree.INTERNAL_ROOT:
                Assert.assertEquals(dt.getRootAddress(), loggable.getAddress());
                assertAffected(dtm, dt.getRoot(), page, path);
                return;
            case BTree.BOTTOM:
            case BTree.INTERNAL:
                assertAffected(dtm, dt.loadPage(loggable), page, path);
                return; */
            case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
            case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                assertAffected(dtm, dt.getRoot(), page, path);
                return;
            case BTreeBase.DUP_BOTTOM:
            case BTreeBase.DUP_INTERNAL:
                assertAffected(dtm, dt.loadPage(loggable.getAddress()), page, path);
                return;
            default:
                // do nothing
        }
    }

    private static boolean assertAffected(@NotNull BTreeMutable treeMutable, @NotNull final INode leaf, @NotNull final ByteIterable key) {
        if (isEqual(leaf.getKey(), key)) {
            final ILeafNode ln = treeMutable.getRoot().get(key);
            Assert.assertNotNull(ln);
            Assert.assertTrue(ln.isMutable());
            return true;
        }
        return false;
    }

    private static void assertAffected(@NotNull BTreeMutable treeMutable, @NotNull final BasePage page, @NotNull final ByteIterable key, @NotNull final BTreeTraverser path) {
        if (isBetween(page.getMinKey().getKey(), key, page.getMaxKey().getKey())) {
            Assert.assertTrue(getPage(treeMutable, path).isMutable());
        }
    }

    private static void assertAffected(@NotNull BTreeMutable treeMutable, @NotNull final BasePage sourcePage, @NotNull final BasePage page, @NotNull final BTreeTraverser path) {
        if (isBetween(sourcePage.getMinKey().getKey(), page.getMinKey().getKey(), sourcePage.getMaxKey().getKey())) {
            Assert.assertTrue(getPage(treeMutable, path).isMutable());
        }
    }

    private static boolean isBetween(@NotNull final ByteIterable min, @NotNull final ByteIterable current, @NotNull final ByteIterable max) {
        return ByteIterableUtil.compare(min, current) <= 0 && ByteIterableUtil.compare(current, max) <= 0;
    }

    private static boolean isEqual(@NotNull final ByteIterable found, @NotNull final ByteIterable current) {
        return ByteIterableUtil.compare(found, current) == 0;
    }

    @NotNull
    private static BasePage getPage(@NotNull BTreeMutable treeMutable, @NotNull final BTreeTraverser path) {
        BasePage result = treeMutable.getRoot();
        final BTreeTraverser.PageIterator itr = path.iterator();
        while (itr.hasNext()) {
            final BasePage node = (BasePage) itr.next();
            if (result.isBottom() ^ node.isBottom()) {
                Assert.fail("Tree structure not matched by type");
            }
            if (itr.hasNext()) {
                if (result.isBottom()) {
                    Assert.fail("Tree structure not matched by depth");
                } else {
                    result = result.getChild(Math.min(node.size - 1, itr.getPos()));
                }
            }
        }
        return result;
    }

    private static AddressIterator getTreeAddresses(@NotNull BTreeBase tree) {
        return tree.addressIterator();
    }
}
