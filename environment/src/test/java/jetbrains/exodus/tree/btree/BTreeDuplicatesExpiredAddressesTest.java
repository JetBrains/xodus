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
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.LongIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BTreeDuplicatesExpiredAddressesTest extends BTreeTestBase {

    @Test
    public void testNestedInternals() {
        tm = new BTreeEmpty(log, new BTreeBalancePolicy(3), false, 1).getMutableCopy();
        for (int i = 0; i < 10; i++) {
            getTreeMutable().put(kv(i, "value" + i));
        }

        t = new BTree(log, tm.save(), false, 1);

        checkAddressSet(getTree(), 18);
    }

    @Test
    public void testAdd() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        long address = tm.save();
        //Expired: none
        checkExpiredAddress(tm, 0);

        tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        //Expired: root
        checkExpiredAddress(tm, 1);
        getTreeMutable().put(kv(0, "value"));
        //Expired: still root
        checkExpiredAddress(tm, 1);
    }

    @Test
    public void testAddDup() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        //Expired: none
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        getTreeMutable().put(kv(0, "value2"));
        //Expired: root, LeafNode -> LeafNodeDupMutable
        checkExpiredAddress(tm, 2);
        tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        getTreeMutable().put(kv(0, "value3"));
        // Expired: root, dupTree
        checkExpiredAddress(tm, 2);
        tm.save();
    }

    @Test
    public void testDeleteDup() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        getTreeMutable().put(kv(0, "v0"));
        getTreeMutable().put(kv(0, "v1"));
        long address = tm.save();

        //Expired: none
        checkExpiredAddress(tm, 0);

        tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        //Expired: root
        checkExpiredAddress(tm, 1);
        Assert.assertFalse(getTreeMutable().delete(key(0), value("v2")));
        tm.save();
        //Expired: still root
        checkExpiredAddress(tm, 1);
    }

    @Test
    public void testDeleteAllDups() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        //Expired: none
        checkExpiredAddress(tm, 0);
        INode leafNode = kv(0, "value2");
        getTreeMutable().put(leafNode);
        //Expired: still none (changes in memory)
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        tm.delete(leafNode.getKey());
        //Expired: root, dupTree, value, value2
        checkExpiredAddress(tm, 4);
        tm.save();
    }

    @Test
    public void testDeleteSingleConvert() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        INode leafNode = kv(0, "value2");
        getTreeMutable().put(leafNode);
        //Expired: none
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        AddressIterator it = getTreeMutable().addressIterator();
        System.out.println("Before delete:");
        dumplLoggable(it);
        getTreeMutable().delete(leafNode.getKey(), leafNode.getValue());
        //Expired: root, dupTree, value, value2
        checkExpiredAddress(tm, 4);
        tm.save();
    }

    private void dumplLoggable(AddressIterator it) {
        while (it.hasNext()) {
            long address = it.next();
            System.out.println("Address: " + address + " type: " + log.read(address).getType());
        }
    }

    @Test
    public void testDeleteSingleNoConvert() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        getTreeMutable().put(kv(0, "value2"));
        INode leafNode = kv(0, "value3");
        getTreeMutable().put(leafNode);
        //Expired: none
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        getTreeMutable().delete(leafNode.getKey(), leafNode.getValue());
        //Expired: root, dupTree, value3
        checkExpiredAddress(tm, 3);
        tm.save();
    }

    @Test
    public void testBulkAdd() throws IOException {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        long address = tm.save();

        checkExpiredAddress(tm, 0);

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        for (int i = 0; i < 1000; i++) {
            getTreeMutable().put(kv(i, "value"));
            getTreeMutable().put(kv(i, "value2"));
            getTreeMutable().put(kv(i, "value3"));
            getTreeMutable().put(kv(i, "value4"));
            getTreeMutable().put(kv(i, "value5"));
        }
        checkExpiredAddress(tm, 1);
        tm.save();
    }

    @Test
    public void testBulkDeleteByKey() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();
        ByteIterable[] keys = new ByteIterable[1000];
        for (int i = 0; i < 1000; i++) {
            INode node = kv(i, "value");
            getTreeMutable().put(node);
            getTreeMutable().put(kv(i, "value2"));
            getTreeMutable().put(kv(i, "value3"));
            getTreeMutable().put(kv(i, "value4"));
            getTreeMutable().put(kv(i, "value5"));
            getTreeMutable().put(kv(i, "value6"));
            getTreeMutable().put(kv(i, "value7"));
            getTreeMutable().put(kv(i, "value8"));
            keys[i] = node.getKey();
        }
        //Expired: none
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        long addresses = countNodes(getTreeMutable());
        for (int i = 0; i < 1000; i++) {
            tm.delete(keys[i]);
        }
        checkExpiredAddress(tm, addresses);
        tm.save();

    }

    @Test
    public void testBulkDeleteByKV() throws IOException {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), true, 1).getMutableCopy();
        List<INode> leaves = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            INode[] nodes = new INode[5];
            nodes[0] = kv(i, "value");
            nodes[1] = kv(i, "value2");
            nodes[2] = kv(i, "value3");
            nodes[3] = kv(i, "value4");
            nodes[4] = kv(i, "value5");
            for (INode iLeafNode : nodes) {
                getTreeMutable().put(iLeafNode);
                leaves.add(iLeafNode);
            }
        }
        //Expired: none
        checkExpiredAddress(tm, 0);
        long address = tm.save();

        t = new BTree(log, address, true, 1);
        tm = getTree().getMutableCopy();
        long addresses = countNodes(getTreeMutable());
        for (INode leafNode : leaves) {
            getTreeMutable().delete(leafNode.getKey(), leafNode.getValue());
        }
        checkExpiredAddress(tm, addresses);
        tm.save();

    }

    public void checkExpiredAddress(ITreeMutable tree, long expectedAddresses) {
        Assert.assertEquals(expectedAddresses, tree.getExpiredLoggables().size());
    }

    protected long countNodes(BTreeMutable tree) {
        return countNodes(tree.getRoot());
    }

    private long countNodes(BasePage page) {
        if (page.isBottom()) {
            long result = 1;
            for (int i = 0; i < page.getSize(); i++) {
                long r = 1;
                BaseLeafNode node = page.getKey(i);
                if (node.isDup()) {
                    LongIterator it = node.addressIterator();
                    while (it.hasNext()) {
                        it.next();
                        r += 1;
                    }
                } else {
                    r += 1;
                }
                result += r;
            }
            return result;
        }
        long result = 1;
        for (int i = 0; i < page.getSize(); i++) {
            result += countNodes(page.getChild(i));
        }
        return result;
    }


}
