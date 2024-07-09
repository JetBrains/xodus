/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.ITreeMutable;
import org.junit.Assert;
import org.junit.Test;

public class BTreeNoDuplicatesExpiredAddressesTest extends BTreeTestBase {


    @Test
    public void testAdd() {
        tm = new BTreeEmpty(log, false, 1, Integer.MAX_VALUE).getMutableCopy();
        long address = saveTree();
        checkExpiredAddress(tm, 0, "Expired: none");

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        checkExpiredAddress(tm, 1, "Expired: root");
        saveTree();
    }

    @Test
    public void testModify() {
        tm = new BTreeEmpty(log, false, 1, Integer.MAX_VALUE).getMutableCopy();
        getTreeMutable().put(kv(0, "value"));
        getTreeMutable().put(kv(1, "value2"));
        checkExpiredAddress(tm, 0, "Expired: none");
        long address = saveTree();

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        getTreeMutable().put(kv(0, "value2"));
        checkExpiredAddress(tm, 2, "Expired: root, value");
        saveTree();
    }

    @Test
    public void testDelete() {
        tm = new BTreeEmpty(log, false, 1, Integer.MAX_VALUE).getMutableCopy();
        INode leafNode = kv(0, "value");
        getTreeMutable().put(leafNode);
        checkExpiredAddress(tm, 0, "Expired: none");
        long address = saveTree();

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        tm.delete(leafNode.getKey());
        checkExpiredAddress(tm, 2, "Expired: root, value");
        saveTree();
    }

    @Test
    public void testBulkAdd() {
        tm = new BTreeEmpty(log, false, 1, Integer.MAX_VALUE).getMutableCopy();
        long address = saveTree();

        checkExpiredAddress(tm, 0, "Expired: none");

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        for (int i = 0; i < 1000; i++) {
            getTreeMutable().put(kv(i, "value"));
        }
        checkExpiredAddress(tm, 1, "Expired: root");
        saveTree();
    }

    @Test
    public void testBulkModify() {
        tm = new BTreeEmpty(log, createTestSplittingPolicy(), false, 1, Integer.MAX_VALUE).getMutableCopy();
        for (int i = 0; i < 1000; i++) {
            getTreeMutable().put(kv(i, "value"));
        }
        checkExpiredAddress(tm, 0, "Expired: none");
        long address = saveTree();

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        for (int i = 0; i < 1000; i++) {
            getTreeMutable().put(kv(i, "value2"));
        }

        checkExpiredAddress(tm, countNodes(getTreeMutable()), "Expired: root, 1000 values + internal nodes");
        saveTree();
    }

    @Test
    public void testBulkDelete() {
        tm =
                new BTreeEmpty(log, createTestSplittingPolicy(),
                        false, 1, Integer.MAX_VALUE).getMutableCopy();
        INode[] leafNode = new INode[1000];
        for (int i = 0; i < 1000; i++) {
            leafNode[i] = kv(i, "value");
            getTreeMutable().put(leafNode[i]);
        }
        checkExpiredAddress(tm, 0, "Expired: none");
        long address = saveTree();

        t = new BTree(log, address, false, 1, Integer.MAX_VALUE);
        tm = getTree().getMutableCopy();
        long addresses = countNodes(getTreeMutable());
        for (int i = 0; i < 1000; i++) {
            tm.delete(leafNode[i].getKey());
        }
        checkExpiredAddress(tm, addresses, "Expired: root, 1000 values + internal nodes");
        saveTree();
    }

    public void checkExpiredAddress(ITreeMutable tree, long expectedAddresses, String message) {
        Assert.assertEquals(message, expectedAddresses, tree.getExpiredLoggables().getSize());
    }

    protected long countNodes(BTreeMutable tree) {
        return countNodes(tree.getRoot());
    }

    private long countNodes(BasePage page) {
        if (page.isBottom()) {
            return page.getSize() + 1;
        }
        long result = 1;
        for (int i = 0; i < page.getSize(); i++) {
            result += countNodes(page.getChild(i));
        }
        return result;
    }


}
