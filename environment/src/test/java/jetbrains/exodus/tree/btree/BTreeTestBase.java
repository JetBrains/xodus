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

import jetbrains.exodus.tree.TreeBaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BTreeTestBase extends TreeBaseTest {

    @Override
    public BTreeBase getTree() {
        return (BTreeBase) super.getTree();
    }

    @Override
    public BTreeMutable getTreeMutable() {
        return (BTreeMutable) super.getTreeMutable();
    }

    protected static BTreeBalancePolicy createTestSplittingPolicy() {
        return new BTreeBalancePolicy(5);
    }

    public void assertMatches(BTreeBase t, IP IP) {
        IP.matches(t.getRoot());
    }

    public IP IP(IP... children) {
        return new IP(children);
    }

    public BP BP(int size) {
        return new BP(size);
    }

    protected BTreeEmpty createEmptyTreeForCursor(final int structureId) {
        return new BTreeEmpty(log, new BTreeBalancePolicy(4), true, structureId);
    }

    @Override
    protected BTreeMutable createMutableTree(final boolean hasDuplicates, final int structureId) {
        return doCreateMutableTree(hasDuplicates, structureId);
    }

    @Override
    protected BTree openTree(long address, boolean hasDuplicates) {
        return doOpenTree(address, hasDuplicates);
    }

    protected static BTreeMutable doCreateMutableTree(final boolean hasDuplicates, final int structureId) {
        return new BTreeEmpty(log, createTestSplittingPolicy(), hasDuplicates, structureId).getMutableCopy();
    }

    protected static BTree doOpenTree(long address, boolean hasDuplicates) {
        return new BTree(log, createTestSplittingPolicy(), address, hasDuplicates, 1);
    }

    public static class IP {

        IP[] children;

        IP(IP... children) {
            this.children = children;
        }

        void matches(BasePage p) {
            assertTrue(p instanceof InternalPage || p instanceof InternalPageMutable);
            assertEquals(children.length, p.getSize());
            final BasePage basePage = (BasePage) p;
            for (int i = 0; i < p.getSize(); i++) {
                children[i].matches(basePage.getChild(i));

                // check min key == first key of children
                assertEquals(basePage.getChild(i).getKey(0), p.getKey(i));
            }
        }

    }

    public static class BP extends IP {
        int size;

        BP(int size) {
            this.size = size;
        }

        @Override
        void matches(BasePage p) {
            assertTrue(p instanceof BottomPage || p instanceof BottomPageMutable);
            assertEquals(size, p.getSize());
        }
    }

}
