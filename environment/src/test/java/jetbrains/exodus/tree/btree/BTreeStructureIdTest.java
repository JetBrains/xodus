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

import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.LongIterator;
import org.junit.Assert;
import org.junit.Test;

public class BTreeStructureIdTest extends BTreeTestBase {

    @Test
    public void testStructureIdSaveEmpty() {
        BTreeMutable firstTree = new BTreeEmpty(log, false, 1).getMutableCopy();
        BTreeMutable secondTree = new BTreeEmpty(log, false, 2).getMutableCopy();
        long first = firstTree.save();
        long second = secondTree.save();
        assertContains(1, first);
        assertContains(2, second);
        assertStructureIdNotEqual(first, second);
    }

    @Test
    public void testStructureIdSave() {
        BTreeMutable firstTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 42).getMutableCopy();
        BTreeMutable secondTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 142).getMutableCopy();
        for (INode node : createLNs("v", 100)) {
            firstTree.put(node);
            secondTree.put(node);
        }
        checkTree(firstTree, 100).run();
        checkTree(secondTree, 100).run();
        long first = firstTree.save();
        long second = secondTree.save();
        assertContains(42, first);
        assertContains(142, second);
        assertStructureIdNotEqual(first, second);
    }

    @Test
    public void testStructureIdDuplicatesSave() {
        BTreeMutable firstTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 42).getMutableCopy();
        BTreeMutable secondTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 142).getMutableCopy();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                firstTree.put(kv(i, duplicate("v", j)));
                secondTree.put(kv(i, duplicate("v", j)));
            }
        }
        long first = firstTree.save();
        long second = secondTree.save();
        assertContains(42, first);
        assertContains(142, second);
        assertStructureIdNotEqual(first, second);
    }

    @Test
    public void testStructureIdModify() {
        BTreeMutable firstTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 42).getMutableCopy();
        BTreeMutable secondTree = new BTreeEmpty(log, createTestSplittingPolicy(), false, 142).getMutableCopy();
        for (INode node : createLNs("v", 100)) {
            firstTree.put(node);
            secondTree.put(node);
        }
        checkTree(firstTree, 100).run();
        checkTree(secondTree, 100).run();
        long first = firstTree.save();
        long second = secondTree.save();
        assertContains(42, first);
        assertContains(142, second);
        assertStructureIdNotEqual(first, second);
        firstTree = new BTree(log, first, false, 42).getMutableCopy();
        secondTree = new BTree(log, second, false, 142).getMutableCopy();
        for (INode node : createLNs("vvv", 100)) {
            firstTree.put(node);
            secondTree.put(node);
        }
        checkTree(firstTree, "vvv", 100).run();
        checkTree(secondTree, "vvv", 100).run();
        first = firstTree.save();
        second = secondTree.save();
        assertContains(42, first);
        assertContains(142, second);
        assertStructureIdNotEqual(first, second);
    }

    public String duplicate(String single, int repeat) {
        StringBuilder builder = new StringBuilder(single);
        for (int i = 0; i < repeat; i++)
            builder.append(single);
        return builder.toString();
    }

    private void assertContains(long expectedId, long address) {
        ITree firstImTree = new BTree(log, address, false, 3);
        LongIterator it = firstImTree.addressIterator();
        while (it.hasNext()) Assert.assertEquals(expectedId, log.read(it.next()).getStructureId());
    }

    public static void assertStructureIdNotEqual(long firstAddress, long secondAddress) {
        ITree firstImTree = new BTree(log, firstAddress, false, 3);
        ITree secondImTree = new BTree(log, secondAddress, false, 3);
        LongIterator it = firstImTree.addressIterator();
        LongHashSet firstSet = new LongHashSet();
        LongHashSet secondSet = new LongHashSet();
        while (it.hasNext()) firstSet.add(log.read(it.next()).getStructureId());
        it = secondImTree.addressIterator();
        while (it.hasNext()) secondSet.add(log.read(it.next()).getStructureId());
        for (long firstStructureId : firstSet) {
            for (long seconfStrutureId : secondSet) {
                if (firstStructureId == seconfStrutureId)
                    throw new AssertionError("Structure ids are equal!");
            }
        }
    }
}
