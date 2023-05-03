/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.core.dataStructures.hash.LongHashSet
import jetbrains.exodus.tree.ITree
import org.junit.Assert
import org.junit.Test

class BTreeStructureIdTest : BTreeTestBase() {
    @Test
    fun testStructureIdSaveEmpty() {
        val firstTree = BTreeEmpty(log!!, false, 1).mutableCopy
        val secondTree = BTreeEmpty(log!!, false, 2).mutableCopy
        log!!.beginWrite()
        val first = firstTree.save()
        val second = secondTree.save()
        log!!.flush()
        log!!.endWrite()
        assertContains(1, first)
        assertContains(2, second)
        assertStructureIdNotEqual(first, second)
    }

    @Test
    fun testStructureIdSave() {
        val firstTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            42
        ).mutableCopy
        val secondTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            142
        ).mutableCopy
        for (node in createLNs("v", 100)) {
            firstTree.put(node)
            secondTree.put(node)
        }
        checkTree(firstTree, 100).run()
        checkTree(secondTree, 100).run()
        log!!.beginWrite()
        val first = firstTree.save()
        val second = secondTree.save()
        log!!.flush()
        log!!.endWrite()
        assertContains(42, first)
        assertContains(142, second)
        assertStructureIdNotEqual(first, second)
    }

    @Test
    fun testStructureIdDuplicatesSave() {
        val firstTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            42
        ).mutableCopy
        val secondTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            142
        ).mutableCopy
        for (i in 0..99) {
            for (j in 0..9) {
                firstTree.put(kv(i, duplicate("v", j)))
                secondTree.put(kv(i, duplicate("v", j)))
            }
        }
        log!!.beginWrite()
        val first = firstTree.save()
        val second = secondTree.save()
        log!!.flush()
        log!!.endWrite()
        assertContains(42, first)
        assertContains(142, second)
        assertStructureIdNotEqual(first, second)
    }

    @Test
    fun testStructureIdModify() {
        var firstTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            42
        ).mutableCopy
        var secondTree = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            142
        ).mutableCopy
        for (node in createLNs("v", 100)) {
            firstTree.put(node)
            secondTree.put(node)
        }
        checkTree(firstTree, 100).run()
        checkTree(secondTree, 100).run()
        log!!.beginWrite()
        var first = firstTree.save()
        var second = secondTree.save()
        log!!.flush()
        log!!.endWrite()
        assertContains(42, first)
        assertContains(142, second)
        assertStructureIdNotEqual(first, second)
        firstTree = BTree(log!!, first, false, 42).mutableCopy
        secondTree = BTree(log!!, second, false, 142).mutableCopy
        for (node in createLNs("vvv", 100)) {
            firstTree.put(node)
            secondTree.put(node)
        }
        checkTree(firstTree, "vvv", 100).run()
        checkTree(secondTree, "vvv", 100).run()
        log!!.beginWrite()
        first = firstTree.save()
        second = secondTree.save()
        log!!.flush()
        log!!.endWrite()
        assertContains(42, first)
        assertContains(142, second)
        assertStructureIdNotEqual(first, second)
    }

    private fun duplicate(@Suppress("SameParameterValue") single: String?, repeat: Int): String {
        val builder = StringBuilder(single)
        for (i in 0 until repeat) builder.append(single)
        return builder.toString()
    }

    private fun assertContains(expectedId: Long, address: Long) {
        val firstImTree: ITree = BTree(log!!, address, false, 3)
        val it = firstImTree.addressIterator()
        while (it.hasNext()) Assert.assertEquals(
            expectedId,
            log!!.read(it.next()).structureId.toLong()
        )
    }

    companion object {
        fun assertStructureIdNotEqual(firstAddress: Long, secondAddress: Long) {
            val firstImTree: ITree = BTree(log!!, firstAddress, false, 3)
            val secondImTree: ITree = BTree(log!!, secondAddress, false, 3)
            var it = firstImTree.addressIterator()
            val firstSet = LongHashSet()
            val secondSet = LongHashSet()
            while (it.hasNext()) firstSet.add(log!!.read(it.next()).structureId.toLong())
            it = secondImTree.addressIterator()
            while (it.hasNext()) secondSet.add(log!!.read(it.next()).structureId.toLong())
            for (firstStructureId in firstSet) {
                for (seconfStrutureId in secondSet) {
                    if (firstStructureId == seconfStrutureId) throw AssertionError("Structure ids are equal!")
                }
            }
        }
    }
}
