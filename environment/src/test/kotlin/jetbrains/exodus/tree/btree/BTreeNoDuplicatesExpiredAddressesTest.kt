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

import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.ITreeMutable
import org.junit.Assert
import org.junit.Test

open class BTreeNoDuplicatesExpiredAddressesTest : BTreeTestBase() {
    @Test
    fun testAdd() {
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        val address = saveTree()
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        treeMutable!!.put(kv(0, "value"))
        checkExpiredAddress(treeMutable!!, 1, "Expired: root")
        saveTree()
    }

    @Test
    fun testModify() {
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        treeMutable!!.put(kv(0, "value"))
        treeMutable!!.put(kv(1, "value2"))
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        val address = saveTree()
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        treeMutable!!.put(kv(0, "value2"))
        checkExpiredAddress(treeMutable!!, 2, "Expired: root, value")
        saveTree()
    }

    @Test
    fun testDelete() {
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        val leafNode: INode = kv(0, "value")
        treeMutable!!.put(leafNode)
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        val address = saveTree()
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        treeMutable!!.delete(leafNode.key)
        checkExpiredAddress(treeMutable!!, 2, "Expired: root, value")
        saveTree()
    }

    @Test
    fun testBulkAdd() {
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        val address = saveTree()
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        for (i in 0..999) {
            treeMutable!!.put(kv(i, "value"))
        }
        checkExpiredAddress(treeMutable!!, 1, "Expired: root")
        saveTree()
    }

    @Test
    fun testBulkModify() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).mutableCopy
        for (i in 0..999) {
            treeMutable!!.put(kv(i, "value"))
        }
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        val address = saveTree()
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        for (i in 0..999) {
            treeMutable!!.put(kv(i, "value2"))
        }
        checkExpiredAddress(treeMutable!!, countNodes(treeMutable!!), "Expired: root, 1000 values + internal nodes")
        saveTree()
    }

    @Test
    fun testBulkDelete() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).mutableCopy
        val leafNode = arrayOfNulls<INode>(1000)
        for (i in 0..999) {
            leafNode[i] = kv(i, "value")
            treeMutable!!.put(leafNode[i]!!)
        }
        checkExpiredAddress(treeMutable!!, 0, "Expired: none")
        val address = saveTree()
        tree = BTree(log!!, address, false, 1)
        treeMutable = tree!!.mutableCopy
        val addresses = countNodes(treeMutable!!)
        for (i in 0..999) {
            treeMutable!!.delete(leafNode[i]!!.key)
        }
        checkExpiredAddress(treeMutable!!, addresses, "Expired: root, 1000 values + internal nodes")
        saveTree()
    }

    private fun checkExpiredAddress(tree: ITreeMutable, expectedAddresses: Long, message: String?) {
        Assert.assertEquals(message, expectedAddresses, tree.expiredLoggables.size.toLong())
    }

    private fun countNodes(tree: BTreeMutable): Long {
        return countNodes(tree.root)
    }

    private fun countNodes(page: BasePage): Long {
        if (page.isBottom) {
            return (page.size + 1).toLong()
        }
        var result: Long = 1
        for (i in 0 until page.size) {
            result += countNodes(page.getChild(i))
        }
        return result
    }
}
