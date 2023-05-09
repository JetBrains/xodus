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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.ITreeMutable
import org.junit.Assert
import org.junit.Test

open class BTreeDuplicatesExpiredAddressesTest : BTreeTestBase() {
    @Test
    fun testNestedInternals() {
        treeMutable = BTreeEmpty(
            log!!, BTreeBalancePolicy(3), false,
            1
        ).getMutableCopy()
        for (i in 0..9) {
            treeMutable!!.put(kv(i, "value$i"))
        }
        tree = BTree(log!!, saveTree(), false, 1)
        checkAddressSet(tree!!, 18)
    }

    @Test
    fun testAdd() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        val address = saveTree()
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        //Expired: root
        checkExpiredAddress(treeMutable!!, 1)
        treeMutable!!.put(kv(0, "value"))
        //Expired: still root
        checkExpiredAddress(treeMutable!!, 1)
    }

    @Test
    fun testAddDup() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(kv(0, "value"))
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.put(kv(0, "value2"))
        //Expired: root, LeafNode -> LeafNodeDupMutable
        checkExpiredAddress(treeMutable!!, 2)
        saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.put(kv(0, "value3"))
        // Expired: root, dupTree
        checkExpiredAddress(treeMutable!!, 2)
        saveTree()
    }

    @Test
    fun testDeleteDup() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(kv(0, "v0"))
        treeMutable!!.put(kv(0, "v1"))
        val address = saveTree()

        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        //Expired: root
        checkExpiredAddress(treeMutable!!, 1)
        Assert.assertFalse(treeMutable!!.delete(key(0), value("v2")))
        saveTree()
        //Expired: still root
        checkExpiredAddress(treeMutable!!, 1)
    }

    @Test
    fun testDeleteAllDups() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(kv(0, "value"))
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val leafNode: INode = kv(0, "value2")
        treeMutable!!.put(leafNode)
        //Expired: still none (changes in memory)
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.delete(leafNode.getKey())
        //Expired: root, dupTree, value, value2
        checkExpiredAddress(treeMutable!!, 4)
        saveTree()
    }

    @Test
    fun testDeleteSingleConvert() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(kv(0, "value"))
        val leafNode: INode = kv(0, "value2")
        treeMutable!!.put(leafNode)
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        val it = treeMutable!!.addressIterator()
        println("Before delete:")
        dumplLoggable(it)
        treeMutable!!.delete(leafNode.getKey(), leafNode.getValue())
        //Expired: root, dupTree, value, value2
        checkExpiredAddress(treeMutable!!, 4)
        saveTree()
    }

    private fun dumplLoggable(it: AddressIterator) {
        while (it.hasNext()) {
            val address = it.next()
            println("Address: " + address + " type: " + log!!.read(address).getType())
        }
    }

    @Test
    fun testDeleteSingleNoConvert() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(kv(0, "value"))
        treeMutable!!.put(kv(0, "value2"))
        val leafNode: INode = kv(0, "value3")
        treeMutable!!.put(leafNode)
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.delete(leafNode.getKey(), leafNode.getValue())
        //Expired: root, dupTree, value3
        checkExpiredAddress(treeMutable!!, 3)
        saveTree()
    }

    @Test
    fun testBulkAdd() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        val address = saveTree()
        checkExpiredAddress(treeMutable!!, 0)
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        for (i in 0..999) {
            treeMutable!!.put(kv(i, "value"))
            treeMutable!!.put(kv(i, "value2"))
            treeMutable!!.put(kv(i, "value3"))
            treeMutable!!.put(kv(i, "value4"))
            treeMutable!!.put(kv(i, "value5"))
        }
        checkExpiredAddress(treeMutable!!, 1)
        saveTree()
    }

    @Test
    fun testBulkDeleteByKey() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).getMutableCopy()
        val keys = arrayOfNulls<ByteIterable>(1000)
        for (i in 0..999) {
            val node: INode = kv(i, "value")
            treeMutable!!.put(node)
            treeMutable!!.put(kv(i, "value2"))
            treeMutable!!.put(kv(i, "value3"))
            treeMutable!!.put(kv(i, "value4"))
            treeMutable!!.put(kv(i, "value5"))
            treeMutable!!.put(kv(i, "value6"))
            treeMutable!!.put(kv(i, "value7"))
            treeMutable!!.put(kv(i, "value8"))
            keys[i] = node.getKey()
        }
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        val addresses = countNodes(treeMutable)
        for (i in 0..999) {
            treeMutable!!.delete(keys[i]!!)
        }
        checkExpiredAddress(treeMutable!!, addresses)
        saveTree()
    }

    @Test
    fun testBulkDeleteByKV() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).getMutableCopy()
        val leaves: MutableList<INode?> = ArrayList()
        for (i in 0..999) {
            val nodes = arrayOfNulls<INode>(5)
            nodes[0] = kv(i, "value")
            nodes[1] = kv(i, "value2")
            nodes[2] = kv(i, "value3")
            nodes[3] = kv(i, "value4")
            nodes[4] = kv(i, "value5")
            for (iLeafNode in nodes) {
                treeMutable!!.put(iLeafNode!!)
                leaves.add(iLeafNode)
            }
        }
        //Expired: none
        checkExpiredAddress(treeMutable!!, 0)
        val address = saveTree()
        tree = BTree(log!!, address, true, 1)
        treeMutable = tree!!.getMutableCopy()
        val addresses = countNodes(treeMutable)
        for (leafNode in leaves) {
            treeMutable!!.delete(leafNode!!.getKey(), leafNode.getValue())
        }
        checkExpiredAddress(treeMutable!!, addresses)
        saveTree()
    }

    private fun checkExpiredAddress(tree: ITreeMutable, expectedAddresses: Long) {
        Assert.assertEquals(expectedAddresses, tree.getExpiredLoggables().size().toLong())
    }

    private fun countNodes(tree: BTreeMutable?): Long {
        return countNodes(tree!!.root)
    }

    private fun countNodes(page: BasePage): Long {
        if (page.isBottom()) {
            var result: Long = 1
            for (i in 0 until page.size) {
                var r: Long = 1
                val node = page.getKey(i)
                if (node.isDup()) {
                    val it = node.addressIterator()
                    while (it.hasNext()) {
                        it.next()
                        r += 1
                    }
                } else {
                    r += 1
                }
                result += r
            }
            return result
        }
        var result: Long = 1
        for (i in 0 until page.size) {
            result += countNodes(page.getChild(i))
        }
        return result
    }
}
