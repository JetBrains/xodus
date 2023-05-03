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

import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.INode
import org.junit.Assert
import org.junit.Test

class BTreeSimpleTest : BTreeTestBase() {
    @Test
    fun testEmptyTree() {
        checkEmptyTree(BTreeEmpty(log!!, false, 1).also { tree = it })
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        checkEmptyTree(treeMutable!!)
        val address = saveTree()
        reopen()
        tree = BTree(log!!, address, true, 1)
        checkEmptyTree(tree!!)
    }

    @Test
    fun testPutSaveGet() {
        // put
        treeMutable = BTreeEmpty(log!!, false, 1).mutableCopy
        val ln1: INode = kv("1", "vadim")
        treeMutable!!.put(ln1)
        Assert.assertEquals(1, treeMutable!!.size)
        Assert.assertTrue(treeMutable!!.hasKey(key("1")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("vadim")))
        Assert.assertTrue(treeMutable!!.root is BottomPageMutable)
        val bpm = treeMutable!!.root as BottomPageMutable
        Assert.assertEquals(1, bpm.size.toLong())
        Assert.assertEquals(ln1, bpm.keys[0])
        Assert.assertEquals(Loggable.NULL_ADDRESS, bpm.keysAddresses[0])
        assertMatchesIterator(treeMutable!!, ln1)
        valueEquals("vadim", treeMutable!![key("1")])

        // save
        val newRootAddress = saveTree()
        valueEquals("vadim", treeMutable!![key("1")])

        // get
        tree = BTree(log!!, newRootAddress, false, 1)
        val r: TreeAwareRunnable = object : TreeAwareRunnable() {
            override fun run() {
                Assert.assertEquals(1, tree!!.size)
                Assert.assertTrue(treeMutable!!.hasKey(key("1")))
                Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("vadim")))
                Assert.assertTrue(tree!!.root is BottomPage)
                val bp = tree!!.root as BottomPage
                Assert.assertEquals(1, bp.size.toLong())
                Assert.assertTrue(bp.getKeyAddress(0) != Loggable.NULL_ADDRESS)
                Assert.assertEquals(ln1, bp[key("1")])
                valueEquals("vadim", tree!![key("1")])
            }
        }
        r.run()

        // get after log reopen
        reopen()
        tree = BTree(log!!, newRootAddress, false, 1)
        r.run()
    }
}
