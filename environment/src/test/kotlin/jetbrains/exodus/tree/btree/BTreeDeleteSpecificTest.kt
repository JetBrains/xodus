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
@file:Suppress("SameParameterValue")

package jetbrains.exodus.tree.btree


import jetbrains.exodus.tree.INode
import org.junit.Assert
import org.junit.Test

class BTreeDeleteSpecificTest : BTreeTestBase() {
    private var policy = BTreeBalancePolicy(10)
    private fun refresh(): BTreeMutable? {
        val a = saveTree()
        tree = BTree(log!!, policy, a, false, 1)
        treeMutable = tree!!.mutableCopy
        return treeMutable!!
    }

    @Test
    fun testDeleteKeys() {
        treeMutable = BTreeEmpty(log!!, policy, false, 1).mutableCopy
        for (i in 0..124) {
            treeMutable!!.put(kv(i, "k$i"))
        }
        val key = treeMutable!!.root.getKey(1).key
        refresh()
        treeMutable!!.delete(key)
        Assert.assertEquals(
            0,
            treeMutable!!.root.getKey(1).compareKeyTo(treeMutable!!.root.getChild(1).minKey.key).toLong()
        )
    }

    @Test
    fun testDeleteNoDuplicatesDeleteFirstTwoPages() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), false, 1).mutableCopy
        for (i in 0..19) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        val a = saveTree()
        treeMutable = BTree(log!!, BTreeBalancePolicy(4), a, false, 1).mutableCopy
        dump(treeMutable!!)
        for (i in 0..7) {
            treeMutable!!.delete(key(i))
            dump(treeMutable!!)
        }
        Assert.assertEquals(12, treeMutable!!.size)
    }

    @Test
    fun testDeleteNotExistingKeys() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), false, 1).mutableCopy
        for (i in 0..19) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        val a = saveTree()
        treeMutable = BTree(log!!, BTreeBalancePolicy(4), a, false, 1).mutableCopy
        dump(treeMutable!!)
        for (i in -5..7) {
            treeMutable!!.delete(key(i))
            dump(treeMutable!!)
        }
        Assert.assertEquals(12, treeMutable!!.size)
        for (i in 0..19) {
            treeMutable!!.delete(key(i))
            dump(treeMutable!!)
        }
        Assert.assertEquals(0, treeMutable!!.size)
    }

    @Test
    fun testDeleteNoDuplicatesBottomPage() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(16), false, 1).mutableCopy
        val res: MutableList<INode> = ArrayList()
        for (i in 0..63) {
            val ln: INode = kv(i, "v$i")
            treeMutable!!.put(ln)
            res.add(ln)
        }
        dump(treeMutable!!)
        val a = saveTree()
        treeMutable = BTree(log!!, BTreeBalancePolicy(16), a, false, 1).mutableCopy
        for (i in 0..63) {
            treeMutable!!.delete(key(i))
            res.removeAt(0)
            dump(treeMutable!!)
            assertMatchesIterator(treeMutable!!, res)
        }
    }

    @Test
    fun testDeleteDuplicates() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv(1, "11"))
        treeMutable!!.put(kv(1, "12"))
        Assert.assertFalse(treeMutable!!.delete(key(2)))
        Assert.assertTrue(treeMutable!!.delete(key(1)))
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertNull(treeMutable!![key(1)])
        val a = saveTree()
        reopen()
        tree = BTree(log!!, a, false, 1)
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertNull(treeMutable!![key(1)])
    }

    @Test
    fun testDeleteDuplicates2() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv(1, "11"))
        treeMutable!!.put(kv(1, "12"))
        Assert.assertTrue(treeMutable!!.delete(key(1), value("11")))
        Assert.assertTrue(treeMutable!!.delete(key(1), value("12")))
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertNull(treeMutable!![key(1)])
        val a = saveTree()
        reopen()
        tree = BTree(log!!, a, false, 1)
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertNull(treeMutable!![key(1)])
    }

    @Test
    fun testMergeWithDefaultPolicy() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(7), true, 1).mutableCopy
        for (i in 0..7) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        dump(treeMutable!!)
        assertMatches(treeMutable!!, ip(bp(6), bp(2)))
        treeMutable!!.delete(key(7))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, ip(bp(6), bp(1)))
        treeMutable!!.delete(key(6))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, bp(6))
        treeMutable!!.put(kv(6, "v6"))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, bp(7))
        treeMutable!!.put(kv(7, "v7"))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, ip(bp(6), bp(2)))
        treeMutable!!.delete(key(1))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, ip(bp(5), bp(2)))
        treeMutable!!.delete(key(2))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, bp(6))
        treeMutable!!.delete(key(3))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, bp(5))
        treeMutable!!.delete(key(4))
        dump(treeMutable!!)
        assertMatches(treeMutable!!, bp(4))
    }

    @Test
    fun testRemoveFirst() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), true, 1).mutableCopy
        for (i in 0..13) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )

        // remove first
        Assert.assertTrue(treeMutable!!.delete(key(0)))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(2), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )
    }

    @Test
    fun testMergeDuplicatesWithDefaultPolicyOnRemoveLast() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), true, 1).mutableCopy
        for (i in 0..13) {
            treeMutable!!.put(kv(i, "v$i"))
            dump(treeMutable!!)
        }
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )

        // remove last
        Assert.assertTrue(treeMutable!!.delete(key(13)))
        Assert.assertTrue(treeMutable!!.delete(key(12)))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3), bp(3)),
                ip(bp(3))
            )
        )
        treeMutable!!.put(kv(14, "v14"))
        treeMutable!!.put(kv(15, "v15"))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )
    }

    @Test
    fun testMergeDuplicatesWithDefaultPolicyOnRemoveMiddle() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), true, 1).mutableCopy
        for (i in 0..13) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )
        Assert.assertTrue(treeMutable!!.delete(key(1)))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(2), bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )
        Assert.assertTrue(treeMutable!!.delete(key(4)))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(2), bp(2), bp(3)),
                ip(bp(3), bp(2))
            )
        )
        Assert.assertTrue(treeMutable!!.delete(key(5)))
        dump(treeMutable!!)
        assertMatches(
            treeMutable!!, ip(
                ip(bp(3), bp(3)),
                ip(bp(3), bp(2))
            )
        )
    }

    @Test
    fun testGetNextEmpty() {
        val copy = BTreeEmpty(log!!, true, 1).mutableCopy
        log!!.beginWrite()
        val address = copy.save()
        log!!.flush()
        log!!.endWrite()
        treeMutable = BTree(log!!, address, true, 1).mutableCopy
        Assert.assertTrue(treeMutable!!.isEmpty)
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertFalse(treeMutable!!.openCursor().next)
    }

    @Test
    fun testBulkDelete() {
        prepareData(5000)
        var i = 0
        while (!treeMutable!!.isEmpty && i <= 5000) {
            if (i == 103) dump(treeMutable!!)
            treeMutable!!.delete(key(i))
            Assert.assertEquals((5000 - i - 1).toLong(), treeMutable!!.size)
            i++
        }
        val a = saveTree()
        treeMutable = BTree(log!!, a, true, 1).mutableCopy
        Assert.assertTrue(treeMutable!!.isEmpty)
        Assert.assertEquals(0, treeMutable!!.size)
        Assert.assertFalse(treeMutable!!.openCursor().next)
    }

    private fun prepareData(size: Int) {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        for (i in 0 until size) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        val a = saveTree()
        treeMutable = BTree(log!!, a, true, 1).mutableCopy
    }
}
