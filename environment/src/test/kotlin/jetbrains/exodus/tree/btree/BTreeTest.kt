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
import org.junit.Assert
import org.junit.Test

class BTreeTest : BTreeTestBase() {
    @Test
    fun testSplitRight2() {
        val s = 1000
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        for (i in 0 until s) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, s).run()
        val rootAddress = saveTree()
        checkTree(treeMutable!!, s).run()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        checkTree(tree!!, s).run()
    }

    @Test
    fun testPutRightSplitRight() {
        val s = 1000
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        for (i in 0 until s) {
            treeMutable!!.putRight(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, s).run()
        val rootAddress = saveTree()
        checkTree(treeMutable!!, s).run()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        checkTree(tree!!, s).run()
    }

    @Test
    fun testSplitLeft() {
        val s = 50
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        for (i in s - 1 downTo 0) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, s).run()
        val rootAddress = saveTree()
        checkTree(treeMutable!!, s).run()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        checkTree(tree!!, s).run()
    }

    @Test
    fun testSplitRandom() {
        val s = 10000
        val lns = createLNs(s)
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        while (lns.isNotEmpty()) {
            val index = (Math.random() * lns.size).toInt()
            val ln = lns[index]
            treeMutable!!.put(ln)
            lns.removeAt(index)
        }
        checkTree(treeMutable!!, s).run()
        val rootAddress = saveTree()
        checkTree(treeMutable!!, s).run()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        checkTree(tree!!, s).run()
    }

    @Test
    fun testPutOverwriteTreeWithoutDuplicates() {
        // add existing key to tree that supports duplicates
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).mutableCopy
        for (i in 0..99) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, 100).run()

        // put must add 100 new values
        for (i in 0..99) {
            val ln: INode = kv(i, "vv$i")
            treeMutable!!.put(ln)
        }
        checkTree(treeMutable!!, "vv", 100).run()
        val rootAddress = saveTree()
        checkTree(treeMutable!!, "vv", 100).run()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        checkTree(treeMutable!!, "vv", 100).run()
    }

    @Test
    fun testPutOverwriteTreeWithDuplicates() {
        // add existing key to tree that supports duplicates
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        for (i in 0..99) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, 100).run()

        // put must add 100 new values
        for (i in 0..99) {
            val ln: INode = kv(i, "vv$i")
            treeMutable!!.put(ln)
        }

        // expected nodes
        val l: MutableList<INode> = ArrayList()
        for (i in 0..99) {
            l.add(kv(i, "v$i"))
            l.add(kv(i, "vv$i"))
        }
        assertMatchesIterator(treeMutable!!, l)
        val rootAddress = saveTree()
        assertMatchesIterator(treeMutable!!, l)
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        assertMatchesIterator(treeMutable!!, l)
    }

    @Test
    fun testPutAndDelete() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        for (i in 0..99) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        var rootAddress = saveTree()
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).mutableCopy
        checkTree(treeMutable!!, 100).run()
        for (i in 0..99) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        Assert.assertEquals(1L, treeMutable!!.expiredLoggables.size.toLong())
        for (i in 0..99) {
            val ln: INode = kv(i, "v$i")
            treeMutable!!.delete(ln.key, ln.value)
        }
        Assert.assertEquals(0, treeMutable!!.size)
        assertMatchesIterator(treeMutable!!, emptyList())
        rootAddress = saveTree()
        reopen()
        tree = BTree(log!!, rootAddress, true, 1)
        assertMatchesIterator(treeMutable!!, emptyList())
    }

    @Test
    fun testPutNoOverwriteTreeWithoutDuplicates() {
        putNoOverwrite(false)
    }

    @Test
    fun testPutNoOverwriteTreeWithDuplicates() {
        putNoOverwrite(true)
    }

    private fun putNoOverwrite(duplicates: Boolean) {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            duplicates,
            1
        ).mutableCopy
        for (i in 0..99) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        checkTree(treeMutable!!, 100).run()
        for (i in 0..99) {
            val ln: INode = kv(i, "vv$i")
            Assert.assertFalse(treeMutable!!.add(ln))
        }
    }

    @Test
    fun testPutSortDuplicates() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        val expected: MutableList<INode> = ArrayList()
        expected.add(kv("1", "1"))
        expected.add(kv("2", "2"))
        expected.add(kv("3", "3"))
        expected.add(kv("5", "51"))
        expected.add(kv("5", "52"))
        expected.add(kv("5", "53"))
        expected.add(kv("5", "54"))
        expected.add(kv("5", "55"))
        expected.add(kv("5", "56"))
        expected.add(kv("5", "57"))
        expected.add(kv("7", "7"))
        for (ln in expected) {
            treeMutable!!.put(ln)
        }
        assertMatchesIterator(treeMutable!!, expected)
    }

    @Test
    fun testPutRightSortDuplicates() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        val expected: MutableList<INode> = ArrayList()
        expected.add(kv("1", "1"))
        expected.add(kv("2", "2"))
        expected.add(kv("3", "3"))
        expected.add(kv("5", "51"))
        expected.add(kv("5", "52"))
        expected.add(kv("5", "53"))
        expected.add(kv("5", "54"))
        expected.add(kv("5", "55"))
        expected.add(kv("5", "56"))
        expected.add(kv("5", "57"))
        expected.add(kv("7", "7"))
        for (ln in expected) {
            treeMutable!!.putRight(ln)
        }
        assertMatchesIterator(treeMutable!!, expected)
    }

    @Test
    fun testGetReturnsFirstSortedDuplicate() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).mutableCopy
        val l: MutableList<INode> = ArrayList()
        l.add(kv("1", "1"))
        l.add(kv("2", "2"))
        l.add(kv("3", "3"))
        l.add(kv("5", "51"))
        l.add(kv("5", "52"))
        l.add(kv("5", "53"))
        l.add(kv("5", "54"))
        l.add(kv("5", "55"))
        l.add(kv("5", "56"))
        l.add(kv("5", "57"))
        l.add(kv("7", "7"))
        for (ln in l) {
            treeMutable!!.add(ln)
        }
        valueEquals("51", treeMutable!![key("5")])
    }
}
