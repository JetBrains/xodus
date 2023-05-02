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

import org.junit.Assert
import org.junit.Test

class BTreePutSpecificTest : BTreeTestBase() {
    @Test
    fun testPutDuplicateTreeWithDuplicates() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv("1", "1"))
        valueEquals("1", treeMutable!![key("1")])
        treeMutable!!.put(kv("1", "11"))
        valueEquals("1", treeMutable!![key("1")])
        Assert.assertTrue(treeMutable!!.hasKey(key("1")))
        Assert.assertFalse(treeMutable!!.hasKey(key("2")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("11")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("1")))
    }

    @Test
    fun testPutDuplicateTreeWithDuplicates2() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("1", "11"))
        treeMutable!!.put(kv("1", "11"))
        assertMatchesIterator(
            treeMutable!!,
            true,
            kv("1", "1"),
            kv("1", "11")
        )
    }

    @Test
    fun testNextDup() {
        treeMutable = createEmptyTreeForCursor(1).mutableCopy
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("1", "2"))
        treeMutable!!.put(kv("1", "3"))
        treeMutable!!.put(kv("1", "4"))
        treeMutable!!.put(kv("1", "5"))
        val cursor = treeMutable!!.openCursor()
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
    }

    @Test
    fun testNextDupWithSearch() {
        treeMutable = createEmptyTreeForCursor(1).mutableCopy
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("1", "2"))
        treeMutable!!.put(kv("1", "3"))
        treeMutable!!.put(kv("1", "4"))
        treeMutable!!.put(kv("1", "5"))
        val cursor = treeMutable!!.openCursor()
        Assert.assertNotNull(cursor.getSearchKey(key("1")))
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
        Assert.assertTrue(cursor.nextDup)
    }

    @Test
    fun testPutNoOverwriteDuplicateTreeWithDuplicates2() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        Assert.assertTrue(treeMutable!!.add(kv("1", "1")))
        valueEquals("1", treeMutable!![key("1")])
        Assert.assertFalse(treeMutable!!.add(kv("1", "11")))
        valueEquals("1", treeMutable!![key("1")])
        Assert.assertTrue(treeMutable!!.hasKey(key("1")))
        Assert.assertFalse(treeMutable!!.hasKey(key("2")))
        Assert.assertFalse(treeMutable!!.hasPair(key("1"), value("11")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("1")))
    }

    @Test
    fun testIterateOverDuplicates1() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("1", "11"))
        Assert.assertEquals(2, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            treeMutable!!,
            kv("1", "1"),
            kv("1", "11")
        )
        val address = saveTree()
        tree =BTree(log!!, address, true, 2)
        Assert.assertEquals(2, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            tree!!,
            kv("1", "1"),
            kv("1", "11")
        )
    }

    @Test
    fun testPutDuplicateTreeWithDuplicatesAfterSaveNoOrigDups() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), true, 1).mutableCopy

        // no duplicates
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("2", "1"))
        var a = saveTree()
        reopen()
        treeMutable = BTree(log!!, BTreeBalancePolicy(4), a, true, 2).mutableCopy
        treeMutable!!.put(kv("1", "11"))
        treeMutable!!.put(kv("2", "22"))
        a = saveTree()
        reopen()
        tree =BTree(log!!, BTreeBalancePolicy(4), a, true, 2)
        Assert.assertTrue(tree!!.hasKey(key("1")))
        Assert.assertTrue(tree!!.hasKey(key("2")))
        Assert.assertFalse(tree!!.hasKey(key("3")))
        Assert.assertTrue(tree!!.hasPair(key("1"), value("11")))
        Assert.assertTrue(tree!!.hasPair(key("2"), value("22")))
        Assert.assertFalse(tree!!.hasPair(key("3"), value("1")))
    }

    @Test
    fun testPutDuplicateTreeWithDuplicatesAfterSaveOrigDupsPresent() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), true, 1).mutableCopy

        // dups present
        treeMutable!!.put(kv("1", "11"))
        treeMutable!!.put(kv("1", "12"))
        treeMutable!!.put(kv("2", "21"))
        var a = saveTree()
        reopen()
        treeMutable = BTree(log!!, BTreeBalancePolicy(4), a, true, 1).mutableCopy
        treeMutable!!.put(kv("1", "13"))
        treeMutable!!.put(kv("2", "22"))
        a = saveTree()
        reopen()
        tree =BTree(log!!, BTreeBalancePolicy(4), a, true, 1)
        assertMatchesIterator(
            tree!!,
            kv("1", "11"),
            kv("1", "12"),
            kv("1", "13"),
            kv("2", "21"),
            kv("2", "22")
        )
    }

    @Test
    fun testIterateOverDuplicates2() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv("0", "0"))
        treeMutable!!.put(kv("1", "1"))
        treeMutable!!.put(kv("2", "2"))
        treeMutable!!.put(kv("1", "11"))
        Assert.assertEquals(4, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            treeMutable!!,
            kv("0", "0"),
            kv("1", "1"),
            kv("1", "11"),
            kv("2", "2")
        )
        val address = saveTree()
        tree =BTree(log!!, address, true, 1)
        Assert.assertEquals(4, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            treeMutable!!,
            kv("0", "0"),
            kv("1", "1"),
            kv("1", "11"),
            kv("2", "2")
        )
    }

    @Test
    fun testIterateOverDuplicates3() {
        treeMutable = BTreeEmpty(log!!, true, 1).mutableCopy
        treeMutable!!.put(kv("0", "0"))
        treeMutable!!.put(kv("2", "2"))
        treeMutable!!.put(kv("1", "11"))
        treeMutable!!.put(kv("1", "1"))
        Assert.assertEquals(4, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            treeMutable!!,
            kv("0", "0"),
            kv("1", "1"),
            kv("1", "11"),
            kv("2", "2")
        )
        val address = saveTree()
        tree =BTree(log!!, address, true, 1)
        Assert.assertEquals(4, treeMutable!!.size)
        assertMatchesIteratorAndExists(
            treeMutable!!,
            kv("0", "0"),
            kv("1", "1"),
            kv("1", "11"),
            kv("2", "2")
        )
    }

    @Test
    fun testSplitRight() {
        treeMutable = BTreeEmpty(
            log!!,
            object : BTreeBalancePolicy(5) {
                override fun getSplitPos(page: BasePage, insertPosition: Int): Int {
                    return page.size - 1
                }
            }, true, 1
        ).mutableCopy
        for (i in 0..6) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        Assert.assertEquals(7, treeMutable!!.size)
        val r: TreeAwareRunnable = object : TreeAwareRunnable() {
            override fun run() {
                // root = internal -> bottom1, bottom2
                Assert.assertTrue(treeMutable!!.root is InternalPageMutable)
                // root
                val ipm = treeMutable!!.root as InternalPageMutable
                Assert.assertEquals(2, ipm.size.toLong())
                Assert.assertEquals(key(0), ipm.keys[0]!!.key)
                Assert.assertEquals(key(4), ipm.keys[1]!!.key)

                // bottom1
                val bp1 = ipm.children[0] as BottomPageMutable
                Assert.assertEquals(4, bp1.size.toLong())
                for (i in 0..3) {
                    Assert.assertEquals(key(i), bp1.keys[i]!!.key)
                    valueEquals("v$i", bp1.keys[i]!!.value)
                }

                // bottom2
                val bp2 = ipm.children[1] as BottomPageMutable
                Assert.assertEquals(3, bp2.size.toLong())
                for (i in 4..6) {
                    Assert.assertEquals(key(i), bp2.keys[i - 4]!!.key)
                    valueEquals("v$i", bp2.keys[i - 4]!!.value)
                }
            }
        }
        val r2 = checkTree(treeMutable!!, 7)
        r.run()
        r2.run()

        //
        val rootAddress = saveTree()
        r.run()
        r2.run()
        reopen()
        tree =BTree(log!!, rootAddress, true, 1)
        Assert.assertEquals(7, tree!!.size)
        r2.setTree(tree)
        r2.run()
    }

    @Test
    fun testSplitDefault() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(7), true, 1).mutableCopy
        for (i in 0..9) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        assertMatches(treeMutable!!, ip(bp(6), bp(4)))
    }

    @Test
    fun testSplitAfterSave() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(4), false, 1).mutableCopy
        treeMutable!!.put(kv(1, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(3, "v3"))
        treeMutable!!.put(kv(4, "v4"))
        treeMutable!!.put(kv(5, "v5"))
        treeMutable!!.put(kv(6, "v6"))
        treeMutable!!.put(kv(7, "v7"))
        assertMatches(treeMutable!!, ip(bp(3), bp(4)))
        val a = saveTree()
        treeMutable = BTree(log!!, BTreeBalancePolicy(4), a, false, 1).mutableCopy
        treeMutable!!.put(kv(8, "v8"))
        assertMatches(treeMutable!!, ip(bp(3), bp(3), bp(2)))
    }
}
