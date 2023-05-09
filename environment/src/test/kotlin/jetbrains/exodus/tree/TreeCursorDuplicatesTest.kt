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
package jetbrains.exodus.tree

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.core.dataStructures.hash.LongHashSet
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Before
import org.junit.Test

abstract class TreeCursorDuplicatesTest : TreeBaseTest<ITree, ITreeMutable>() {
    var values: MutableList<INode> = ArrayList()
    var valuesNoDup: MutableSet<INode> = LinkedHashSet()

    @Before
    fun prepareTree() {
        treeMutable = createMutableTree(true, 1)
        values.add(kv(1, "v1"))
        values.add(kv(2, "v2"))
        values.add(kv(5, "v51"))
        values.add(kv(5, "v52"))
        values.add(kv(5, "v53"))
        values.add(kv(7, "v7"))
        values.add(kv(8, "v8"))
        values.add(kv(9, "v9"))
        values.add(kv(10, "v10"))
        values.add(kv(11, "v11"))
        values.add(kv(12, "v12"))
        for (ln in values) {
            treeMutable!!.put(ln)
            valuesNoDup.add(ln)
        }
    }

    @Test
    fun testInitialState() {
        val initial: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable!!) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertFalse(c.key.iterator().hasNext())
                Assert.assertFalse(c.value.iterator().hasNext())
            }
        }
        initial.run()
        val a = saveTree()
        initial.run()
        reopen()
        initial.setTree(openTree(a, true))
        initial.run()
    }

    @Test
    fun testCount() {
        val count: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(value("v1"), c.getSearchKey(key(1)))
                Assert.assertEquals(1, c.count().toLong())
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchKey(key(5))
                )
                Assert.assertEquals(3, c.count().toLong())
                Assert.assertEquals(
                    value("v12"),
                    c.getSearchKey(key(12))
                )
                Assert.assertEquals(1, c.count().toLong())
            }
        }
        count.run()
        val a = saveTree()
        count.run()
        reopen()
        count.setTree(openTree(a, true))
        count.run()
    }

    @Test
    fun testGetNext() {
        val getNext: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in values) {
                    Assert.assertTrue(c.next)
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertFalse(c.next)
            }
        }
        getNext.run()
        val a = saveTree()
        getNext.run()
        reopen()
        getNext.setTree(openTree(a, true))
        getNext.run()
    }

    @Test
    fun testGetNextDup() {
        val getNextDup: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(value("v1"), c.getSearchKey(key(1)))
                Assert.assertFalse(c.nextDup)
                c.getSearchKey(key(5)) // 51
                Assert.assertTrue(c.nextDup) // 52
                Assert.assertTrue(c.nextDup) // 53
                Assert.assertFalse(c.nextDup)
                c.getSearchKey(key(12))
                Assert.assertFalse(c.nextDup)
            }
        }
        getNextDup.run()
        val a = saveTree()
        getNextDup.run()
        reopen()
        getNextDup.setTree(openTree(a, true))
        getNextDup.run()
    }

    @Test
    fun testGetNextDup2() {
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(value("v1"), c.getSearchKey(key(1)))
        Assert.assertFalse(c.nextDup)
        c.getSearchKey(key(5)) // 51
        c.nextDup // 52
        c.nextDup // 53
        c.deleteCurrent()
        Assert.assertFalse(c.nextDup)
    }

    @Test
    fun testGetNextNoDup() {
        val getNextNoDup: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in valuesNoDup) {
                    Assert.assertTrue(c.nextNoDup)
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertFalse(c.nextNoDup)
            }
        }
        getNextNoDup.run()
        val a = saveTree()
        getNextNoDup.run()
        reopen()
        getNextNoDup.setTree(openTree(a, true))
        getNextNoDup.run()
    }

    @Test
    fun testGetNextNoDup2() {
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(value("v1"), c.getSearchKey(key(1)))
        Assert.assertFalse(c.nextDup)
        c.getSearchKey(key(5)) // 51
        c.deleteCurrent()
        Assert.assertTrue(c.nextNoDup)
        Assert.assertEquals(key(7), c.key)
        Assert.assertEquals(value("v7"), c.value)
    }

    @Test
    fun testGetSearchKey() {
        val getSearchKey: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in valuesNoDup) {
                    Assert.assertEquals(ln.getValue(), c.getSearchKey(ln.getKey()))
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertNull(c.getSearchKey(key(0)))
                Assert.assertNull(c.getSearchKey(key(4)))
                Assert.assertNull(c.getSearchKey(key(13)))
                // prev state due to failed search
                Assert.assertEquals(values[values.size - 1].getValue(), c.value)
                Assert.assertEquals(values[values.size - 1].getKey(), c.key)
            }
        }
        getSearchKey.run()
        val a = saveTree()
        getSearchKey.run()
        reopen()
        getSearchKey.setTree(openTree(a, true))
        getSearchKey.run()
    }

    @Test
    fun testGetSearchBoth() {
        val getSearchBoth: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in values) {
                    Assert.assertTrue(c.getSearchBoth(ln.getKey(), ln.getValue()!!))
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertFalse(
                    c.getSearchBoth(
                        key(0),
                        value("v1")
                    )
                )
                Assert.assertFalse(
                    c.getSearchBoth(
                        key(4),
                        value("v1")
                    )
                )
                Assert.assertFalse(
                    c.getSearchBoth(
                        key(13),
                        value("v1")
                    )
                )
                // prev state due to failed search
                Assert.assertEquals(values[values.size - 1].getValue(), c.value)
                Assert.assertEquals(values[values.size - 1].getKey(), c.key)
            }
        }
        getSearchBoth.run()
        val a = saveTree()
        getSearchBoth.run()
        reopen()
        getSearchBoth.setTree(openTree(a, true))
        getSearchBoth.run()
    }

    @Test
    fun testGetSearchKeyRange1() {
        val getSearchKeyRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in valuesNoDup) {
                    Assert.assertEquals(ln.getValue(), c.getSearchKeyRange(ln.getKey()))
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
            }
        }
        getSearchKeyRange.run()
        val a = saveTree()
        getSearchKeyRange.run()
        reopen()
        getSearchKeyRange.setTree(openTree(a, true))
        getSearchKeyRange.run()
    }

    @Test
    fun testGetSearchKeyRange2() {
        val getSearchKeyRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(
                    value("v1"),
                    c.getSearchKeyRange(key(0))
                )
                Assert.assertEquals(key(1), c.key)
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchKeyRange(key(3))
                )
                Assert.assertEquals(key(5), c.key)
                Assert.assertTrue(c.nextDup)
                Assert.assertEquals(value("v52"), c.value)
                Assert.assertEquals(key(5), c.key)
                Assert.assertTrue(c.nextDup)
                Assert.assertEquals(value("v53"), c.value)
                Assert.assertEquals(key(5), c.key)
                Assert.assertNull(c.getSearchKeyRange(key(13)))
                // cursor keep prev pos
                Assert.assertEquals(value("v53"), c.value)
                Assert.assertEquals(key(5), c.key)
            }
        }
        getSearchKeyRange.run()
        val a = saveTree()
        getSearchKeyRange.run()
        reopen()
        getSearchKeyRange.setTree(openTree(a, true))
        getSearchKeyRange.run()
    }

    @Test
    fun testGetSearchKeyRange3() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(kv(1, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(3, "v3"))
        treeMutable!!.put(kv(5, "v51"))
        treeMutable!!.put(kv(5, "v52"))
        treeMutable!!.put(kv(5, "v53"))
        treeMutable!!.put(kv(7, "v7"))
        treeMutable!!.put(kv(8, "v8"))

        // assertMatches(getTreeMutable(), IP(BP(3), BP(3)));
        val getSearchKeyRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchKeyRange(key(4))
                )
                Assert.assertEquals(key(5), c.key)
            }
        }
        getSearchKeyRange.run()
        val a = saveTree()
        getSearchKeyRange.run()
        reopen()
        getSearchKeyRange.setTree(openTree(a, true))
        getSearchKeyRange.run()
    }

    @Test
    fun testGetSearchKeyRange4() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(kv(1, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(3, "v3"))
        treeMutable!!.put(kv(5, "v51"))
        treeMutable!!.put(kv(5, "v52"))
        treeMutable!!.put(kv(5, "v53"))
        treeMutable!!.put(kv(7, "v7"))
        treeMutable!!.put(kv(8, "v8"))
        treeMutable!!.openCursor().use { cursor ->
            cursor.getSearchKeyRange(key(6))
            Assert.assertEquals(value("v7"), cursor.value)
            Assert.assertTrue(cursor.next)
            Assert.assertEquals(value("v8"), cursor.value)
            Assert.assertFalse(cursor.next)
        }
    }

    @Test
    fun testGetSearchBothRange1() {
        val getSearchBothRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (ln in values) {
                    Assert.assertEquals(ln.getValue(), c.getSearchBothRange(ln.getKey(), ln.getValue()!!))
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
            }
        }
        getSearchBothRange.run()
        val a = saveTree()
        getSearchBothRange.run()
        reopen()
        getSearchBothRange.setTree(openTree(a, true))
        getSearchBothRange.run()
    }

    @Test
    fun testGetSearchBothRange2() {
        val getSearchBothRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                // miss
                Assert.assertNull(
                    c.getSearchBothRange(
                        key(0),
                        value("v1")
                    )
                )

                // found
                Assert.assertEquals(
                    value("v1"),
                    c.getSearchBothRange(key(1), value("v0"))
                )
                Assert.assertEquals(key(1), c.key)
                Assert.assertEquals(value("v1"), c.value)

                // miss
                Assert.assertNull(
                    c.getSearchBothRange(
                        key(2),
                        value("v21")
                    )
                )
                // check keep prev state
                Assert.assertEquals(key(1), c.key)
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchBothRange(key(5), value("v50"))
                )
                Assert.assertEquals(key(5), c.key)
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchBothRange(key(5), value("v51"))
                )
                Assert.assertEquals(key(5), c.key)
                Assert.assertEquals(
                    value("v53"),
                    c.getSearchBothRange(key(5), value("v521"))
                )
                Assert.assertEquals(key(5), c.key)
                Assert.assertNull(
                    c.getSearchBothRange(
                        key(5),
                        value("v54")
                    )
                )
                Assert.assertEquals(value("v53"), c.value)
            }
        }
        getSearchBothRange.run()
        val a = saveTree()
        getSearchBothRange.run()
        reopen()
        getSearchBothRange.setTree(openTree(a, true))
        getSearchBothRange.run()
    }

    @Test
    fun testGetSearchBothRange3() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(kv(5, "v51"))
        treeMutable!!.put(kv(5, "v52"))
        treeMutable!!.put(kv(6, "v61"))
        treeMutable!!.put(kv(6, "v62"))

        // assertMatches(getTreeMutable(), IP(BP(3), BP(3)));
        val getSearchKeyRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(
                    value("v51"),
                    c.getSearchKeyRange(key(4))
                )
                Assert.assertEquals(key(5), c.key)
                Assert.assertNull(
                    c.getSearchBothRange(
                        key(5),
                        value("v54")
                    )
                )
                Assert.assertEquals(key(5), c.key) // key unchanged
            }
        }
        getSearchKeyRange.run()
        val a = saveTree()
        getSearchKeyRange.run()
        reopen()
        getSearchKeyRange.setTree(openTree(a, true))
        getSearchKeyRange.run()
    }

    @Test
    fun testGetPrev() {
        val getPrev: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                val itr: ListIterator<INode> = values.listIterator(values.size)
                while (itr.hasPrevious()) {
                    val ln = itr.previous()
                    Assert.assertTrue(c.prev)
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertFalse(c.prev)
            }
        }
        getPrev.run()
        val a = saveTree()
        getPrev.run()
        reopen()
        getPrev.setTree(openTree(a, true))
        getPrev.run()
    }

    @Test
    fun testGetPrev2() {
        treeMutable = createMutableTree(true, 1)
        values.clear()
        values.add(kv(0, "v0"))
        values.add(kv(0, "v1"))
        values.add(kv(1, "v1"))
        for (ln in values) {
            treeMutable!!.put(ln)
            valuesNoDup.add(ln)
        }
        val getPrev: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                val itr: ListIterator<INode> = values.listIterator(values.size)
                while (itr.hasPrevious()) {
                    val ln = itr.previous()
                    Assert.assertTrue(c.prev)
                    Assert.assertEquals(ln.getValue(), c.value)
                    Assert.assertEquals(ln.getKey(), c.key)
                }
                Assert.assertFalse(c.prev)
            }
        }
        getPrev.run()
        val a = saveTree()
        getPrev.run()
        reopen()
        getPrev.setTree(openTree(a, true))
        getPrev.run()
    }

    @Test
    fun test_xd_347_like() {
        treeMutable = createMutableTree(true, 1)
        val count = 20000
        var value: Long = 0
        val values = LongHashMap<LongHashSet>()
        val rnd = Random()
        var i = 0
        while (i < count) {
            if (i > count / 2) {
                val pair: Array<Pair<Long, LongHashSet>?> = arrayOfNulls(1)
                values.forEachEntry { (key, value1): Map.Entry<Long, LongHashSet> ->
                    pair[0] = Pair(
                        key, value1
                    )
                    false
                }
                val p = pair[0]
                val oldSet = p!!.getSecond()
                val oldValue = oldSet.iterator().nextLong()
                val oldKey = p.getFirst()
                treeMutable!!.openCursor().use { cursor ->
                    if (!cursor.getSearchBoth(
                            key(oldKey),
                            value(oldValue)
                        )
                    ) {
                        Assert.assertTrue(
                            cursor.getSearchBoth(
                                key(oldKey),
                                value(oldValue)
                            )
                        )
                    }
                    cursor.deleteCurrent()
                }
                Assert.assertTrue(oldSet.remove(oldValue))
                if (oldSet.isEmpty()) {
                    Assert.assertEquals(oldSet, values.remove(oldKey))
                }
            }
            val key = System.currentTimeMillis() + rnd.nextInt(count / 10)
            var keyValues = values[key]
            if (keyValues == null) {
                keyValues = LongHashSet()
                values[key] = keyValues
            }
            Assert.assertTrue(keyValues.add(value))
            treeMutable!!.put(key(key), value(value))
            ++i
            ++value
        }
    }
}
