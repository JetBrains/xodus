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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.TestFor
import jetbrains.exodus.core.dataStructures.hash.HashSet
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.*

abstract class TreeCursorNoDuplicatesTest : CursorTestBase() {
    @Before
    fun prepareTree() {
        treeMutable = createMutableTree(false, 1)!!.mutableCopy
        for (i in 0 until s) {
            treeMutable!!.put(kv(i, "v$i"))
        }
    }

    @Test
    fun testOneNode() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "v1"))
        Assert.assertEquals(1, treeMutable!!.size)
        val cursor = treeMutable!!.openCursor()
        Assert.assertTrue(cursor.next)
        Assert.assertFalse(cursor.next)
    }

    @Test
    fun testGetNext() {
        val getNext = object : GetNext {
            override fun n(c: Cursor?): Boolean {
                return c!!.next
            }
        }
        check(treeMutable!!, getNext)
        val a = saveTree()
        check(treeMutable!!, getNext)
        reopen()
        tree = openTree(a, false)
        check(tree!!, getNext)
    }

    @Test
    fun testGetNext2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(3, "v3"))
        treeMutable!!.put(kv(4, "v4"))
        treeMutable!!.put(kv(5, "v5"))
        var c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(value("v5"), c.getSearchKey(key(5)))
        Assert.assertFalse(c.next)
        val a = saveTree()
        c = treeMutable!!.openCursor()
        Assert.assertEquals(value("v5"), c.getSearchKey(key(5)))
        Assert.assertFalse(c.getNext())
        tree = openTree(a, false)
        c = treeMutable!!.openCursor()
        Assert.assertEquals(value("v5"), c.getSearchKey(key(5)))
        Assert.assertFalse(c.getNext())
    }

    @Test
    fun testGetNext3() {
        treeMutable = createMutableTree(false, 1)
        for (i in 0..999) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(value("v998"), c.getSearchKey(key(998)))
        Assert.assertTrue(c.next) //v999 - last
        Assert.assertFalse(c.next)
    }

    @Test
    fun testCount() {
        val getNext = object : GetNext {
            override fun n(c: Cursor?): Boolean {
                return c!!.next && c.count() == 1
            }
        }
        check(treeMutable!!, getNext)
        val a = saveTree()
        check(treeMutable!!, getNext)
        reopen()
        tree = openTree(a, true)
        check(treeMutable!!, getNext)
    }

    @Test
    fun testGetNextNoDup() {
        val getNextNoDup = object : GetNext {
            override fun n(c: Cursor?): Boolean {
                return c!!.nextNoDup
            }
        }
        check(treeMutable!!, getNextNoDup)
        val a = saveTree()
        check(treeMutable!!, getNextNoDup)
        reopen()
        tree = openTree(a, true)
        check(treeMutable!!, getNextNoDup)
    }

    @Test
    fun testGetSearchKey() {
        val c: Cursor = treeMutable!!.openCursor()
        for (i in 0 until s) {
            Assert.assertEquals(
                "v$i",
                value("v$i"),
                c.getSearchKey(key(i))
            )
            Assert.assertEquals(c.value, value("v$i"))
            Assert.assertEquals(c.key, key(i))
        }
        Assert.assertFalse(c.next)
    }

    @Test
    fun testGetSearchBoth() {
        val c: Cursor = treeMutable!!.openCursor()
        for (i in 0 until s) {
            Assert.assertTrue(c.getSearchBoth(key(i), value("v$i")))
            Assert.assertEquals(c.value, value("v$i"))
            Assert.assertEquals(c.key, key(i))
        }
        Assert.assertFalse(c.next)
    }

    @Test
    fun testGetSearchBoth2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv("1", "2"))
        val address = saveTree()
        val cursor = openTree(address, false)!!.openCursor()
        Assert.assertFalse(cursor.getSearchBoth(key("1"), value("1")))
    }

    @Test
    fun testGetSearchKeyRange1() {
        val c: Cursor = treeMutable!!.openCursor()
        for (i in 0 until s) {
            Assert.assertEquals(
                value("v$i"),
                c.getSearchKeyRange(key(i))
            )
            Assert.assertEquals(c.value, value("v$i"))
            Assert.assertEquals(c.key, key(i))
        }
        Assert.assertFalse(c.next)
    }

    @Test
    fun testGetSearchKeyRange2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("10"), value("v10"))
        treeMutable!!.put(key("20"), value("v20"))
        treeMutable!!.put(key("30"), value("v30"))
        treeMutable!!.put(key("40"), value("v40"))
        treeMutable!!.put(key("50"), value("v50"))
        treeMutable!!.put(key("60"), value("v60"))
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(
            value("v10"),
            c.getSearchKeyRange(key("01"))
        )
        Assert.assertEquals(key("10"), c.key)
        Assert.assertEquals(
            value("v60"),
            c.getSearchKeyRange(key("55"))
        )
        Assert.assertEquals(key("60"), c.key)
        Assert.assertNull(c.getSearchKeyRange(key("61")))
        // cursor keep prev pos
        Assert.assertEquals(key("60"), c.key)
        Assert.assertFalse(c.next)
    }

    @Test
    fun testGetSearchKeyRange3() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(ArrayByteIterable(byteArrayOf(1)), value("v1"))
        val key = ArrayByteIterable(byteArrayOf(1, 2, 1, 0))
        treeMutable!!.put(key, value("v2"))
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertEquals(
            value("v2"),
            c.getSearchKeyRange(ArrayByteIterable(byteArrayOf(1, 2, 1)))
        )
        Assert.assertEquals(key, c.key)
        Assert.assertFalse(c.next)
    }

    @Test
    fun testGetSearchKeyRange4() {
        treeMutable = createMutableTree(false, 1)
        val v: ByteIterable = value("0")
        treeMutable!!.put(key("aaaa"), v)
        treeMutable!!.put(key("aaab"), v)
        treeMutable!!.put(key("aaba"), v)
        val c: Cursor = treeMutable!!.openCursor()
        c.getSearchKeyRange(key("aaac"))
        Assert.assertEquals(key("aaba"), c.key)
    }

    @Test
    fun testGetSearchKeyRange5() {
        treeMutable = createMutableTree(false, 1)
        val v: ByteIterable = value("0")
        treeMutable!!.put(key("aaba"), v)
        treeMutable!!.put(key("aabb"), v)
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertNotNull(c.getSearchKeyRange(key("aababa")))
        Assert.assertEquals(key("aabb"), c.key)
    }

    @Test
    fun testGetSearchBothRange1() {
        val c: Cursor = treeMutable!!.openCursor()
        for (i in 0 until s) {
            Assert.assertEquals(
                value("v$i"), c.getSearchBothRange(
                    key(i), value(
                        "v$i"
                    )
                )
            )
            Assert.assertEquals(c.value, value("v$i"))
            Assert.assertEquals(c.key, key(i))
        }
    }

    @Test
    fun testGetSearchBothRange2() {
        treeMutable = treeMutable!!.mutableCopy
        treeMutable!!.put(key("10"), value("v10"))
        treeMutable!!.put(key("20"), value("v20"))
        treeMutable!!.put(key("30"), value("v30"))
        treeMutable!!.put(key("40"), value("v40"))
        treeMutable!!.put(key("50"), value("v50"))
        treeMutable!!.put(key("60"), value("v60"))
        val c: Cursor = treeMutable!!.openCursor()
        // miss
        Assert.assertNull(
            c.getSearchBothRange(
                key("01"),
                value("v10")
            )
        )

        // found
        Assert.assertEquals(
            value("v10"),
            c.getSearchBothRange(key("10"), value("v01"))
        )

        // miss
        Assert.assertNull(
            c.getSearchBothRange(
                key("20"),
                value("v21")
            )
        )

        // check keep prev state
        Assert.assertEquals(key("10"), c.key)
    }

    @Test
    fun testGetPrev() {
        val getPrev = object : GetPrev {
            override fun p(c: Cursor?): Boolean {
                return c!!.prev
            }
        }
        val a = saveTree()
        reopen()
        tree = openTree(a, false)
        check(tree!!, getPrev)
    }

    @Test
    fun testGetPrev2() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(kv("the", "fuck"))
        val c = treeMutable!!.openCursor()
        Assert.assertTrue(c.prev)
        c.close()
    }

    @Test
    fun testGetLast_XD_466() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.openCursor().use { c -> Assert.assertFalse(c.last) }
        for (i in 0..9998) {
            val kv = kv(i, i.toString()) as StringKVNode
            treeMutable!!.put(kv)
            treeMutable!!.openCursor().use { c ->
                Assert.assertTrue(c.last)
                Assert.assertEquals(kv.key, c.key)
                Assert.assertEquals(kv.value, c.value)
                if (i > 0) {
                    Assert.assertTrue(c.prev)
                    Assert.assertNotEquals(kv.key, c.key)
                    Assert.assertNotEquals(kv.value, c.value)
                }
                Assert.assertTrue(c.last)
                Assert.assertEquals(kv.key, c.key)
                Assert.assertEquals(kv.value, c.value)
            }
        }
    }

    @Test
    fun testSplitRange() {
        treeMutable = treeMutable!!.mutableCopy
        treeMutable!!.put(key("aaabbb"), value("v10"))
        treeMutable!!.put(key("aaaddd"), value("v20"))
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertNotNull(c.getSearchKeyRange(key("aaa")))
        Assert.assertEquals(value("v10"), c.value)
        Assert.assertNull(c.getSearchKey(key("aaa")))
    }

    @Test
    fun testSplitRange2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("aa"), value("v"))
        treeMutable!!.put(key("ab"), value("v"))
        val cursor = treeMutable!!.openCursor()
        Assert.assertNull(cursor.getSearchKeyRange(key("bb")))
    }

    @Test
    fun xd_333() {
        rnd = Random(0)
        val value: ByteIterable = value("value")
        treeMutable = createMutableTree(false, 1)
        val keys = TreeSet<String>()
        for (i in 0..14) {
            val key = rndString()
            treeMutable!!.put(key(key), value)
            keys.add(key)
        }
        /*final long address = saveTree();
        reopen();
        final ITree tree = openTree(address, false);*/testCursorOrder(keys)
    }

    @Test
    fun testOrderedInserts() {
        val value: ByteIterable = value("value")
        val keys = TreeSet<String>()
        for (i in 0..9999) {
            keys.add(rndString())
        }
        treeMutable = createMutableTree(false, 1)
        for (key in keys) {
            Assert.assertTrue(treeMutable!!.add(key(key), value))
        }
        testCursorOrder(keys)
        treeMutable = createMutableTree(false, 1)
        for (key in keys.descendingSet()) {
            Assert.assertTrue(treeMutable!!.add(key(key), value))
        }
        testCursorOrder(keys)
    }

    @Test
    fun testRandomInserts() {
        val value: ByteIterable = value("value")
        val keys: MutableSet<String> = HashSet()
        for (i in 0..9999) {
            keys.add(rndString())
        }
        treeMutable = createMutableTree(false, 1)
        for (key in keys) {
            Assert.assertTrue(treeMutable!!.add(key(key), value))
        }
        testCursorOrder(TreeSet(keys))
    }

    @Test
    fun testInsertDeletes() {
        val value: ByteIterable = value("value")
        val keys = TreeSet<String>()
        treeMutable = createMutableTree(false, 1)
        for (i in 0..9999) {
            val key = rndString()
            if (keys.add(key)) {
                Assert.assertTrue(treeMutable!!.add(key(key), value))
            }
            testCursorOrder(keys)

            if (keys.size > 1000) {
                val obsoleteKey = keys.first()
                keys.remove(obsoleteKey)
                Assert.assertTrue(treeMutable!!.delete(key(obsoleteKey)))
            }

            testCursorOrder(keys)
        }
        testCursorOrder(keys)
    }

    @Test
    fun testInsertDeletes2() {
        val value: ByteIterable = value("value")
        val keys: MutableSet<String> = HashSet()
        treeMutable = createMutableTree(false, 1)
        for (i in 0..9999) {
            val key = rndString()
            if (keys.add(key)) {
                Assert.assertTrue(treeMutable!!.add(key(key), value))
            }
            if (keys.size > 1000) {
                val obsoleteKey = keys.iterator().next()
                keys.remove(obsoleteKey)
                Assert.assertTrue(treeMutable!!.delete(key(obsoleteKey)))
            }
        }
        testCursorOrder(TreeSet(keys))
    }

    @Test
    @TestFor(issue = "XD-614")
    fun failingGetNextAndGetPrevDontInvalidateKeyValue() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv("0", "0"))
        treeMutable!!.openCursor().use { cursor ->
            Assert.assertTrue(cursor.next)
            Assert.assertEquals(key("0"), cursor.key)
            Assert.assertEquals(key("0"), cursor.value)
            Assert.assertFalse(cursor.next)
            Assert.assertEquals(key("0"), cursor.key)
            Assert.assertEquals(key("0"), cursor.value)
        }
        treeMutable!!.openCursor().use { cursor ->
            Assert.assertTrue(cursor.prev)
            Assert.assertEquals(key("0"), cursor.key)
            Assert.assertEquals(key("0"), cursor.value)
            Assert.assertFalse(cursor.prev)
            Assert.assertEquals(key("0"), cursor.key)
            Assert.assertEquals(key("0"), cursor.value)
        }
    }

    @Test
    @TestFor(issue = "XD-619")
    fun failingGetNextAndGetPrevDontInvalidateKeyValue2() {
        treeMutable = createMutableTree(false, 1)
        val treeSize = 10000
        for (i in 0 until treeSize) {
            treeMutable!!.put(kv(i, i.toString()))
        }
        treeMutable!!.openCursor().use { cursor ->
            var key: ByteIterable? = null
            while (cursor.next) {
                key = cursor.key
            }
            Assert.assertEquals(key(treeSize - 1), key)
            Assert.assertEquals(key(treeSize - 1), cursor.key)
            cursor.next
            cursor.next
            Assert.assertEquals(key(treeSize - 1), cursor.key)
            cursor.prev
            Assert.assertEquals(key(treeSize - 2), cursor.key)
        }
        treeMutable!!.openCursor().use { cursor ->
            var key: ByteIterable? = null
            while (cursor.prev) {
                key = cursor.key
            }
            Assert.assertEquals(key(0), key)
            Assert.assertEquals(key(0), cursor.key)
            cursor.prev
            cursor.prev
            Assert.assertEquals(key(0), cursor.key)
            cursor.next
            Assert.assertEquals(key(1), cursor.key)
        }
    }

    private fun testCursorOrder(keys: TreeSet<String>) {
        val cursor = treeMutable!!.openCursor()
        for (key in keys) {
            Assert.assertNotNull(treeMutable!![key(key)])
            Assert.assertTrue(cursor.next)
            valueEquals(key, cursor.key)
        }
        cursor.close()
    }

    companion object {
        private var rnd = Random(3243)
        private fun rndString(): String {
            val len = rnd.nextInt(4) + 1
            val builder = StringBuilder(len)
            for (i in 0 until len) {
                builder.append(('0'.code + rnd.nextInt(10)).toChar())
            }
            return builder.toString()
        }
    }
}
