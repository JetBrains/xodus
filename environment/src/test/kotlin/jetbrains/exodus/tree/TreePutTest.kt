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

import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.IntHashSet
import org.junit.Assert
import org.junit.Test
import kotlin.math.abs

abstract class TreePutTest : TreeBaseTest<ITree, ITreeMutable>() {
    @Test
    fun testTrivialGet() {
        treeMutable = createMutableTree(false, 1)
        val address = saveTree()
        tree = openTree(address, false)
        Assert.assertNull(tree!![key(1)])
    }

    @Test
    fun testPutOverwriteWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        Companion.valueEquals("1", treeMutable!![key("1")])
        treeMutable!!.put(key("1"), value("11"))
        Companion.valueEquals("11", treeMutable!![key("1")])
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertTrue(treeMutable!!.hasKey(key("1")))
        Assert.assertFalse(treeMutable!!.hasKey(key("2")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("11")))
        Assert.assertFalse(treeMutable!!.hasPair(key("1"), value("1")))
    }

    @Test
    fun testPutOverwriteWithDuplicates() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(key("1"), value("1"))
        Companion.valueEquals("1", treeMutable!![key("1")])
        Assert.assertTrue(treeMutable!!.put(key("1"), value("11")))
        Companion.valueEquals("1", treeMutable!![key("1")])
        Assert.assertEquals(2, treeMutable!!.size())
        Assert.assertFalse(treeMutable!!.put(key("1"), value("11")))
    }

    @Test
    fun testAddNoOverwriteWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        Assert.assertTrue(treeMutable!!.add(key("1"), value("1")))
        Companion.valueEquals("1", treeMutable!![key("1")])
        Assert.assertFalse(treeMutable!!.add(key("1"), value("11")))
        Companion.valueEquals("1", treeMutable!![key("1")])
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertTrue(treeMutable!!.hasKey(key("1")))
        Assert.assertFalse(treeMutable!!.hasKey(key("2")))
        Assert.assertFalse(treeMutable!!.hasPair(key("1"), value("11")))
        Assert.assertTrue(treeMutable!!.hasPair(key("1"), value("1")))
    }

    @Test
    fun testPutKeysWithSamePrefix() {
        treeMutable = createMutableTree(false, 1)
        val key = StringBuilder()
        for (i in 0..99) {
            key.append('1')
            Assert.assertTrue(
                treeMutable!!.add(
                    key(key.toString()),
                    value(i.toString())
                )
            )
        }
        key.setLength(0)
        for (i in 0..99) {
            key.append('1')
            Companion.valueEquals(i.toString(), treeMutable!![key(key.toString())])
        }
    }

    @Test
    fun testPutAllOverwriteWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        val count = 1000
        for (i in 0 until count) {
            treeMutable!!.put(key(i.toString()), value(i.toString()))
        }
        for (i in 0 until count) {
            treeMutable!!.put(
                key(i.toString()),
                value((count - i).toString())
            )
        }
        Assert.assertEquals(count.toLong(), treeMutable!!.size())
        for (i in 0 until count) {
            Companion.valueEquals(
                (count - i).toString(),
                treeMutable!![key(i.toString())]
            )
        }
    }

    @Test
    fun testPutReopen() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Companion.valueEquals("1", tree!![key("1")])
    }

    @Test
    fun testPutReopen2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("11"), value("1"))
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Companion.valueEquals("1", tree!![key("11")])
    }

    @Test
    fun testPutReopen3() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        treeMutable!!.put(key("2"), value("1"))
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Companion.valueEquals("1", tree!![key("1")])
        Companion.valueEquals("1", tree!![key("2")])
    }

    @Test
    fun testPutReopen4() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        treeMutable!!.put(key("2"), value("1"))
        var address = saveTree()
        reopen()
        tree = openTree(address, false)
        Companion.valueEquals("1", tree!![key("1")])
        Companion.valueEquals("1", tree!![key("2")])
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.put(key("2"), value("2"))
        address = saveTree()
        reopen()
        tree = openTree(address, false)
        Companion.valueEquals("1", tree!![key("1")])
        Companion.valueEquals("2", tree!![key("2")])
    }

    @Test
    fun testPutRight() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        treeMutable!!.put(key("2"), value("1"))
        TestUtil.runWithExpectedException({
            treeMutable!!.putRight(
                key("1"),
                value("1")
            )
        }, IllegalArgumentException::class.java)
        TestUtil.runWithExpectedException({
            treeMutable!!.putRight(
                key("2"),
                value("2")
            )
        }, IllegalArgumentException::class.java)
        treeMutable!!.putRight(key("3"), value("3"))
        TestUtil.runWithExpectedException({
            treeMutable!!.putRight(
                key("1"),
                value("1")
            )
        }, IllegalArgumentException::class.java)
        TestUtil.runWithExpectedException({
            treeMutable!!.putRight(
                key("2"),
                value("1")
            )
        }, IllegalArgumentException::class.java)
    }

    @Test
    fun testPutRight3() {
        treeMutable = createMutableTree(false, 1)
        val count = 10000
        for (i in 0 until count) {
            treeMutable!!.putRight(IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
            if (i % 32 == 0) {
                val address = saveTree()
                treeMutable = openTree(address, false)!!.getMutableCopy()
            }
        }
        val address = saveTree()
        treeMutable = openTree(address, false)!!.getMutableCopy()
        val cursor = treeMutable!!.openCursor()
        for (i in 0 until count) {
            Assert.assertTrue(cursor.next)
            val key = cursor.key
            val value = cursor.value
            Assert.assertEquals(0, key.compareTo(value).toLong())
            Assert.assertEquals(i.toLong(), IntegerBinding.readCompressed(key.iterator()).toLong())
        }
        cursor.close()
    }

    @Test
    fun testPutRight2() {
        treeMutable = createMutableTree(false, 1)
        val key = StringBuilder()
        val count = 1000
        for (i in 0 until count) {
            key.append('1')
            Assert.assertTrue(
                treeMutable!!.add(
                    key(key.toString()),
                    value(i.toString())
                )
            )
        }
        key.setLength(0)
        for (i in 0 until count - 1) {
            key.append('1')
            TestUtil.runWithExpectedException({
                treeMutable!!.putRight(
                    key(key.toString()),
                    value("0")
                )
            }, IllegalArgumentException::class.java)
        }
    }

    @Test
    fun xd_329() {
        treeMutable = createMutableTree(false, 1)
        val count: Long = 17
        for (i in 0 until count) {
            treeMutable!!.putRight(LongBinding.longToCompressedEntry(i), LongBinding.longToCompressedEntry(i))
            val address = saveTree()
            reopen()
            treeMutable = openTree(address, false)!!.getMutableCopy()
        }
        treeMutable!!.putRight(LongBinding.longToCompressedEntry(count), LongBinding.longToCompressedEntry(count))
    }

    @Test
    fun xd_329_with_ints() {
        treeMutable = createMutableTree(false, 1)
        val count = 33
        for (i in 0 until count) {
            treeMutable!!.putRight(IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
            val address = saveTree()
            reopen()
            treeMutable = openTree(address, false)!!.getMutableCopy()
        }
        treeMutable!!.putRight(IntegerBinding.intToCompressedEntry(count), IntegerBinding.intToCompressedEntry(count))
    }

    @Test
    fun testPutKeysWithSamePrefixReopen() {
        treeMutable = createMutableTree(false, 1)
        val key = StringBuilder()
        val count = 500
        for (i in 0 until count) {
            key.append('1')
            Assert.assertTrue(
                treeMutable!!.add(
                    key(key.toString()),
                    value(i.toString())
                )
            )
        }
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        key.setLength(0)
        for (i in 0 until count) {
            key.append('1')
            Companion.valueEquals(i.toString(), tree!![key(key.toString())])
        }
    }

    @Test
    fun testPutRandomWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        val map = IntHashMap<String>()
        val count = 200000
        TestUtil.time("put()") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                val value = i.toString()
                treeMutable!!.put(key(key.toString()), value(value))
                map.put(key, value)
            }
        }
        Assert.assertEquals(map.size.toLong(), treeMutable!!.size())
        TestUtil.time("get()") {
            for ((key, value) in map) {
                Companion.valueEquals(value, treeMutable!![key(key.toString())])
            }
        }
    }

    @Test
    fun testPutRandomWithoutDuplicates2() {
        treeMutable = createMutableTree(false, 1)
        val map = IntHashMap<String>()
        val count = 200000
        TestUtil.time("put()") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                val value = i.toString()
                treeMutable!!.put(key(key.toString()), value(value))
                map.put(key, value)
            }
        }
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Assert.assertEquals(map.size.toLong(), tree!!.size())
        TestUtil.time("get()") {
            for ((key, value) in map) {
                Companion.valueEquals(value, tree!![key(key.toString())])
            }
        }
    }

    @Test
    fun testPutRightRandomWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        val map = IntHashMap<String>()
        val count = 99999
        TestUtil.time("putRight()") {
            for (i in 0 until count) {
                val value = i.toString()
                treeMutable!!.putRight(key(i), value(value))
                map.put(i, value)
            }
        }
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Assert.assertEquals(map.size.toLong(), tree!!.size())
        TestUtil.time("get()") {
            for ((key, value) in map) {
                Companion.valueEquals(value, tree!![key(key)])
            }
        }
    }

    @Test
    fun testAddRandomWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        val map = IntHashMap<String>()
        val count = 50000
        TestUtil.time("add()") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                val value = i.toString()
                Assert.assertEquals(
                    !map.containsKey(key),
                    treeMutable!!.add(key(key.toString()), value(value))
                )
                if (!map.containsKey(key)) {
                    map.put(key, value)
                }
            }
        }
        val address = saveTree()
        reopen()
        tree = openTree(address, false)
        Assert.assertEquals(map.size.toLong(), tree!!.size())
        TestUtil.time("get()") {
            for ((key, value) in map) {
                Companion.valueEquals(value, tree!![key(key.toString())])
            }
        }
        treeMutable = tree!!.getMutableCopy()
        TestUtil.time("Failing add()") {
            for ((key, value) in map) {
                Assert.assertFalse(
                    treeMutable!!.add(
                        key(key.toString()),
                        value(value)
                    )
                )
            }
        }
    }

    @Test
    @TestFor(issue = "XD-539")
    fun createHugeTree() {
        if (Runtime.getRuntime().maxMemory() < 4000000000L) {
            return
        }
        treeMutable = createMutableTree(false, 1)
        val set = IntHashSet()
        val count = 20000
        val builder = StringBuilder("value")
        TestUtil.time("put()") {
            for (i in 0 until count) {
                treeMutable!!.put(
                    key(i.toString()),
                    value(builder.toString())
                )
                set.add(i)
                builder.append(i)
            }
        }
        val address = saveTree()
        println("Log size: " + treeMutable!!.getLog().getHighAddress())
        reopen()
        tree = openTree(address, false)
        Assert.assertEquals(set.size.toLong(), tree!!.size())
        TestUtil.time("get()") {
            for (i in set) {
                Assert.assertTrue(tree!!.hasKey(key(i!!.toString())))
            }
        }
    }
}
