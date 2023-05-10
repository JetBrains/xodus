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

import jetbrains.exodus.TestUtil
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import org.junit.Assert
import org.junit.Test
import kotlin.math.abs

abstract class TreeDeleteTest : TreeBaseTest<ITree, ITreeMutable>() {
    @Test
    fun testDeleteNoDuplicates() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "1"))
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertEquals(value("1"), treeMutable!![key(1)])
        Assert.assertFalse(treeMutable!!.delete(key(2)))
        Assert.assertTrue(treeMutable!!.delete(key(1)))
        Assert.assertEquals(0, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key(1)])
        val a = saveTree()
        reopen()
        tree = openTree(a, false)
        Assert.assertEquals(0, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key(1)])
    }

    @Test
    fun testDeleteNoDuplicates2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(key("1"), value("1"))
        treeMutable!!.put(key("11"), value("11"))
        treeMutable!!.put(key("111"), value("111"))
        var a = saveTree()
        reopen()
        tree = openTree(a, false)
        treeMutable = tree!!.getMutableCopy()
        Assert.assertEquals(3, treeMutable!!.size())
        Assert.assertEquals(value("1"), treeMutable!![key("1")])
        Assert.assertEquals(value("11"), treeMutable!![key("11")])
        Assert.assertEquals(value("111"), treeMutable!![key("111")])
        Assert.assertFalse(treeMutable!!.delete(key(2)))
        Assert.assertTrue(treeMutable!!.delete(key("111")))
        Assert.assertTrue(treeMutable!!.delete(key("11")))
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key("11")])
        a = saveTree()
        reopen()
        tree = openTree(a, false)
        treeMutable = tree!!.getMutableCopy()
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key("111")])
        Assert.assertNull(treeMutable!![key("11")])
        valueEquals("1", treeMutable!![key("1")])
    }

    @Test
    fun testDeleteNotExistingKey() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "1"))
        Assert.assertEquals(1, treeMutable!!.size())
        Assert.assertEquals(value("1"), treeMutable!![key(1)])
        Assert.assertFalse(treeMutable!!.delete(key(-1)))
        Assert.assertFalse(treeMutable!!.delete(key(-2)))
        Assert.assertFalse(treeMutable!!.delete(key(-3)))
        Assert.assertFalse(treeMutable!!.delete(key(2)))
        Assert.assertTrue(treeMutable!!.delete(key(1)))
        Assert.assertFalse(treeMutable!!.delete(key(1)))
        Assert.assertFalse(treeMutable!!.delete(key(-1)))
        Assert.assertFalse(treeMutable!!.delete(key(-2)))
        Assert.assertEquals(0, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key(1)])
        val a = saveTree()
        reopen()
        tree = openTree(a, false)
        Assert.assertEquals(0, tree!!.size())
        Assert.assertNull(tree!![key(1)])
        Assert.assertNull(tree!![key(-1)])
        Assert.assertNull(tree!![key(2)])
    }

    @Test
    fun testDeleteNotExistingKey2() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "1"))
        treeMutable!!.put(kv(11, "1"))
        treeMutable!!.put(kv(111, "1"))
        Assert.assertEquals(3, treeMutable!!.size())
        Assert.assertEquals(value("1"), treeMutable!![key(1)])
        Assert.assertFalse(treeMutable!!.delete(key(-1)))
        Assert.assertFalse(treeMutable!!.delete(key(-2)))
        Assert.assertFalse(treeMutable!!.delete(key(-3)))
        Assert.assertFalse(treeMutable!!.delete(key(2)))
        Assert.assertTrue(treeMutable!!.delete(key(1)))
        Assert.assertFalse(treeMutable!!.delete(key(1)))
        Assert.assertTrue(treeMutable!!.delete(key(11)))
        Assert.assertFalse(treeMutable!!.delete(key(11)))
        Assert.assertTrue(treeMutable!!.delete(key(111)))
        Assert.assertFalse(treeMutable!!.delete(key(111)))
        Assert.assertFalse(treeMutable!!.delete(key(-1)))
        Assert.assertFalse(treeMutable!!.delete(key(-2)))
        Assert.assertEquals(0, treeMutable!!.size())
        Assert.assertNull(treeMutable!![key(1)])
        val a = saveTree()
        reopen()
        tree = openTree(a, false)
        Assert.assertEquals(0, tree!!.size())
        Assert.assertNull(tree!![key(1)])
        Assert.assertNull(tree!![key(-1)])
        Assert.assertNull(tree!![key(2)])
    }

    @Test
    fun testPutDeleteRandomWithoutDuplicates() {
        treeMutable = createMutableTree(false, 1)
        val map = IntHashMap<String?>()
        val count = 30000
        val seed = System.nanoTime()
        println("TestPutDeleteRandomWithoutDuplicates seed $seed")
        RANDOM!!.setSeed(seed)
        TestUtil.time("Put took ") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                val value = i.toString()
                treeMutable!!.put(key(key.toString()), value(value))
                map.put(key, value)
            }
        }
        var address = saveTree()
        reopen()
        tree = openTree(address, false)
        treeMutable = tree!!.getMutableCopy()
        TestUtil.time("Delete took ") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                Assert.assertEquals(
                    map.remove(key) != null,
                    treeMutable!!.delete(key(key.toString()))
                )
            }
        }
        address = saveTree()
        reopen()
        tree = openTree(address, false)
        Assert.assertEquals(map.size.toLong(), tree!!.size())
        TestUtil.time("Get took ") {
            for ((key, value) in map) {
                valueEquals(value, tree!![key(key.toString())])
            }
        }
        treeMutable = tree!!.getMutableCopy()
        TestUtil.time("Missing get took ") {
            for (i in 0 until count) {
                val key: Int = abs(RANDOM!!.nextInt())
                if (!map.containsKey(key)) {
                    Assert.assertFalse(treeMutable!!.delete(key(key.toString())))
                }
            }
        }
    }
}
