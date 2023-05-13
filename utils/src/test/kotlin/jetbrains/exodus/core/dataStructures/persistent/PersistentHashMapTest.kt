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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test

class PersistentHashMapTest {
    @Test
    fun mutableTreeRandomInsertDeleteTest() {
        val random = Random(2343489)
        val map = PersistentHashMap<Int, String>()
        checkInsertRemove(random, map, 100)
        checkInsertRemove(random, map, ENTRIES_TO_ADD)
        for (i in 0..99) {
            checkInsertRemove(random, map, 100)
        }
    }

    @Test
    fun hashKeyCollision() {
        val map = PersistentHashMap<HashKey, String>()
        var w = map.beginWrite()
        val first = HashKey(1)
        w.put(first, "a")
        val second = HashKey(1)
        w.put(second, "b")
        w.endWrite()
        Assert.assertEquals(2, map.current.size().toLong())
        w = map.beginWrite()
        w.removeKey(first)
        w.endWrite()
        Assert.assertEquals(1, map.current.size().toLong())
    }

    @Test
    fun competingWritesTest() {
        val tree = PersistentHashMap<Int, String>()
        val write1 = tree.beginWrite()
        val write2 = tree.beginWrite()
        write1.put(0, "0")
        write2.removeKey(1)
        Assert.assertTrue(write2.endWrite())
        Assert.assertTrue(write1.endWrite())
        var read = tree.current
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertFalse(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(1, read.size().toLong())
        write1.put(2, "2")
        write2.put(3, "3")
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertFalse(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(1, read.size().toLong())
        read = tree.current
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertTrue(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(2, read.size().toLong())
        var root: Any = write1.getRoot()
        write1.put(2, "2")
        Assert.assertNotSame(write1.getRoot(), root)
        root = write2.getRoot()
        write2.put(2, "_2")
        Assert.assertNotSame(write2.getRoot(), root)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        read = tree.current
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertTrue(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(2, read.size().toLong())
    }

    @Test
    fun testOverwrite() {
        val tree = PersistentHashMap<Int, String>()
        var mutable = tree.beginWrite()
        mutable.put(0, "0")
        Assert.assertTrue(mutable.endWrite())
        Assert.assertEquals("0", tree.current[0])
        mutable = tree.beginWrite()
        mutable.put(0, "0.0")
        Assert.assertTrue(mutable.endWrite())
        Assert.assertEquals("0.0", tree.current[0])
    }

    private inner class HashKey(private val hashCode: Int) {
        // equals isn't overriden intentionally (default is identity comparison) to emulate hash collision
        override fun hashCode(): Int {
            return hashCode
        }
    }

    companion object {
        private const val ENTRIES_TO_ADD = 5000
        private fun checkInsertRemove(random: Random, map: PersistentHashMap<Int, String>, count: Int) {
            val write = map.beginWrite()
            write.checkTip()
            addEntries(random, write, count)
            removeEntries(random, write, count)
            Assert.assertEquals(0, write.size().toLong())
            Assert.assertTrue(write.isEmpty())
            Assert.assertTrue(write.endWrite())
        }

        private fun addEntries(random: Random, tree:  PersistentHashMap<Int, String>.MutablePersistentHashMap, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size: Int = tree.size()
                Assert.assertEquals(i.toLong(), size.toLong())
                val key = p[i]
                tree.put(key, "$key ")
                Assert.assertFalse(tree.isEmpty())
                tree.checkTip()
                Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
                tree.put(key, key.toString())
                tree.checkTip()
                Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
                for (j in 0..10) {
                    val testKey = p[i * j / 10]
                    Assert.assertTrue(tree.containsKey(testKey))
                }
                if (i < count - 1) {
                    Assert.assertFalse(tree.containsKey(p[i + 1]))
                }
            }
        }

        private fun removeEntries(random: Random, tree:  PersistentHashMap<Int, String>.MutablePersistentHashMap, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size: Int = tree.size()
                Assert.assertEquals((count - i).toLong(), size.toLong())
                Assert.assertFalse(tree.isEmpty())
                val key = p[i]
                Assert.assertEquals(key.toString(), tree.removeKey(key))
                tree.checkTip()
                Assert.assertNull(tree.removeKey(key))
                tree.checkTip()
                for (j in 0..10) {
                    val testKey = p[i * j / 10]
                    Assert.assertFalse(tree.containsKey(testKey))
                }
                if (i < count - 1) {
                    Assert.assertTrue(tree.containsKey(p[i + 1]))
                }
            }
        }

        private fun genPermutation(random: Random, size: Int): IntArray {
            val p = IntArray(size)
            for (i in 1 until size) {
                val j = random.nextInt(i)
                p[i] = p[j]
                p[j] = i
            }
            return p
        }
    }
}
