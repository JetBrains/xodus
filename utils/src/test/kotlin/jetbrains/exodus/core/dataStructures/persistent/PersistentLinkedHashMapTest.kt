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

import jetbrains.exodus.core.dataStructures.persistent.PersistentLinkedHashMap.PersistentLinkedHashMapMutable
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test

class PersistentLinkedHashMapTest {
    @Test
    fun mutableTreeRandomInsertDeleteTest() {
        val random = Random(2343489)
        val map = PersistentLinkedHashMap<Int, String>()
        checkInsertRemove(random, map, 100)
        checkInsertRemove(random, map, ENTRIES_TO_ADD)
        for (i in 0..99) {
            checkInsertRemove(random, map, 100)
        }
    }

    @Test
    fun testOverwrite() {
        val tree = PersistentLinkedHashMap<Int, String>()
        var mutable = tree.beginWrite()
        mutable.put(0, "0")
        Assert.assertTrue(tree.endWrite(mutable))
        Assert.assertEquals("0", tree.beginWrite()[0])
        mutable = tree.beginWrite()
        mutable.put(0, "0.0")
        Assert.assertTrue(tree.endWrite(mutable))
        Assert.assertEquals("0.0", tree.beginWrite()[0])
    }

    companion object {
        private const val ENTRIES_TO_ADD = 5000
        private fun checkInsertRemove(random: Random, map: PersistentLinkedHashMap<Int, String>, count: Int) {
            val write = map.beginWrite()
            write.checkTip()
            addEntries(random, write, count)
            removeEntries(random, write, count)
            Assert.assertEquals(0, write.size().toLong())
            Assert.assertTrue(write.isEmpty)
            Assert.assertTrue(map.endWrite(write))
        }

        private fun addEntries(random: Random, tree: PersistentLinkedHashMapMutable<Int, String>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals(i.toLong(), size.toLong())
                val key = p[i]
                tree.put(key, "$key ")
                Assert.assertFalse(tree.isEmpty)
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

        private fun removeEntries(random: Random, tree: PersistentLinkedHashMapMutable<Int, String>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals((count - i).toLong(), size.toLong())
                Assert.assertFalse(tree.isEmpty)
                val key = p[i]
                Assert.assertEquals(key.toString(), tree.remove(key))
                tree.checkTip()
                Assert.assertNull(tree.remove(key))
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
