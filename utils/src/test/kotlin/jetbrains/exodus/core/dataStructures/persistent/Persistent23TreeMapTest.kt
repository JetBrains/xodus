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
import java.util.*

class Persistent23TreeMapTest {
    @Test
    fun mutableTreeRandomInsertDeleteTest() {
        val random = Random(2343489)
        val map = Persistent23TreeMap<Int, String>()
        checkInsertRemove(random, map, 100)
        checkInsertRemove(random, map, ENTRIES_TO_ADD)
        for (i in 0..99) {
            checkInsertRemove(random, map, 100)
        }
    }

    @Test
    fun competingWritesTest() {
        val tree = Persistent23TreeMap<Int, String>()
        val write1 = tree.beginWrite()
        val write2 = tree.beginWrite()
        write1.put(0, "0")
        write2.remove(1)
        Assert.assertTrue(write2.endWrite())
        Assert.assertTrue(write1.endWrite())
        var read = tree.beginRead()
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
        read = tree.beginRead()
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertTrue(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(2, read.size().toLong())
        var root: Any? = write1.root
        write1.put(2, "2")
        Assert.assertNotSame(write1.root, root)
        root = write2.root
        write2.put(2, "_2")
        Assert.assertNotSame(write2.root, root)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        read = tree.beginRead()
        Assert.assertTrue(read.containsKey(0))
        Assert.assertFalse(read.containsKey(1))
        Assert.assertTrue(read.containsKey(2))
        Assert.assertFalse(read.containsKey(3))
        Assert.assertEquals(2, read.size().toLong())
    }

    @Test
    fun iterationTest() {
        val random = Random(8234890)
        val map = Persistent23TreeMap<Int, String>()
        val write = map.beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = write.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                var iterator: Iterator<Int> = added.iterator()
                for (key in write) {
                    Assert.assertTrue(iterator.hasNext())
                    val next = iterator.next()
                    Assert.assertEquals(next, key.key)
                    Assert.assertEquals(next.toString(), key.value)
                }
                Assert.assertFalse(iterator.hasNext())
                iterator = added.iterator()
                val treeItr: Iterator<Persistent23TreeMap.Entry<Int, String>> = write.iterator()
                for (j in 0 until size) {
                    val key = treeItr.next()
                    Assert.assertTrue(iterator.hasNext())
                    val next = iterator.next()
                    Assert.assertEquals(next, key.key)
                    Assert.assertEquals(next.toString(), key.value)
                }
                Assert.assertFalse(iterator.hasNext())
                try {
                    treeItr.next()
                    Assert.fail()
                } catch (e: NoSuchElementException) {
                }
                Assert.assertFalse(treeItr.hasNext())
            }
            write.put(p[i], p[i].toString())
            added.add(p[i])
        }
    }

    @Test
    fun reverseIterationTest() {
        val random = Random(5743)
        val tree = Persistent23TreeMap<Int, String>().beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                var iterator = added.descendingIterator()
                run {
                    val treeItr = tree.reverseIterator()
                    while (treeItr.hasNext()) {
                        Assert.assertTrue(iterator.hasNext())
                        val key = treeItr.next()
                        val next = iterator.next()
                        Assert.assertEquals(next, key.key)
                        Assert.assertEquals(next.toString(), key.value)
                    }
                }
                Assert.assertFalse(iterator.hasNext())
                iterator = added.descendingIterator()
                val treeItr = tree.reverseIterator()
                for (j in 0 until size) {
                    val key = treeItr.next()
                    Assert.assertTrue(iterator.hasNext())
                    val next = iterator.next()
                    Assert.assertEquals(next, key.key)
                    Assert.assertEquals(next.toString(), key.value)
                }
                Assert.assertFalse(iterator.hasNext())
                try {
                    treeItr.next()
                    Assert.fail()
                } catch (e: NoSuchElementException) {
                }
                Assert.assertFalse(treeItr.hasNext())
            }
            tree.put(p[i], p[i].toString())
            added.add(p[i])
        }
    }

    @Test
    fun tailIterationTest() {
        val random = Random(239786)
        val map = Persistent23TreeMap<Int, String>()
        val write = map.beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = write.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                if (i > 0) {
                    checkTailIteration(write, added, map.createEntry(added.first()))
                    checkTailIteration(write, added, map.createEntry(added.first() - 1))
                    checkTailIteration(write, added, map.createEntry(added.last()))
                    checkTailIteration(write, added, map.createEntry(added.last() + 1))
                }
                checkTailIteration(write, added, map.createEntry(Int.MAX_VALUE))
                checkTailIteration(write, added, map.createEntry(Int.MIN_VALUE))
                for (j in 0..9) {
                    checkTailIteration(write, added, map.createEntry(p[i * j / 10]))
                }
            }
            write.put(p[i], p[i].toString())
            added.add(p[i])
        }
    }

    @Test
    fun tailReverseIterationTest() {
        val random = Random(239786)
        val map = Persistent23TreeMap<Int, String>()
        val write = map.beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = write.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                if (i > 0) {
                    checkTailReverseIteration(write, added, map.createEntry(added.first()))
                    checkTailReverseIteration(write, added, map.createEntry(added.first() - 1))
                    checkTailReverseIteration(write, added, map.createEntry(added.last()))
                    checkTailReverseIteration(write, added, map.createEntry(added.last() + 1))
                }
                checkTailReverseIteration(write, added, map.createEntry(Int.MAX_VALUE))
                checkTailReverseIteration(write, added, map.createEntry(Int.MIN_VALUE))
                for (j in 0..9) {
                    checkTailReverseIteration(write, added, map.createEntry(p[i * j / 10]))
                }
            }
            write.put(p[i], p[i].toString())
            added.add(p[i])
        }
    }

    @Test
    fun testSize() {
        val random = Random(249578)
        var p = genPermutation(random, 10000)
        val source = Persistent23TreeMap<Int, String>()
        var tree: Persistent23TreeMap.MutableMap<Int, String>? = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    tree!!.endWrite()
                    Assert.assertEquals(i.toLong(), source.beginRead().size().toLong())
                }
                tree = source.beginWrite()
            }
            Assert.assertEquals(i.toLong(), tree!!.size().toLong())
            tree.put(p[i], p[i].toString())
            Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
            for (j in 0..2) {
                tree.put(p[random.nextInt(i + 1)], p[random.nextInt(i + 1)].toString() + " " + i + " " + j)
                Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
            }
        }
        tree!!.endWrite()
        Assert.assertEquals(p.size.toLong(), source.beginRead().size().toLong())
        p = genPermutation(random, p.size)
        tree = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    tree!!.endWrite()
                    Assert.assertEquals((p.size - i).toLong(), source.beginRead().size().toLong())
                }
                tree = source.beginWrite()
            }
            Assert.assertEquals((p.size - i).toLong(), tree!!.size().toLong())
            tree.remove(p[i])
            Assert.assertEquals((p.size - i - 1).toLong(), tree.size().toLong())
            for (j in 0..2) {
                tree.remove(p[random.nextInt(i + 1)])
                Assert.assertEquals((p.size - i - 1).toLong(), tree.size().toLong())
            }
        }
        tree!!.endWrite()
        Assert.assertEquals(0, source.beginRead().size().toLong())
    }

    companion object {
        private const val ENTRIES_TO_ADD = 5000
        private fun checkTailIteration(
            tree: Persistent23TreeMap.MutableMap<Int, String>,
            added: SortedSet<Int>,
            first: Persistent23TreeMap.Entry<Int, String>
        ) {
            val iterator: Iterator<Int> = added.tailSet(first.key).iterator()
            val treeItr = tree.tailIterator(first)
            while (treeItr.hasNext()) {
                Assert.assertTrue(iterator.hasNext())
                val entry = treeItr.next()
                val next = iterator.next()
                Assert.assertEquals(next, entry.key)
                Assert.assertEquals(next.toString(), entry.value)
            }
            Assert.assertFalse(iterator.hasNext())
        }

        private fun checkTailReverseIteration(
            tree: Persistent23TreeMap.MutableMap<Int, String>,
            added: SortedSet<Int>,
            first: Persistent23TreeMap.Entry<Int, String>
        ) {
            val iterator = (added.tailSet(first.key) as NavigableSet<Int>).descendingIterator()
            val treeItr = tree.tailReverseIterator(first)
            while (treeItr.hasNext()) {
                Assert.assertTrue(iterator.hasNext())
                val entry = treeItr.next()
                val next = iterator.next()
                Assert.assertEquals(next, entry.key)
                Assert.assertEquals(next.toString(), entry.value)
            }
            Assert.assertFalse(iterator.hasNext())
        }

        private fun checkInsertRemove(random: Random, map: Persistent23TreeMap<Int, String>, count: Int) {
            val write = map.beginWrite()
            write.testConsistency()
            addEntries(random, write, count)
            removeEntries(random, write, count)
            Assert.assertEquals(0, write.size().toLong())
            Assert.assertTrue(write.isEmpty)
            Assert.assertTrue(write.endWrite())
        }

        private fun addEntries(random: Random, tree: Persistent23TreeMap.MutableMap<Int, String>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals(i.toLong(), size.toLong())
                val key = p[i]
                tree.put(key, "$key ")
                Assert.assertFalse(tree.isEmpty)
                tree.testConsistency()
                Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
                tree.put(key, key.toString())
                tree.testConsistency()
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

        private fun removeEntries(random: Random, tree: Persistent23TreeMap.MutableMap<Int, String>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals((count - i).toLong(), size.toLong())
                Assert.assertFalse(tree.isEmpty)
                val key = p[i]
                Assert.assertEquals(key.toString(), tree.remove(key))
                tree.testConsistency()
                Assert.assertNull(tree.remove(key))
                tree.testConsistency()
                for (j in 0..10) {
                    val testKey = p[i * j / 10]
                    Assert.assertFalse(tree.containsKey(testKey))
                }
                if (i < count - 1) {
                    Assert.assertTrue(tree.containsKey(p[i + 1]))
                }
            }
        }

        private fun genPermutation(random: Random, size: Int = ENTRIES_TO_ADD): IntArray {
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
