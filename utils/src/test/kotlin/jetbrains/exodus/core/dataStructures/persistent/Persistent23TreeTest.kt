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

import jetbrains.exodus.TestUtil.time
import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.ImmutableTree
import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.MutableTree
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import java.util.*
import java.util.concurrent.CyclicBarrier

class Persistent23TreeTest {
    @Test
    fun mutableTreeRandomInsertDeleteTest() {
        val random = Random(2343489)
        val set = Persistent23Tree<Int>()
        checkInsertRemove(random, set, 100)
        checkInsertRemove(random, set, ENTRIES_TO_ADD)
        for (i in 0..99) {
            checkInsertRemove(random, set, 100)
        }
    }

    @Test
    fun competingWritesTest() {
        val tree = Persistent23Tree<Int>()
        val write1 = tree.beginWrite()
        val write2 = tree.beginWrite()
        write1.add(0)
        write2.exclude(1)
        Assert.assertTrue(write2.endWrite())
        Assert.assertTrue(write1.endWrite())
        var read = tree.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertFalse(read.contains(2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(1, read.size().toLong())
        write1.add(2)
        write2.add(3)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertFalse(read.contains(2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(1, read.size().toLong())
        read = tree.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertTrue(read.contains(2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(2, read.size().toLong())
        val root: AbstractPersistent23Tree.Node<Int> = write1.root
        write1.add(2)
        Assert.assertNotSame(write1.root, root)
        write2.add(3)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        read = tree.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertTrue(read.contains(2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(2, read.size().toLong())
    }

    @Test
    fun iterationTest() {
        val random = Random(8234890)
        val tree = Persistent23Tree<Int>().beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                var iterator: Iterator<Int?> = added.iterator()
                for (key in tree) {
                    Assert.assertTrue(iterator.hasNext())
                    Assert.assertEquals(iterator.next(), key)
                }
                Assert.assertFalse(iterator.hasNext())
                iterator = added.iterator()
                val treeItr: Iterator<Int> = tree.iterator()
                for (j in 0 until size) {
                    val key = treeItr.next()
                    Assert.assertTrue(iterator.hasNext())
                    Assert.assertEquals(iterator.next(), key)
                }
                Assert.assertFalse(iterator.hasNext())
                try {
                    treeItr.next()
                    Assert.fail()
                } catch (e: NoSuchElementException) {
                }
                Assert.assertFalse(treeItr.hasNext())
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun reverseIterationTest() {
        val random = Random(5743)
        val tree = Persistent23Tree<Int>().beginWrite()
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
                        Assert.assertEquals(iterator.next(), key)
                    }
                }
                Assert.assertFalse(iterator.hasNext())
                iterator = added.descendingIterator()
                val treeItr = tree.reverseIterator()
                for (j in 0 until size) {
                    val key = treeItr.next()
                    Assert.assertTrue(iterator.hasNext())
                    Assert.assertEquals(iterator.next(), key)
                }
                Assert.assertFalse(iterator.hasNext())
                try {
                    treeItr.next()
                    Assert.fail()
                } catch (e: NoSuchElementException) {
                }
                Assert.assertFalse(treeItr.hasNext())
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun iterationBenchmark() {
        val tree = Persistent23Tree<Int>().beginWrite()
        val count = 100000
        for (i in 0 until count) {
            tree.add(i)
        }
        time("Persistent23Tree iteration") {
            for (i in 0..299) {
                var prev = Int.MIN_VALUE
                Assert.assertFalse(tree.contains(prev))
                val it: Iterator<Int> = tree.iterator()
                var j = 0
                while (it.hasNext()) {
                    j = it.next()
                    Assert.assertTrue(prev < j)
                    prev = j
                }
                Assert.assertEquals((count - 1).toLong(), j.toLong())
            }
        }
        time("Persistent23Tree reverse iteration") {
            for (i in 0..299) {
                var prev = Int.MAX_VALUE
                Assert.assertFalse(tree.contains(prev))
                val it = tree.reverseIterator()
                var j = 0
                while (it.hasNext()) {
                    j = it.next()
                    Assert.assertTrue(prev > j)
                    prev = j
                }
                Assert.assertEquals(0, j.toLong())
            }
        }
    }

    @Test
    fun tailIterationTest() {
        val random = Random(239786)
        val tree = Persistent23Tree<Int>().beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                if (i > 0) {
                    checkTailIteration(tree, added, added.first())
                    checkTailIteration(tree, added, added.first() - 1)
                    checkTailIteration(tree, added, added.last())
                    checkTailIteration(tree, added, added.last() + 1)
                }
                checkTailIteration(tree, added, Int.MAX_VALUE)
                checkTailIteration(tree, added, Int.MIN_VALUE)
                for (j in 0..9) {
                    checkTailIteration(tree, added, p[i * j / 10])
                }
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun tailReverseIterationTest() {
        val random = Random(239786)
        val tree = Persistent23Tree<Int>().beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                if (i > 0) {
                    checkTailReverseIteration(tree, added, added.first())
                    checkTailReverseIteration(tree, added, added.first() - 1)
                    checkTailReverseIteration(tree, added, added.last())
                    checkTailReverseIteration(tree, added, added.last() + 1)
                }
                checkTailReverseIteration(tree, added, Int.MAX_VALUE)
                checkTailReverseIteration(tree, added, Int.MIN_VALUE)
                for (j in 0..9) {
                    checkTailReverseIteration(tree, added, p[i * j / 10])
                }
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun testAddAll() {
        val source = Persistent23Tree<Int>()
        val count = 1000
        val entries = ArrayList<Int>(count)
        for (i in 0 until count) {
            val tree = source.beginWrite()
            tree.addAll(entries.iterator(), i)
            Assert.assertEquals(i.toLong(), tree.size().toLong())
            var j = 0
            for (key in tree) {
                Assert.assertEquals(j, key)
                j++
            }
            tree.testConsistency()
            entries.add(i)
        }
    }

    @Test
    fun testGetMinimumMaximum() {
        val source = Persistent23Tree<Int>()
        val tree = source.beginWrite()
        for (i in 0 until BENCHMARK_SIZE) {
            tree.add(i)
        }
        Assert.assertTrue(tree.endWrite())
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        println("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()))
        val current = source.beginRead()
        println("Nodes count: " + nodesCount(current))
        val min = current.minimum
        Assert.assertEquals(0, min!!.toLong())
        val max = tree.maximum
        Assert.assertEquals(MAX_KEY.toLong(), max!!.toLong())
    }

    @Test
    fun testGetMinimumMaximumAddAll() {
        val source = Persistent23Tree<Int>()
        val tree = source.beginWrite()
        tree.addAll(object : MutableIterator<Int?> {
            private var current = -1
            override fun hasNext(): Boolean {
                return current + 1 < BENCHMARK_SIZE
            }

            override fun next(): Int {
                return ++current
            }

            override fun remove() {}
        }, BENCHMARK_SIZE)
        Assert.assertTrue(tree.endWrite())
        System.gc()
        print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()))
        val current = source.beginRead()
        val min = current.minimum
        Assert.assertEquals(0, min!!.toLong())
        val max = tree.maximum
        Assert.assertEquals(MAX_KEY.toLong(), max!!.toLong())
        tree.testConsistency()
    }

    @Test
    fun testGetMinimumMaximum2() {
        val source = Persistent23Tree<Int>()
        val tree = source.beginWrite()
        var min = Int.MAX_VALUE
        var max = Int.MIN_VALUE
        val rnd = Random()
        for (i in 0 until BENCHMARK_SIZE) {
            val key = rnd.nextInt()
            tree.add(key)
            if (key > max) {
                max = key
            }
            if (key < min) {
                min = key
            }
        }
        Assert.assertTrue(tree.endWrite())
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()))
        val current = source.beginRead()
        Assert.assertEquals(min.toLong(), current.minimum!!.toLong())
        Assert.assertEquals(max.toLong(), tree.maximum!!.toLong())
        tree.testConsistency()
    }

    @Test
    fun testGetMinimumMaximum3() {
        val source = Persistent23Tree<Int>()
        val tree = source.beginWrite()
        for (i in MAX_KEY downTo 0) {
            tree.add(i)
        }
        Assert.assertTrue(tree.endWrite())
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        System.gc()
        print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()))
        val current = source.beginRead()
        val min = current.minimum
        Assert.assertEquals(0, min!!.toLong())
        val max = tree.maximum
        Assert.assertEquals(MAX_KEY.toLong(), max!!.toLong())
    }

    @Test
    fun testCache() {
        val random = Random(34790)
        val p = genPermutation(random, BENCHMARK_SIZE)
        val source = Persistent23Tree<Int>()
        var tree: MutableTree<Int>? = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    tree!!.endWrite()
                }
                tree = source.beginWrite()
            }
            tree!!.add(p[i])
            if (i >= 4048) {
                tree.exclude(p[i - 4048])
            }
        }
    }

    @Test
    fun testSize() {
        val random = Random(249578)
        var p = genPermutation(random, 10000)
        val source = Persistent23Tree<Int>()
        var tree: MutableTree<Int>? = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    tree!!.endWrite()
                    Assert.assertEquals(i.toLong(), source.size().toLong())
                }
                tree = source.beginWrite()
            }
            Assert.assertEquals(i.toLong(), tree!!.size().toLong())
            tree.add(p[i])
            Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
            for (j in 0..2) {
                tree.add(p[random.nextInt(i + 1)])
                Assert.assertEquals((i + 1).toLong(), tree.size().toLong())
            }
        }
        tree!!.endWrite()
        Assert.assertEquals(p.size.toLong(), source.size().toLong())
        p = genPermutation(random, p.size)
        tree = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    tree!!.endWrite()
                    Assert.assertEquals((p.size - i).toLong(), source.size().toLong())
                }
                tree = source.beginWrite()
            }
            Assert.assertEquals((p.size - i).toLong(), tree!!.size().toLong())
            tree.exclude(p[i])
            Assert.assertEquals((p.size - i - 1).toLong(), tree.size().toLong())
            for (j in 0..2) {
                tree.exclude(p[random.nextInt(i + 1)])
                Assert.assertEquals((p.size - i - 1).toLong(), tree.size().toLong())
            }
        }
        tree!!.endWrite()
        Assert.assertEquals(0, source.size().toLong())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSizeAtomicity() { // for XD-259
        val source = Persistent23Tree<Int>()
        val barrier = CyclicBarrier(2)
        val itr = 10000
        val errors: MutableList<Throwable> = LinkedList()
        val writer = Thread {
            try {
                barrier.await()
                var even = true
                for (i in 0 until itr) {
                    val tree = source.beginWrite()
                    even = if (even) {
                        tree.add(1)
                        tree.add(2)
                        false
                    } else {
                        tree.exclude(1)
                        tree.exclude(2)
                        true
                    }
                    tree.endWrite()
                }
            } catch (t: Throwable) {
                rememberError(errors, t)
            }
        }
        val reader = Thread {
            try {
                barrier.await()
                for (i in 0 until itr) {
                    val tree = source.beginRead()
                    var size = 0
                    for (ignored in tree) {
                        size++
                    }
                    Assert.assertEquals("at reader iteration $i", size.toLong(), tree.size().toLong())
                }
            } catch (t: Throwable) {
                rememberError(errors, t)
            }
        }
        writer.start()
        reader.start()
        writer.join()
        reader.join()
        for (t in errors) {
            t.printStackTrace()
        }
        Assert.assertTrue(errors.isEmpty())
    }

    private fun rememberError(errors: MutableList<Throwable>, t: Throwable) {
        synchronized(errors) { errors.add(t) }
    }

    companion object {
        private const val BENCHMARK_SIZE = 2000000
        private const val MAX_KEY = BENCHMARK_SIZE - 1
        const val ENTRIES_TO_ADD = 5000
        private fun checkTailIteration(tree: MutableTree<Int>, added: SortedSet<Int>, first: Int) {
            val iterator: Iterator<Int> = added.tailSet(first).iterator()
            val treeItr = tree.tailIterator(first)
            while (treeItr.hasNext()) {
                Assert.assertTrue(iterator.hasNext())
                val key = treeItr.next()
                Assert.assertEquals(iterator.next(), key)
            }
            Assert.assertFalse(iterator.hasNext())
        }

        private fun checkTailReverseIteration(tree: MutableTree<Int>, added: SortedSet<Int>, first: Int) {
            val iterator = (added.tailSet(first) as NavigableSet<Int>).descendingIterator()
            val treeItr = tree.tailReverseIterator(first)
            while (treeItr.hasNext()) {
                Assert.assertTrue(iterator.hasNext())
                val key = treeItr.next()
                Assert.assertEquals(iterator.next(), key)
            }
            Assert.assertFalse(iterator.hasNext())
        }

        private fun checkInsertRemove(random: Random, set: Persistent23Tree<Int>, count: Int) {
            val tree = set.beginWrite()
            tree.testConsistency()
            addEntries(random, tree, count)
            removeEntries(random, tree, count)
            Assert.assertEquals(0, tree.size().toLong())
            Assert.assertTrue(tree.isEmpty)
            Assert.assertTrue(tree.endWrite())
        }

        private fun addEntries(random: Random, tree: MutableTree<Int>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals(i.toLong(), size.toLong())
                val key = p[i]
                tree.add(key)
                Assert.assertFalse(tree.isEmpty)
                tree.testConsistency()
                tree.add(key)
                tree.testConsistency()
                for (j in 0..10) {
                    val testKey = p[i * j / 10]
                    Assert.assertTrue(tree.contains(testKey))
                }
                if (i < count - 1) {
                    Assert.assertFalse(tree.contains(p[i + 1]))
                }
            }
        }

        private fun removeEntries(random: Random, tree: MutableTree<Int>, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = tree.size()
                Assert.assertEquals((count - i).toLong(), size.toLong())
                Assert.assertFalse(tree.isEmpty)
                val key = p[i]
                Assert.assertTrue(tree.exclude(key))
                tree.testConsistency()
                Assert.assertFalse(tree.exclude(key))
                tree.testConsistency()
                for (j in 0..10) {
                    val testKey = p[i * j / 10]
                    Assert.assertFalse(tree.contains(testKey))
                }
                if (i < count - 1) {
                    Assert.assertTrue(tree.contains(p[i + 1]))
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

        private fun nodesCount(tree: ImmutableTree<Int>): Int {
            return nodesCount(tree.root)
        }

        private fun nodesCount(node: AbstractPersistent23Tree.Node<Int>): Int {
            if (node.isLeaf) {
                return 1
            }
            return if (node.isTernary) {
                nodesCount(node.firstChild) + nodesCount(
                    node.secondChild
                ) + nodesCount(node.thirdChild)
            } else nodesCount(node.firstChild) + nodesCount(
                node.secondChild
            )
        }
    }
}
