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

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure
import jetbrains.exodus.core.dataStructures.persistent.AbstractPersistentHashSet.TableNode
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import java.util.*
import java.util.concurrent.CountDownLatch

class PersistentHashSetTest {
    @Test
    fun mutableSetRandomInsertDeleteTest() {
        val random = Random(2343489)
        val tree = PersistentHashSet<Int>().beginWrite()
        tree.checkTip()
        var p = genPermutation(random)
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            val key = p[i]
            tree.add(key)
            val root: TableNode<Int> = tree.root
            Assert.assertFalse(tree.isEmpty)
            tree.checkTip()
            tree.add(key)
            Assert.assertNotSame(root, tree.root)
            tree.checkTip()
            for (j in 0..10) {
                val testKey = p[i * j / 10]
                Assert.assertTrue(tree.contains(testKey))
                if (i < ENTRIES_TO_ADD - 1) {
                    Assert.assertFalse(tree.contains(p[i + 1]))
                }
            }
        }
        p = genPermutation(random)
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals((ENTRIES_TO_ADD - i).toLong(), size.toLong())
            Assert.assertFalse(tree.isEmpty)
            val key = p[i]
            Assert.assertTrue(tree.remove(key))
            tree.checkTip()
            Assert.assertFalse(tree.remove(key))
            tree.checkTip()
            for (j in 0..10) {
                val testKey = p[i * j / 10]
                Assert.assertFalse(tree.contains(testKey))
                if (i < ENTRIES_TO_ADD - 1) {
                    Assert.assertTrue(tree.contains(p[i + 1]))
                }
            }
        }
        Assert.assertEquals(0, tree.size().toLong())
        Assert.assertTrue(tree.isEmpty)
        Assert.assertTrue(tree.endWrite())
    }

    @Test
    fun competingWritesTest() {
        val tree = PersistentHashSet<Int>()
        val write1 = tree.beginWrite()
        val write2 = tree.beginWrite()
        write1.add(0)
        write2.remove(1)
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
    }

    @Test
    fun iterationTest() {
        val random = Random(8234890)
        val tree = PersistentHashSet<Int>().beginWrite()
        val p = genPermutation(random)
        val added = HashSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                val actual: MutableCollection<Int> = HashSet(size)
                for (key in tree) {
                    Assert.assertFalse(actual.contains(key))
                    actual.add(key)
                }
                Assert.assertEquals(size.toLong(), actual.size.toLong())
                for (key in added) {
                    Assert.assertTrue(actual.contains(key))
                }
                val treeItr: Iterator<Int> = tree.iterator()
                actual.clear()
                for (j in 0 until size) {
                    val key = treeItr.next()
                    Assert.assertFalse(actual.contains(key))
                    actual.add(key)
                }
                Assert.assertEquals(size.toLong(), actual.size.toLong())
                for (key in added) {
                    Assert.assertTrue(actual.contains(key))
                }
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun forEachKeyTest() {
        val random = Random(8234890)
        val tree = PersistentHashSet<Int>().beginWrite()
        val p = genPermutation(random)
        val added = HashSet<Int>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = tree.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                val actual: MutableCollection<Int> = HashSet(size)
                val proc = ObjectProcedure<Int> { `object`: Int ->
                    Assert.assertFalse(actual.contains(`object`))
                    actual.add(`object`)
                    true
                }
                tree.forEachKey(proc)
                Assert.assertEquals(size.toLong(), actual.size.toLong())
                for (key in added) {
                    Assert.assertTrue(actual.contains(key))
                }
            }
            tree.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun testRootCollision() {
        val source = PersistentHashSet<Any>()
        val writeable = source.beginWrite()
        writeable.add(createClashingHashCodeObject())
        writeable.add(createClashingHashCodeObject())
        Assert.assertEquals(2, writeable.size().toLong())
        writeable.endWrite()
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSizeAtomicity() {
        val source = PersistentHashSet<Int>()
        val latch = CountDownLatch(2)
        val itr = 10000
        val errors: MutableList<Throwable> = LinkedList()
        val writer = Thread {
            try {
                latch.countDown()
                var even = true
                for (i in 0 until itr) {
                    val tree = source.beginWrite()
                    even = if (even) {
                        tree.add(1)
                        tree.add(2)
                        false
                    } else {
                        tree.remove(1)
                        tree.remove(2)
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
                latch.countDown()
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

    @Test
    fun savesObject() {
        val set = PersistentHashSet<String>()
        val mutableSet = set.beginWrite()
        val e = "271828"
        mutableSet.add(e)
        mutableSet.endWrite()
        Assert.assertSame(e, set.beginRead().getKey("271828"))
    }

    companion object {
        private const val ENTRIES_TO_ADD = 5000
        private fun createClashingHashCodeObject(): Any {
            return object : Any() {
                override fun hashCode(): Int {
                    return 0xFFFFF
                }
            }
        }

        private fun genPermutation(random: Random): IntArray {
            val p = IntArray(ENTRIES_TO_ADD)
            for (i in 1 until ENTRIES_TO_ADD) {
                val j = random.nextInt(i)
                p[i] = p[j]
                p[j] = i
            }
            return p
        }
    }
}
