/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

open class PersistentBitTreeLongSetTest {

    @Test
    fun testEmpty() {
        val set = createSet()
        Assert.assertFalse(set.beginRead().contains(0))
        Assert.assertFalse(set.beginRead().contains(-1))
        Assert.assertFalse(set.beginRead().contains(-2))
        Assert.assertFalse(set.beginRead().contains(3))
        Assert.assertEquals(0, set.beginRead().size().toLong())
        Assert.assertFalse(set.beginRead().longIterator().hasNext())
    }

    @Test
    fun testClear() {
        val set = createFilledSet()
        Assert.assertEquals(100000, set.read { size() })
        set.write {
            clear()
        }
        Assert.assertEquals(0, set.read { size() })
        Assert.assertFalse(set.read { longIterator().hasNext() })
        Assert.assertFalse(set.read { reverseLongIterator().hasNext() })
    }

    @Test
    fun testLongIterator() {
        val set = createFilledSet()
        set.read {
            val iterator = longIterator()
            for (i in 0L..99999L) {
                Assert.assertTrue(iterator.hasNext())
                Assert.assertEquals(i, iterator.nextLong())
            }
            Assert.assertFalse(iterator.hasNext())
        }
        set.write {
            val iterator = longIterator()
            for (i in 0L..99999L) {
                Assert.assertTrue(iterator.hasNext())
                Assert.assertEquals(i, iterator.nextLong())
            }
            Assert.assertFalse(iterator.hasNext())
        }
    }

    @Test
    fun testReverseLongIterator() {
        val set = createFilledSet()
        set.read {
            val iterator = reverseLongIterator()
            for (i in 99999L downTo 0L) {
                Assert.assertTrue(iterator.hasNext())
                Assert.assertEquals(i, iterator.nextLong())
            }
            Assert.assertFalse(iterator.hasNext())
        }
        set.write {
            val iterator = reverseLongIterator()
            for (i in 99999L downTo 0L) {
                Assert.assertTrue(iterator.hasNext())
                Assert.assertEquals(i, iterator.nextLong())
            }
            Assert.assertFalse(iterator.hasNext())
        }
    }

    @Test
    fun testTailLongIterator() {
        val set = createFilledSet()
        set.read {
            for (i in 0L..99999L step 777) {
                val it = tailLongIterator(i)
                for (j in i..99999L) {
                    Assert.assertTrue(it.hasNext())
                    Assert.assertEquals(j, it.nextLong())
                }
                Assert.assertFalse(it.hasNext())
            }
        }
        set.write {
            for (i in 0L..99999L step 777) {
                val it = tailLongIterator(i)
                for (j in i..99999L) {
                    Assert.assertTrue(it.hasNext())
                    Assert.assertEquals(j, it.nextLong())
                }
                Assert.assertFalse(it.hasNext())
            }
        }
    }

    @Test
    fun testTailReverseLongIterator() {
        val set = createFilledSet()
        set.read {
            for (i in 0L..99999L step 777) {
                val it = tailReverseLongIterator(i)
                for (j in 99999L downTo i) {
                    if (!it.hasNext()) {
                        println("i = $i, j = $j")
                    }
                    Assert.assertTrue(it.hasNext())
                    Assert.assertEquals(j, it.nextLong())
                }
                Assert.assertFalse(it.hasNext())
            }
        }
        set.write {
            for (i in 0L..99999L step 777) {
                val it = tailReverseLongIterator(i)
                for (j in 99999L downTo i) {
                    Assert.assertTrue(it.hasNext())
                    Assert.assertEquals(j, it.nextLong())
                }
                Assert.assertFalse(it.hasNext())
            }
        }
    }

    @Test
    fun mutableTreeRandomInsertDeleteTest() {
        val random = Random(2343489)
        val set = createSet()
        checkInsertRemove(random, set, 100)
        checkInsertRemove(random, set, ENTRIES_TO_ADD)
        for (i in 0..99) {
            checkInsertRemove(random, set, 100)
        }
    }

    @Test
    fun competingWritesTest() {
        val set = createSet()
        val write1 = set.beginWrite()
        val write2 = set.beginWrite()
        write1.add(0)
        write2.remove(1)
        Assert.assertTrue(write2.endWrite())
        Assert.assertTrue(write1.endWrite())
        var read: PersistentLongSet.ImmutableSet = set.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertFalse(read.contains(-2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(1, read.size().toLong())

        write1.add(-2)
        write2.add(3)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertFalse(read.contains(-2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(1, read.size().toLong())
        read = set.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertTrue(read.contains(-2))
        Assert.assertFalse(read.contains(3))
        Assert.assertEquals(2, read.size().toLong())

        write1.add(3)
        write2.add(-2)
        Assert.assertTrue(write1.endWrite())
        Assert.assertFalse(write2.endWrite())
        read = set.beginRead()
        Assert.assertTrue(read.contains(0))
        Assert.assertFalse(read.contains(1))
        Assert.assertTrue(read.contains(-2))
        Assert.assertFalse(read.contains(4))
        Assert.assertEquals(3, read.size().toLong())
    }

    @Test
    fun iterationTest() {
        val random = Random(8234890)
        val set = createSet()
        val write = set.beginWrite()
        val p = genPermutation(random)
        val added = TreeSet<Long>()
        for (i in 0 until ENTRIES_TO_ADD) {
            val size = write.size()
            Assert.assertEquals(i.toLong(), size.toLong())
            if (size and 1023 == 0 || size < 100) {
                var iterator = added.iterator()
                var setItr = write.longIterator()
                while (setItr.hasNext()) {
                    Assert.assertTrue(iterator.hasNext())
                    val next = iterator.next()
                    Assert.assertEquals(next, setItr.next())
                }
                Assert.assertFalse(iterator.hasNext())

                iterator = added.iterator()
                setItr = write.longIterator()
                for (j in 0 until size) {
                    val key = setItr.next()
                    Assert.assertTrue(iterator.hasNext())
                    val next = iterator.next()
                    Assert.assertEquals(next, key)
                }
                Assert.assertFalse(iterator.hasNext())
                try {
                    setItr.next()
                    Assert.fail()
                } catch (e: NoSuchElementException) {
                }

                Assert.assertFalse(setItr.hasNext())
            }
            write.add(p[i])
            added.add(p[i])
        }
    }

    @Test
    fun testSize() {
        val random = Random(249578)
        var p = genPermutation(random, ENTRIES_TO_ADD)
        val source = createSet()
        var set: PersistentLongSet.MutableSet? = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    set!!.endWrite()
                    Assert.assertEquals(i.toLong(), source.beginRead().size().toLong())
                }
                set = source.beginWrite()
            }
            Assert.assertEquals(i.toLong(), set!!.size().toLong())
            set.add(p[i])
            Assert.assertEquals((i + 1).toLong(), set.size().toLong())
            for (j in 0..2) {
                set.add(p[random.nextInt(i + 1)])
                Assert.assertEquals((i + 1).toLong(), set.size().toLong())
            }
        }
        set!!.endWrite()
        Assert.assertEquals(p.size.toLong(), source.beginRead().size().toLong())

        p = genPermutation(random, p.size)
        set = null
        for (i in p.indices) {
            if (i and 15 == 0) {
                if (i > 0) {
                    set!!.endWrite()
                    Assert.assertEquals((p.size - i).toLong(), source.beginRead().size().toLong())
                }
                set = source.beginWrite()
            }
            Assert.assertEquals((p.size - i).toLong(), set!!.size().toLong())
            set.remove(p[i])
            Assert.assertEquals((p.size - i - 1).toLong(), set.size().toLong())
            for (j in 0..2) {
                set.remove(p[random.nextInt(i + 1)])
                Assert.assertEquals((p.size - i - 1).toLong(), set.size().toLong())
            }
        }
        set!!.endWrite()
        Assert.assertEquals(0, source.beginRead().size().toLong())
    }

    protected open fun createSet(): PersistentLongSet {
        return PersistentBitTreeLongSet(7)
    }

    private fun createFilledSet(): PersistentLongSet {
        return createSet().apply {
            write {
                for (i in 0L..99999L) {
                    add(i)
                }
            }
        }
    }

    companion object {

        private val ENTRIES_TO_ADD = 5000

        private fun checkInsertRemove(random: Random, set: PersistentLongSet, count: Int) {
            val write = set.beginWrite()
            addEntries(random, write, count)
            removeEntries(random, write, count)
            Assert.assertEquals(0, write.size().toLong())
            Assert.assertTrue(write.isEmpty)
            Assert.assertTrue(write.endWrite())
        }

        private fun addEntries(random: Random, set: PersistentLongSet.MutableSet, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = set.size()
                Assert.assertEquals(i.toLong(), size.toLong())
                val key = p[i]
                set.add(key)
                Assert.assertFalse(set.isEmpty)
                Assert.assertEquals((i + 1).toLong(), set.size().toLong())
                set.add(key)
                Assert.assertEquals((i + 1).toLong(), set.size().toLong())
                (0..10)
                        .map { p[i * it / 10] }
                        .forEach { Assert.assertTrue(set.contains(it)) }
                if (i < count - 1) {
                    Assert.assertFalse(set.contains(p[i + 1]))
                }
            }
        }

        private fun removeEntries(random: Random, set: PersistentLongSet.MutableSet, count: Int) {
            val p = genPermutation(random, count)
            for (i in 0 until count) {
                val size = set.size()
                Assert.assertEquals((count - i).toLong(), size.toLong())
                Assert.assertFalse(set.isEmpty)
                val key = p[i]
                Assert.assertTrue(set.remove(key))
                Assert.assertFalse(set.remove(key))
                (0..10)
                        .map { p[i * it / 10] }
                        .forEach { Assert.assertFalse(set.contains(it)) }
                if (i < count - 1) {
                    Assert.assertTrue(set.contains(p[i + 1]))
                }
            }
        }

        private fun genPermutation(random: Random, size: Int = ENTRIES_TO_ADD): LongArray {
            val p = LongArray(size)
            for (i in 1 until size) {
                val j = random.nextInt(i)
                p[i] = p[j]
                p[j] = i.toLong()
            }
            return p
        }
    }
}
