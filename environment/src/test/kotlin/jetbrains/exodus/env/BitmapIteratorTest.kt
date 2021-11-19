/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil
import org.junit.Assert.*
import org.junit.Test
import kotlin.random.Random

class BitmapIteratorTest : BitmapImplTest() {

    companion object {
        private const val bit0 = 0L
        private const val bit1 = 1L
        private const val bit63 = 63L
        private const val bit42 = 42L
    }

    @Test
    fun `hasNext for empty`() {
        env.executeInTransaction { txn ->
            val iter = bitmap.iterator(txn)
            assertFalse(iter.hasNext())
        }
    }

    @Test
    fun `reversed hasNext for empty`() {
        env.executeInTransaction { txn ->
            val iter = bitmap.reverseIterator(txn)
            assertFalse(iter.hasNext())
        }
    }

    @Test
    fun `hasNext for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            for (i in 0..20) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                bitmap.set(txn, randomBit, true)
                val iter = bitmap.iterator(txn)
                assertTrue(iter.hasNext())
                assertEquals(iter.next(), randomBit)
                assertFalse(iter.hasNext())
                bitmap.clear(txn, randomBit)
            }
        }
    }

    @Test
    fun `reversed hasNext for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            for (i in 0..20) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                bitmap.set(txn, randomBit, true)
                val iter = bitmap.reverseIterator(txn)
                assertTrue(iter.hasNext())
                assertEquals(iter.next(), randomBit)
                assertFalse(iter.hasNext())
                bitmap.clear(txn, randomBit)
            }
        }
    }

    @Test
    fun `hasNext twice for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE)
            bitmap.set(txn, randomBit, true)
            val iter = bitmap.iterator(txn)

            assertTrue(iter.hasNext())
            assertTrue(iter.hasNext())

            assertEquals(iter.next(), randomBit)
            assertFalse(iter.hasNext())
            bitmap.clear(txn, randomBit)
        }
    }

    @Test
    fun `reversed hasNext twice for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE)
            bitmap.set(txn, randomBit, true)
            val iter = bitmap.reverseIterator(txn)

            assertTrue(iter.hasNext())
            assertTrue(iter.hasNext())

            assertEquals(iter.next(), randomBit)
            assertFalse(iter.hasNext())
            bitmap.clear(txn, randomBit)
        }
    }

    @Test
    fun `hasNext after set and clear`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit0, true)
            bitmap.clear(txn, bit0)
            val iter = bitmap.iterator(txn)
            assertEquals(false, iter.hasNext())
        }
    }

    @Test
    fun `reversed hasNext after set and clear`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit0, true)
            bitmap.clear(txn, bit0)
            val iter = bitmap.reverseIterator(txn)
            assertEquals(false, iter.hasNext())
        }
    }

    @Test
    fun `iterator for bitmap with 1 set bit without hasNext check`() {
        oneBitTest(bit0)
        oneBitTest(bit1)
        oneBitTest(bit42)
        oneBitTest(bit63)
        oneBitTest(1691827968276783104)
        val randomBit = Random.nextLong(Long.MAX_VALUE)
        oneBitTest(randomBit)
    }

    @Test
    fun `reversed iterator for bitmap with 1 set bit without hasNext check`() {
        oneBitTest(bit0)
        oneBitTest(bit1)
        oneBitTest(bit42)
        oneBitTest(bit63)
        oneBitTest(1691827968276783104)
        val randomBit = Random.nextLong(Long.MAX_VALUE)
        oneBitTest(randomBit, -1)
    }


    @Test
    fun `iterator for bitmap with 3 set bits without hasNext check`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 10)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 1, true)
            bitmap.set(txn, randomBit + 10, true)

            val iterBitmap = bitmap.iterator(txn)
            assertEquals(randomBit, iterBitmap.next())
            assertEquals(randomBit + 1, iterBitmap.next())
            assertEquals(randomBit + 10, iterBitmap.next())
        }
    }

    @Test
    fun `reversed iterator for bitmap with 3 set bits without hasNext check`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 10)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 1, true)
            bitmap.set(txn, randomBit + 10, true)

            val iterBitmap = bitmap.reverseIterator(txn)
            assertEquals(randomBit + 10, iterBitmap.next())
            assertEquals(randomBit + 1, iterBitmap.next())
            assertEquals(randomBit, iterBitmap.next())
        }
    }

    @Test
    fun `iterator for bitmap with many set bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..100) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            val iterBitmap = bitmap.iterator(txn)
            val iterList = randomBits.sorted().iterator()
            while (iterBitmap.hasNext()) {
                assertEquals(iterList.next(), iterBitmap.next())
            }
            assertFalse(iterList.hasNext())
        }
    }


    @Test
    fun `reverse iterator for bitmap with many set bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..100) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }

            val iterBitmap = bitmap.reverseIterator(txn)
            val iterList = randomBits.sortedDescending().iterator()
            while (iterBitmap.hasNext()) {
                assertEquals(iterList.next(), iterBitmap.next())
            }
            assertFalse(iterList.hasNext())
        }
    }

    @Test
    fun `call remove on empty element`() {
        TestUtil.runWithExpectedException({
            env.executeInTransaction { txn ->
                val iter = bitmap.iterator(txn)
                iter.remove()
            }
        }, IllegalStateException::class.java)
    }

    @Test
    fun `reverse call remove on empty element`() {
        TestUtil.runWithExpectedException({
            env.executeInTransaction { txn ->
                val iter = bitmap.reverseIterator(txn)
                iter.remove()
            }
        }, IllegalStateException::class.java)
    }

    @Test
    fun `call remove twice on the same element`() {
        TestUtil.runWithExpectedException({
            env.executeInTransaction { txn ->
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                bitmap.set(txn, randomBit, true)
                val iter = bitmap.iterator(txn)
                iter.next()
                iter.remove()
                iter.remove()
            }
        }, IllegalStateException::class.java)
    }

    @Test
    fun `remove from bitmap with 1 bit`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE)
            bitmap.set(txn, randomBit, true)

            val iter = bitmap.iterator(txn)
            while (iter.hasNext()) {
                iter.next()
                iter.remove()
            }

            assertFalse(bitmap.get(txn, randomBit))
        }
    }

    @Test
    fun `remove 3 subsequent bits`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 2)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 1, true)
            bitmap.set(txn, randomBit + 2, true)

            val iter = bitmap.iterator(txn)
            while (iter.hasNext()) {
                iter.next()
                iter.remove()
            }
            assertFalse(bitmap.get(txn, randomBit))
            assertFalse(bitmap.get(txn, randomBit + 1))
            assertFalse(bitmap.get(txn, randomBit + 2))
        }
    }

    @Test
    fun `remove random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..10) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }

            val iter = bitmap.iterator(txn)
            while (iter.hasNext()) {
                iter.next()
                iter.remove()
            }

            randomBits.sorted().forEach {
                assertFalse(bitmap.get(txn, it))
            }
        }
    }

    @Test
    fun `reverse remove random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..10) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }

            val iter = bitmap.reverseIterator(txn)
            while (iter.hasNext()) {
                iter.next()
                iter.remove()
            }

            randomBits.sortedDescending().forEach {
                assertFalse(bitmap.get(txn, it))
            }
        }
    }

    @Test
    fun `navigate to the only set bit`() {
        env.executeInTransaction { txn ->
            for (i in 0..20) {
                val randomBit = Random.nextLong(Long.MAX_VALUE)
                bitmap.set(txn, randomBit, true)
                assertEquals(1, bitmap.count(txn))
                bitmap.iterator(txn).let {
                    assertTrue(it.getSearchBit(randomBit))
                    assertTrue(it.hasNext())
                    assertEquals(randomBit, it.next())
                    assertFalse(it.hasNext())
                }
                bitmap.clear(txn, randomBit)
            }
        }
    }

    @Test
    fun `navigate to the second set bit`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 20)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 10, true)
            bitmap.set(txn, randomBit + 20, true)
            bitmap.iterator(txn).let {
                assertTrue(it.getSearchBit(randomBit + 10))
                assertTrue(it.hasNext())
            }
        }
    }

    @Test
    fun `iterate from the random bit`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 11)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 10, true)
            bitmap.set(txn, randomBit + 11, true)
            bitmap.iterator(txn).let { iter ->
                assertTrue(iter.getSearchBit(randomBit + 10))
                assertEquals(randomBit + 10, iter.next())
                assertEquals(randomBit + 11, iter.next())
                assertFalse(iter.hasNext())
            }
        }
    }

    @Test
    fun `iterate from the bit with big step`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit42, true)
            bitmap.set(txn, bit42 + 10, true)
            bitmap.set(txn, bit42 + 20, true)
            bitmap.iterator(txn).let { iter ->
                assertTrue(iter.getSearchBit(bit42 + 10))
                assertEquals(bit42 + 10, iter.next())
            }
        }
    }

    @Test
    fun `iterate from the random bit with big step`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 20)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 10, true)
            bitmap.set(txn, randomBit + 20, true)
            bitmap.iterator(txn).let { iter ->
                assertTrue(iter.getSearchBit(randomBit + 10))
                assertEquals(randomBit + 10, iter.next())
                assertTrue(iter.getSearchBit(randomBit + 11))
                assertEquals(randomBit + 20, iter.next())
            }
        }
    }

    @Test
    fun `reversed iteration from the random bit`() {
        env.executeInTransaction { txn ->
            val randomBit = bit63
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 1, true)
            bitmap.set(txn, randomBit + 2, true)
            bitmap.reverseIterator(txn).let {
                assertTrue(it.getSearchBit(randomBit + 2))
                assertEquals(randomBit + 1, it.next())
                assertEquals(randomBit, it.next())
            }
        }
    }

    @Test
    fun `reversed iteration from the random bit with big step`() {
        env.executeInTransaction { txn ->
            val randomBit = Random.nextLong(Long.MAX_VALUE - 20)
            bitmap.set(txn, randomBit, true)
            bitmap.set(txn, randomBit + 10, true)
            bitmap.set(txn, randomBit + 20, true)
            bitmap.reverseIterator(txn).let {
                assertTrue(it.getSearchBit(randomBit + 20))
                assertEquals(randomBit + 10, it.next())
                assertTrue(it.getSearchBit(randomBit + 9))
                assertEquals(randomBit, it.next())
            }
        }
    }

    @Test
    fun `navigate to not existing bit between existing and move cursor to bigger one`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 0, true)
            bitmap.set(txn, 10, true)
            bitmap.set(txn, 128, true)
            bitmap.iterator(txn).let {
                assertTrue(it.getSearchBit(65))
                assertEquals(128, it.next())
            }
        }
    }

    @Test
    fun `reversed navigate to not existing bit between existing and move cursor to smaller one`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 0, true)
            bitmap.set(txn, 10, true)
            bitmap.set(txn, 128, true)
            bitmap.reverseIterator(txn).let {
                assertTrue(it.getSearchBit(65))
                assertEquals(10, it.next())
            }
        }
    }

    @Test
    fun `reversed navigate to smaller bit`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 0, true)
            bitmap.set(txn, 10, true)
            bitmap.set(txn, 65, true)
            bitmap.reverseIterator(txn).let {
                assertTrue(it.getSearchBit(128))
                assertEquals(65, it.next())
            }
        }
    }

    @Test
    @TestFor(issue = "XD-848")
    fun `search bit with patricia v2 format`() {
        val startBit = 2048L + 64L
        env.executeInTransaction { txn ->
            bitmap.set(txn, 1024L, true)
            for (i in 0L..33L) {
                bitmap.set(txn, startBit + (i * 64), true)
            }
        }
        env.executeInReadonlyTransaction { txn ->
            bitmap.iterator(txn).let {
                assertTrue(it.getSearchBit(startBit - 64L))
                assertEquals(startBit, it.next())
            }
        }
    }

    private fun oneBitTest(bit: Long, direction: Int = 1) {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit, true)
            val ind =
                if (direction == 1) bitmap.iterator(txn).next()
                else bitmap.reverseIterator(txn).next()
            bitmap.clear(txn, bit)
            assertEquals(bit, ind)
        }
    }
}