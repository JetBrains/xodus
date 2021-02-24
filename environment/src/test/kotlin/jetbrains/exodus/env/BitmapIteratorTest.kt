package jetbrains.exodus.env

import jetbrains.exodus.TestUtil
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

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
    fun `hasNext for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            for (i in 0..20) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
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
    fun `hasNext twice for bitmap with 1 set bit`() {
        env.executeInTransaction { txn ->
            val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
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
    fun `hasNext after set and clear`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit0, true)
            bitmap.clear(txn, bit0)
            val iter = bitmap.iterator(txn)
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
        val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
        oneBitTest(randomBit)
    }


    @Test
    fun `iterator for bitmap with 3 set bits without hasNext check`() {
        env.executeInTransaction { txn ->
            val randomBit = (Math.random() * Long.MAX_VALUE).toLong() - 10
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
    fun `iterator for bitmap with many set bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableListOf<Long>()
            for (i in 0..100) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            randomBits.sort()
            val iterBitmap = bitmap.iterator(txn)
            val iterList = randomBits.iterator()
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
    fun `call remove twice on the same element`() {
        TestUtil.runWithExpectedException({
            env.executeInTransaction { txn ->
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
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
            val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
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
            val randomBit = (Math.random() * Long.MAX_VALUE).toLong() - 2
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
            val randomBits = mutableListOf<Long>()
            for (i in 0..10) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            randomBits.sort()

            val iter = bitmap.iterator(txn)
            while (iter.hasNext()) {
                iter.next()
                iter.remove()
            }

            randomBits.forEach {
                assertFalse(bitmap.get(txn, it))
            }
        }
    }

    private fun oneBitTest(bit: Long) {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit, true)
            val ind = bitmap.iterator(txn).next()
            bitmap.clear(txn, bit)
            assertEquals(bit, ind)
        }
    }


}