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

import jetbrains.exodus.bindings.LongBinding
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

private const val bit0 = 0L
private const val bit63 = 63L
private const val bit42 = 42L
private const val bit6040 = 6040L

open class BitmapImplTest : EnvironmentTestsBase() {

    protected lateinit var bitmap: BitmapImpl

    @Before
    override fun setUp() {
        super.setUp()
        bitmap = env.computeInExclusiveTransaction { env.openBitmap("test", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, it) }
    }

    @Test
    fun `get from unused bitmap`() {
        env.executeInTransaction { txn ->
            assertFalse(bitmap.get(txn, bit0))
            assertFalse(bitmap.get(txn, bit42))
        }
    }

    @Test
    fun `set bits`() {
        env.executeInTransaction { txn ->
            assertFalse(bitmap.set(txn, bit0, false))
            assertTrue(bitmap.set(txn, bit42, true))
            assertTrue(bitmap.set(txn, bit6040, true))
        }
    }

    @Test
    fun `successive bits get compressed`() {
        (0L..63L).forEach { bit ->
            env.executeInTransaction { txn ->
                bitmap.set(txn, bit, true)
            }
            if (bit == 0L || bit == 62L || bit == 63L) {
                env.executeInReadonlyTransaction { txn ->
                    assertEquals(1, bitmap.store.get(txn, LongBinding.longToCompressedEntry(0L))?.length)
                }
            }
        }
    }

    @Test
    fun `set and get 100 random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()

            for (i in 0..100) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)

                bitmap.set(txn, randomBit, true)
            }

            randomBits.forEach { assertTrue(bitmap.get(txn, it)) }
        }
    }

    @Test
    fun `change bits value`() {
        env.executeInTransaction { txn ->
            assertTrue(bitmap.set(txn, bit0, true))
            assertTrue(bitmap.get(txn, bit0))

            assertTrue(bitmap.set(txn, bit42, true))
            assertTrue(bitmap.get(txn, bit42))
            assertTrue(bitmap.get(txn, bit0))

            assertTrue(bitmap.set(txn, bit42, false))
            assertFalse(bitmap.get(txn, bit42))
            assertTrue(bitmap.get(txn, bit0))

            assertFalse(bitmap.set(txn, bit42, false))
            assertFalse(bitmap.get(txn, bit42))
            assertTrue(bitmap.get(txn, bit0))

            assertTrue(bitmap.set(txn, bit42, true))
            assertTrue(bitmap.get(txn, bit42))
            assertTrue(bitmap.get(txn, bit0))
        }
    }

    @Test
    fun `clear unsetted bits`() {
        env.executeInTransaction { txn ->
            assertFalse(bitmap.get(txn, bit0))
            assertFalse(bitmap.get(txn, bit42))
            assertFalse(bitmap.get(txn, bit6040))

            assertFalse(bitmap.clear(txn, bit0))
            assertFalse(bitmap.clear(txn, bit42))
            assertFalse(bitmap.clear(txn, bit6040))

            assertFalse(bitmap.get(txn, bit0))
            assertFalse(bitmap.get(txn, bit42))
            assertFalse(bitmap.get(txn, bit6040))
        }
    }

    @Test
    fun `all operations for 63 bit`() {
        allOperationsForOneBit(bit63)
    }

    @Test
    fun `all operations for 1691827968276783104 bit`() {
        allOperationsForOneBit(1691827968276783104)
    }

    @Test
    fun `all operations for 62nd and 63rd bits`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 62L, true)
            bitmap.set(txn, 63L, true)
            assertTrue(bitmap.get(txn, 62L))
            assertTrue(bitmap.get(txn, 63L))

            bitmap.clear(txn, 62L)
            assertFalse(bitmap.get(txn, 62L))
            assertTrue(bitmap.get(txn, 63L))

            bitmap.clear(txn, 63L)
            assertFalse(bitmap.get(txn, 63L))
        }
    }

    @Test
    fun `all operations for random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..10) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                assertTrue(bitmap.set(txn, randomBit, true))
            }

            randomBits.forEach {
                assertTrue(bitmap.clear(txn, it))
            }

            randomBits.forEach {
                assertFalse(bitmap.get(txn, it))
            }
        }
    }

    @Test
    fun `clear 62nd and 63rd consequent bits`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 62L, true)
            bitmap.set(txn, 63L, true)
            assertTrue(bitmap.get(txn, 62L))
            assertTrue(bitmap.get(txn, 63L))

            bitmap.clear(txn, 62L)
            assertFalse(bitmap.get(txn, 62L))
            assertTrue(bitmap.get(txn, 63L))

            bitmap.clear(txn, 63L)
            assertFalse(bitmap.get(txn, 63L))
        }
    }

    @Test
    fun `clear random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..10) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }

            randomBits.forEach {
                bitmap.clear(txn, it)
            }

            randomBits.forEach {
                assertFalse(bitmap.get(txn, it))
            }
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `set negative bit`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, -1, true)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `get negative bit`() {
        env.executeInTransaction { txn ->
            bitmap.get(txn, -1)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `clear negative bit`() {
        env.executeInTransaction { txn ->
            bitmap.clear(txn, -1)
        }
    }

    @Test
    fun `getFirst and getLast on empty bitmap`() {
        env.executeInTransaction { txn ->
            assertEquals(-1L, bitmap.getFirst(txn))
            assertEquals(-1L, bitmap.getLast(txn))
        }
    }

    @Test
    fun `getFirst and getLast on  bitmap with one element`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit42, true)
            assertEquals(bit42, bitmap.getFirst(txn))
            assertEquals(bit42, bitmap.getLast(txn))
        }
    }

    @Test
    fun `getFirst and getLast on  bitmap with tree elements`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, bit42, true)
            bitmap.set(txn, bit63, true)
            bitmap.set(txn, bit6040, true)
            assertEquals(bit42, bitmap.getFirst(txn))
            assertEquals(bit6040, bitmap.getLast(txn))
        }
    }

    @Test
    fun `count for empty`() {
        env.executeInTransaction { txn ->
            assertEquals(0, bitmap.count(txn))
        }
    }

    @Test
    fun `count for set and clear all bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..10) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            assertEquals(randomBits.size.toLong(), bitmap.count(txn))

            randomBits.forEach {
                bitmap.clear(txn, it)
            }
            assertEquals(0L, bitmap.count(txn))
        }
    }

    @Test
    fun `count for lots of set bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..100) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            assertEquals(randomBits.size.toLong(), bitmap.count(txn))
        }
    }

    @Test
    fun `count for lots of set bits in small interval`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableSetOf<Long>()
            for (i in 0..100) {
                val randomBit = (Math.random() * 1000L).toLong()
                randomBits.add(randomBit)
                bitmap.set(txn, randomBit, true)
            }
            assertEquals(randomBits.size.toLong(), bitmap.count(txn))
        }
    }

    @Test
    fun `count in range`() {
        env.executeInTransaction { txn ->
            bitmap.set(txn, 0, true)
            bitmap.set(txn, 32, true)
            bitmap.set(txn, 64, true)
            bitmap.set(txn, 128, true)
            assertEquals(4L, bitmap.count(txn, 0L, 128L))
            assertEquals(3L, bitmap.count(txn, 1L, 128L))
            assertEquals(2L, bitmap.count(txn, 1L, 127L))
            assertEquals(2L, bitmap.count(txn, 33L, 128L))
            assertEquals(2L, bitmap.count(txn, 64L, 128L))
            assertEquals(1L, bitmap.count(txn, 64L, 127L))
            assertEquals(1L, bitmap.count(txn, 65L, 128L))
            assertEquals(0L, bitmap.count(txn, 65L, 127L))
            assertEquals(0L, bitmap.count(txn, 129L, 1000L))
            assertEquals(0L, bitmap.count(txn, 33L, 63L))
        }
    }

    @Test
    fun `sequential set`() {
        env.executeInTransaction { txn ->
            for (i in 0L..1000L) {
                assertEquals(i, bitmap.count(txn))
                assertTrue(bitmap.set(txn, i, true))
                assertTrue(i.toString(), bitmap.get(txn, i))
            }
        }
    }

    private fun allOperationsForOneBit(bit: Long) {
        env.executeInTransaction { txn ->
            assertTrue(bitmap.set(txn, bit, true))
            assertTrue(bitmap.get(txn, bit))
            assertTrue(bitmap.clear(txn, bit))
            assertFalse(bitmap.get(txn, bit))
        }
    }
}