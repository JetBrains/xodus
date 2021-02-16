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

import jetbrains.exodus.log.LogConfig
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class BitmapImplTest : EnvironmentTestsBase() {
    private lateinit var bitmap: BitmapImpl

    companion object {
        private const val bit0 = 0L
        private const val bit63 = 63L
        private const val bit42 = 42L
        private const val bit6040 = 6040L
    }

    @Before
    override fun setUp() {
        val readerWriterPair = createRW()
        reader = readerWriterPair.first
        writer = readerWriterPair.second
        env = newEnvironmentInstance(LogConfig.create(reader, writer))
        val txn: Transaction = env.beginTransaction()
        bitmap = env.openBitmap("test", txn)
        txn.commit()
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
            assertFalse(bitmap.get(txn, bit0))

            assertTrue(bitmap.set(txn, bit42, true))
            assertTrue(bitmap.get(txn, bit42))

            assertTrue(bitmap.set(txn, bit6040, true))
            assertTrue(bitmap.get(txn, bit6040))
        }
    }


    @Test
    fun `set 63 bit`() {
        env.executeInTransaction { txn ->
            assertTrue(bitmap.set(txn, bit63, true))
        }
    }

    @Test
    fun `set and get 100 random bits`() {
        env.executeInTransaction { txn ->
            val randomBits = mutableListOf<Long>()

            for(i in 0..100) {
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
    fun `set and clear bits`() {
        env.executeInTransaction { txn ->
            assertTrue(bitmap.set(txn, bit0, true))
            assertTrue(bitmap.set(txn, bit42, true))
            assertTrue(bitmap.set(txn, bit6040, true))

            assertTrue(bitmap.get(txn, bit0))
            assertTrue(bitmap.get(txn, bit42))
            assertTrue(bitmap.get(txn, bit6040))

            assertTrue(bitmap.clear(txn, bit0))
            assertTrue(bitmap.clear(txn, bit42))
            assertTrue(bitmap.clear(txn, bit6040))

            assertFalse(bitmap.get(txn, bit0))
            assertFalse(bitmap.get(txn, bit42))
            assertFalse(bitmap.get(txn, bit6040))
        }
    }

}