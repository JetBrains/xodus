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
package jetbrains.exodus.env

import jetbrains.exodus.log.LogConfig
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

internal class ContextualBitmapImplTest : EnvironmentTestsBase() {

    private lateinit var bitmap: ContextualBitmapImpl
    private lateinit var contextualEnv: ContextualEnvironmentImpl

    companion object {
        private const val bit0 = 0L
        private const val bit42 = 42L
        private const val bit63 = 63L
        private const val bit6040 = 6040L
    }

    @Before
    override fun setUp() {
        super.setUp()
        contextualEnv = environment!! as ContextualEnvironmentImpl
        bitmap = contextualEnv.openBitmap("test", StoreConfig.WITHOUT_DUPLICATES)
    }

    override fun createEnvironment() {
        environment = newContextualEnvironmentInstance(LogConfig.create(reader!!, writer!!))
    }

    @Test
    fun `get from unused bitmap`() {
        contextualEnv.executeInTransaction {
            assertFalse(bitmap.get(bit0))
            assertFalse(bitmap.get(bit42))
        }
    }

    @Test
    fun `set bits`() {
        contextualEnv.executeInTransaction {
            assertFalse(bitmap.set(bit0, false))
            assertTrue(bitmap.set(bit42, true))
            assertTrue(bitmap.set(bit6040, true))
        }
    }

    @Test
    fun `set and get 100 random bits`() {
        contextualEnv.executeInTransaction {
            val randomBits = mutableListOf<Long>()

            for(i in 0..100) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)

                bitmap.set(randomBit, true)
            }

            randomBits.forEach { assertTrue(bitmap.get(it)) }
        }
    }

    @Test
    fun `change bits value`() {
        contextualEnv.executeInTransaction {
            assertTrue(bitmap.set(bit0, true))
            assertTrue(bitmap.get(bit0))

            assertTrue(bitmap.set(bit42, true))
            assertTrue(bitmap.get(bit42))
            assertTrue(bitmap.get(bit0))

            assertTrue(bitmap.set(bit42, false))
            assertFalse(bitmap.get(bit42))
            assertTrue(bitmap.get(bit0))

            assertFalse(bitmap.set(bit42, false))
            assertFalse(bitmap.get(bit42))
            assertTrue(bitmap.get(bit0))

            assertTrue(bitmap.set(bit42, true))
            assertTrue(bitmap.get(bit42))
            assertTrue(bitmap.get(bit0))
        }
    }

    @Test
    fun `clear unsetted bits`() {
        contextualEnv.executeInTransaction {
            assertFalse(bitmap.get(bit0))
            assertFalse(bitmap.get(bit42))
            assertFalse(bitmap.get(bit6040))

            assertFalse(bitmap.clear(bit0))
            assertFalse(bitmap.clear(bit42))
            assertFalse(bitmap.clear(bit6040))

            assertFalse(bitmap.get(bit0))
            assertFalse(bitmap.get(bit42))
            assertFalse(bitmap.get(bit6040))
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
        contextualEnv.executeInTransaction {
            bitmap.set(62L, true)
            bitmap.set(63L, true)
            assertTrue(bitmap.get(62L))
            assertTrue(bitmap.get(63L))

            bitmap.clear(62L)
            assertFalse(bitmap.get(62L))
            assertTrue(bitmap.get(63L))

            bitmap.clear(63L)
            assertFalse(bitmap.get(63L))
        }
    }

    @Test
    fun `all operations for random bits`() {
        contextualEnv.executeInTransaction {
            val randomBits = mutableListOf<Long>()
            for (i in 0..10) {
                val randomBit = (Math.random() * Long.MAX_VALUE).toLong()
                randomBits.add(randomBit)
                assertTrue(bitmap.set(randomBit, true))
            }

            randomBits.forEach {
                assertTrue(bitmap.clear(it))
            }

            randomBits.forEach {
                assertFalse(bitmap.get(it))
            }
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `set negative bit`() {
        contextualEnv.executeInTransaction {
            bitmap.set(-1, true)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `get negative bit`() {
        contextualEnv.executeInTransaction {
            bitmap.get(-1)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `clear negative bit`() {
        contextualEnv.executeInTransaction {
            bitmap.clear(-1)
        }
    }

    private fun allOperationsForOneBit(bit: Long) {
        contextualEnv.executeInTransaction {
            assertTrue(bitmap.set(bit, true))
            assertTrue(bitmap.get(bit))
            assertTrue(bitmap.clear(bit))
            assertFalse(bitmap.get(bit))
        }
    }
}