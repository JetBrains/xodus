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
        contextualEnv = env as ContextualEnvironmentImpl
        bitmap = contextualEnv.openBitmap("test")
    }

    override fun createEnvironment() {
        env = newContextualEnvironmentInstance(LogConfig.create(reader, writer))
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
            assertFalse(bitmap.get(bit0))

            assertTrue(bitmap.set(bit42, true))
            assertTrue(bitmap.get(bit42))

            assertTrue(bitmap.set(bit6040, true))
            assertTrue(bitmap.get(bit6040))
        }
    }

    @Test
    fun `set 63 bit`() {
        contextualEnv.executeInTransaction { txn ->
            assertTrue(bitmap.set(txn, bit63, true))
        }
    }

    @Test
    fun `set and get 100 random bits`() {
        val randomBits = mutableListOf<Long>()

        contextualEnv.executeInTransaction {
            for (i in 0..100) {
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
    fun `set and clear bits`() {
        contextualEnv.executeInTransaction {
            assertTrue(bitmap.set(bit0, true))
            assertTrue(bitmap.set(bit42, true))
            assertTrue(bitmap.set(bit6040, true))

            assertTrue(bitmap.get(bit0))
            assertTrue(bitmap.get(bit42))
            assertTrue(bitmap.get(bit6040))

            assertTrue(bitmap.clear(bit0))
            assertTrue(bitmap.clear(bit42))
            assertTrue(bitmap.clear(bit6040))

            assertFalse(bitmap.get(bit0))
            assertFalse(bitmap.get(bit42))
            assertFalse(bitmap.get(bit6040))
        }
    }

}