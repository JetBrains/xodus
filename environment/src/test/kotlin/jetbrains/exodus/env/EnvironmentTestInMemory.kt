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

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.persistent.Persistent23TreeMap
import jetbrains.exodus.io.*
import jetbrains.exodus.io.inMemory.Memory
import jetbrains.exodus.io.inMemory.MemoryDataReader
import jetbrains.exodus.io.inMemory.MemoryDataWriter
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory

class EnvironmentTestInMemory : EnvironmentTest() {
    private val rnd = Random()
    override fun createRW(): Pair<DataReader, DataWriter> {
        val memory = Memory()
        return Pair(MemoryDataReader(memory), MemoryDataWriter(memory))
    }

    override fun deleteRW() {
        reader = null
        writer = null
    }

    @Test
    @Throws(InterruptedException::class)
    fun failedToPut() {
        prepare()
        val keysCount = 50000
        val valuesCount = 20
        val started = System.currentTimeMillis()
        val testMap = Persistent23TreeMap<Int, Int>()
        val primary = openStoreAutoCommit("primary", StoreConfig.WITHOUT_DUPLICATES)
        val secondary = openStoreAutoCommit("secondary", StoreConfig.WITH_DUPLICATES)
        while (System.currentTimeMillis() - started < TEST_DURATION) {
            if (rnd.nextInt() % 100 == 1) {
                Thread.sleep(101)
            }
            val txn: Transaction = environment!!.beginTransaction()
            val mutableMap = testMap.beginWrite()
            try {
                for (i in 0..9) {
                    putRandomKeyValue(primary, secondary, txn, keysCount, valuesCount, mutableMap)
                    deleteRandomKey(primary, secondary, txn, keysCount, mutableMap)
                }
                if (txn.flush()) {
                    mutableMap.endWrite()
                }
            } catch (t: Throwable) {
                logger.error("Failed to put", t)
                break
            } finally {
                txn.abort()
            }
        }
        environment!!.executeInTransaction { txn: Transaction? ->
            primary.openCursor(
                txn!!
            ).use { cursor ->
                Assert.assertTrue(cursor.next)
                for (entry in testMap.beginRead()) {
                    Assert.assertEquals(
                        (entry.key as Int).toLong(),
                        IntegerBinding.readCompressed(cursor.key.iterator()).toLong()
                    )
                    Assert.assertEquals(
                        (entry.value as Int).toLong(),
                        IntegerBinding.readCompressed(cursor.value.iterator()).toLong()
                    )
                    cursor.next
                }
            }
        }
    }

    @Suppress("SameParameterValue")
    private fun putRandomKeyValue(
        primary: Store?,
        secondary: Store?,
        txn: Transaction,
        keysCount: Int,
        valuesCount: Int,
        testMap: Persistent23TreeMap.MutableMap<Int, Int>
    ) {
        val key = rnd.nextInt(keysCount)
        val keyEntry = IntegerBinding.intToCompressedEntry(key)
        val value = rnd.nextInt(valuesCount)
        testMap.put(key, value)
        val valueEntry = IntegerBinding.intToCompressedEntry(value)
        val oldValue = primary!![txn, keyEntry]
        primary.put(txn, keyEntry, valueEntry)
        if (oldValue != null) {
            secondary!!.openCursor(txn).use { cursor ->
                Assert.assertTrue(cursor.getSearchBoth(oldValue, keyEntry))
                Assert.assertTrue(cursor.deleteCurrent())
            }
        }
        secondary!!.put(txn, valueEntry, keyEntry)
    }

    @Suppress("SameParameterValue")
    private fun deleteRandomKey(
        primary: Store?,
        secondary: Store?,
        txn: Transaction,
        keysCount: Int,
        testMap: Persistent23TreeMap.MutableMap<Int, Int>
    ) {
        val key = rnd.nextInt(keysCount)
        testMap.remove(key)
        val keyEntry = IntegerBinding.intToCompressedEntry(key)
        val oldValue = primary!![txn, keyEntry]
        primary.delete(txn, keyEntry)
        if (oldValue != null) {
            secondary!!.openCursor(txn).use { cursor ->
                Assert.assertTrue(cursor.getSearchBoth(oldValue, keyEntry))
                Assert.assertTrue(cursor.deleteCurrent())
            }
        }
    }

    private fun prepare() {
        setLogFileSize(4096)
        environment!!.environmentConfig.setTreeMaxPageSize(16)
        reopenEnvironment()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(EnvironmentTestInMemory::class.java)
        private const val TEST_DURATION = 1000 * 15
    }
}
