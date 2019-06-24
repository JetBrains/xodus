/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.gc

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessor
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.env.TransactionalExecutable
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.io.inMemory.Memory
import jetbrains.exodus.io.inMemory.MemoryDataReader
import jetbrains.exodus.io.inMemory.MemoryDataWriter
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.BrokenBarrierException
import java.util.concurrent.CyclicBarrier

open class GarbageCollectorTestInMemory : GarbageCollectorTest() {

    private val rnd = Random()
    private var memory: Memory? = null

    override fun createRW(): Pair<DataReader, DataWriter> {
        memory = Memory()
        return Pair(MemoryDataReader(memory!!), MemoryDataWriter(memory!!))
    }

    override fun deleteRW() {
        reader = null
        writer = null
        memory = null
    }

    @Test
    fun testTextIndexLike() {
        val started = System.currentTimeMillis()
        prepare()
        val txn = env.beginTransaction()
        val store = env.openStore("store", getStoreConfig(false), txn)
        val storeDups = env.openStore("storeDups", getStoreConfig(true), txn)
        txn.commit()
        try {
            while (System.currentTimeMillis() - started < TEST_DURATION) {
                env.executeInTransaction { txn ->
                    var randomInt = rnd.nextInt() and 0x3fffffff
                    val count = 4 + randomInt and 0x1f
                    var j = 0
                    while (j < count) {
                        val intKey = randomInt and 0x3fff
                        val key = IntegerBinding.intToCompressedEntry(intKey)
                        val valueLength = 50 + randomInt % 100
                        store.put(txn, key, ArrayByteIterable(ByteArray(valueLength)))
                        storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32))
                        randomInt += ++j
                    }
                }
                Thread.sleep(0)
            }
        } catch (t: Throwable) {
            memory!!.dump(File(System.getProperty("user.home"), "dump"))
            logger.error("User code exception: ", t)
            Assert.fail()
        }

    }

    @Test
    fun testTextIndexLikeWithDeletions() {
        val started = System.currentTimeMillis()
        prepare()
        val txn = env.beginTransaction()
        val store = env.openStore("store", getStoreConfig(false), txn)
        val storeDups = env.openStore("storeDups", getStoreConfig(true), txn)
        txn.commit()
        try {
            while (System.currentTimeMillis() - started < TEST_DURATION) {
                env.executeInTransaction(object : TransactionalExecutable {
                    override fun execute(txn: Transaction) {
                        var randomInt = rnd.nextInt() and 0x3fffffff
                        val count = 4 + randomInt and 0x1f
                        run {
                            var j = 0
                            while (j < count) {
                                val intKey = randomInt and 0x3fff
                                val key = IntegerBinding.intToCompressedEntry(intKey)
                                val valueLength = 50 + randomInt % 100
                                store.put(txn, key, ArrayByteIterable(ByteArray(valueLength)))
                                storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32))
                                randomInt += ++j
                            }
                        }
                        randomInt = randomInt * randomInt and 0x3fffffff
                        var j = 0
                        while (j < count / 2) {
                            val intKey = randomInt and 0x3fff
                            val key = IntegerBinding.intToCompressedEntry(intKey)
                            store.delete(txn, key)
                            storeDups.openCursor(txn).use { cursor ->
                                if (cursor.getSearchBoth(key, IntegerBinding.intToEntry(randomInt % 32))) {
                                    cursor.deleteCurrent()
                                }
                            }
                            randomInt += ++j
                        }

                    }
                })
                Thread.sleep(0)
            }
        } catch (t: Throwable) {
            memory!!.dump(File(System.getProperty("user.home"), "dump"))
            logger.error("User code exception: ", t)
            Assert.fail()
        }

    }

    @Test
    @Throws(InterruptedException::class, BrokenBarrierException::class)
    fun testTextIndexLikeWithDeletionsAndConcurrentReading() {
        val started = System.currentTimeMillis()
        prepare()
        val txn = env.beginTransaction()
        val store = env.openStore("store", getStoreConfig(false), txn)
        val storeDups = env.openStore("storeDups", getStoreConfig(true), txn)
        txn.commit()
        var throwable: Throwable? = null
        val processors = arrayOfNulls<JobProcessor>(10)
        for (i in processors.indices) {
            processors[i] = ThreadJobProcessorPool.getOrCreateJobProcessor("test processor$i").apply { start() }
        }
        val barrier = CyclicBarrier(processors.size + 1)
        processors[0]?.queue(object : Job() {
            @Throws(Throwable::class)
            override fun execute() {
                barrier.await()
                try {
                    while (System.currentTimeMillis() - started < TEST_DURATION) {
                        env.executeInTransaction(object : TransactionalExecutable {
                            override fun execute(txn: Transaction) {
                                var randomInt = rnd.nextInt() and 0x3fffffff
                                val count = 4 + randomInt and 0x1f
                                run {
                                    var j = 0
                                    while (j < count) {
                                        val intKey = randomInt and 0x3fff
                                        val key = IntegerBinding.intToCompressedEntry(intKey)
                                        val valueLength = 50 + randomInt % 100
                                        store.put(txn, key, ArrayByteIterable(ByteArray(valueLength)))
                                        storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32))
                                        randomInt += ++j
                                    }
                                }
                                randomInt = randomInt * randomInt and 0x3fffffff
                                var j = 0
                                while (j < count) {
                                    val intKey = randomInt and 0x3fff
                                    val key = IntegerBinding.intToCompressedEntry(intKey)
                                    store.delete(txn, key)
                                    storeDups.openCursor(txn).use { cursor ->
                                        if (cursor.getSearchBoth(key, IntegerBinding.intToEntry(randomInt % 32))) {
                                            cursor.deleteCurrent()
                                        }
                                    }
                                    randomInt += ++j
                                }
                            }
                        })
                        Thread.sleep(0)
                    }
                } catch (t: Throwable) {
                    throwable = t
                }

            }
        })
        for (i in 1 until processors.size) {
            processors[i]?.queue(object : Job() {
                override fun execute() {
                    try {
                        barrier.await()
                        while (System.currentTimeMillis() - started < TEST_DURATION) {
                            var randomInt = rnd.nextInt() and 0x3fffffff
                            var j = 0
                            while (j < 100) {
                                val intKey = randomInt and 0x3fff
                                val key = IntegerBinding.intToCompressedEntry(intKey)
                                getAutoCommit(store, key)
                                getAutoCommit(storeDups, key)
                                Thread.sleep(0)
                                randomInt += ++j
                            }
                            Thread.sleep(50)
                        }
                    } catch (t: Throwable) {
                        throwable = t
                    }
                }
            })
        }
        barrier.await()
        for (processor in processors) {
            processor?.finish()
        }
        val t = throwable
        if (t != null) {
            memory!!.dump(File(System.getProperty("user.home"), "dump"))
            logger.error("User code exception: ", t)
            Assert.fail()
        }
    }

    private fun prepare() {
        setLogFileSize(2)
        env.environmentConfig.treeMaxPageSize = 16
        reopenEnvironment()
    }

    companion object {

        private val logger = LoggerFactory.getLogger(GarbageCollectorTestInMemory::class.java)
        private const val TEST_DURATION = 1000 * 10
    }
}
