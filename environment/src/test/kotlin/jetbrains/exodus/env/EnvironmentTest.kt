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

import jetbrains.exodus.*
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.dataStructures.hash.HashSet
import jetbrains.exodus.core.execution.locks.Latch
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.io.*
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig.Companion.create
import jetbrains.exodus.log.LogTestConfig
import jetbrains.exodus.log.LogUtil.getLogFilename
import jetbrains.exodus.tree.btree.BTreeBase
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import org.junit.Assert
import org.junit.Test
import java.io.File
import java.io.IOException
import java.lang.ref.WeakReference
import java.util.*

open class EnvironmentTest : EnvironmentTestsBase() {
    private val subfolders: MutableMap<String, File> = HashMap()

    @Test
    fun testEmptyEnvironment() {
        assertLoggableTypes(
            log,
            0,
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt()
        )
    }

    @Test
    fun testStatisticsBytesWritten() {
        testEmptyEnvironment()
        Assert.assertTrue(environment!!.statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_WRITTEN).total > 0L)
    }

    @Test
    fun testCreateSingleStore() {
        openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)
        assertLoggableTypes(
            log,
            0,
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt(),
            BTreeBase.BOTTOM_ROOT.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt()
        )
    }

    @Test
    fun testStatisticsTransactions() {
        testCreateSingleStore()
        val statistics = environment!!.statistics
        Assert.assertTrue(statistics.getStatisticsItem(EnvironmentStatistics.Type.TRANSACTIONS).total > 0L)
        Assert.assertTrue(statistics.getStatisticsItem(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS).total > 0L)
    }

    @Test
    fun testStatisticsItemNames() {
        testStatisticsTransactions()
        val statistics = environment!!.statistics
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_WRITTEN))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_READ))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.TRANSACTIONS))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.READONLY_TRANSACTIONS))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.GC_TRANSACTIONS))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.ACTIVE_TRANSACTIONS))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.TRANSACTIONS_DURATION))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.READONLY_TRANSACTIONS_DURATION))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.GC_TRANSACTIONS_DURATION))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.DISK_USAGE))
        Assert.assertNotNull(statistics.getStatisticsItem(EnvironmentStatistics.Type.UTILIZATION_PERCENT))
    }

    @Test
    fun testClear() {
        environment!!.environmentConfig.setGcEnabled(false)
        testCreateSingleStore()
        environment!!.clear()
        testEmptyEnvironment()
        environment!!.clear()
        environment!!.clear()
        testEmptyEnvironment()
    }

    @Test
    fun testClearMoreThanOneFile() {
        setLogFileSize(1)
        environment!!.environmentConfig.setGcEnabled(false)
        testGetAllStoreNames()
        environment!!.clear()
        testEmptyEnvironment()
    }

    @Test
    @TestFor(issue = "XD-457")
    @Throws(InterruptedException::class)
    fun testClearWithTransaction_XD_457() {
        val latch = Latch.create()
        environment!!.executeInTransaction { txn: Transaction ->
            val store = environment!!.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
            store.put(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0"))
            Assert.assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")))
            val th = arrayOf<Throwable?>(null)
            // asynchronously clear the environment
            try {
                latch.acquire()
                runParallelRunnable {
                    latch.release()
                    try {
                        environment!!.clear()
                    } catch (t: Throwable) {
                        th[0] = t
                    }
                    latch.release()
                }
                latch.acquire()
            } catch (ignore: InterruptedException) {
                Thread.currentThread().interrupt()
                Assert.fail()
            }
            Assert.assertNull(th[0])
            Assert.assertTrue(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")))
        }
        latch.acquire()
        environment!!.executeInExclusiveTransaction { txn: Transaction ->
            val store = environment!!.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
            Assert.assertFalse(store.exists(txn, StringBinding.stringToEntry("0"), StringBinding.stringToEntry("0")))
        }
    }

    @Test
    fun testCloseTwice() {
        val count = 100
        val txn: Transaction = environment!!.beginTransaction()
        for (i in 0 until count) {
            environment!!.openStore("new_store$i", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        txn.commit()
        val envConfig = environment!!.environmentConfig
        environment!!.close()
        try {
            Assert.assertFalse(environment!!.isOpen)
            environment!!.close()
        } finally {
            // forget old env anyway to prevent tearDown fail
            environment = newEnvironmentInstance(create(reader!!, writer!!), envConfig)
        }
    }

    @Test
    fun testGetAllStoreNames() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val count = 100
        for (i in 0 until count) {
            environment!!.openStore("new_store$i", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        val stores = environment!!.getAllStoreNames(txn)
        txn.commit()
        Assert.assertEquals(count.toLong(), stores.size.toLong())
        // list is sorted by name in lexicographical order
        val names: MutableSet<String> = TreeSet()
        for (i in 0 until count) {
            names.add("new_store$i")
        }
        for ((i, name) in names.withIndex()) {
            Assert.assertEquals(name, stores[i])
        }
    }

    @Test
    fun testUpdateBalancePolicy() {
        val envConfig = environment!!.environmentConfig
        val bs = environment!!.getBTreeBalancePolicy()
        val wtf = StringBinding.stringToEntry("wtf")
        val count = bs.pageMaxSize - 1
        run {
            val txn: Transaction = environment!!.beginTransaction()
            try {
                val store: Store = environment!!.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
                for (i in 0 until count) {
                    store.put(txn, IntegerBinding.intToEntry(i), wtf)
                }
            } finally {
                txn.commit()
            }
        }
        environment!!.close()
        environment = newEnvironmentInstance(
            create(reader!!, writer!!),
            envConfig.setTreeMaxPageSize(count / 2)
        )
        val txn: Transaction = environment!!.beginTransaction()
        try {
            val store: Store = environment!!.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
            store.put(txn, IntegerBinding.intToEntry(count), wtf)
        } finally {
            txn.commit()
        }
    }

    @Test
    fun testReopenEnvironment() {
        testGetAllStoreNames()
        reopenEnvironment()
        testGetAllStoreNames()
    }

    @Test
    fun testBreakSavingMetaTree() {
        val ec = environment!!.environmentConfig
        if (ec.logCachePageSize > 1024) {
            ec.setLogCachePageSize(1024)
        }
        ec.setTreeMaxPageSize(16)
        recreateEnvinronment(ec)
        environment!!.executeInTransaction { txn: Transaction ->
            val store1 = environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
            val store2 = environment!!.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn)
            val store3 = environment!!.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn)
            val store4 = environment!!.openStore("store4", StoreConfig.WITHOUT_DUPLICATES, txn)
            store4.put(txn, IntegerBinding.intToCompressedEntry(0), IntegerBinding.intToCompressedEntry(0))
            for (i in 0..15) {
                store1.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
                store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
                store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
            }
        }
        reopenEnvironment()
        val testConfig = LogTestConfig()
        testConfig.maxHighAddress = 10470
        environment!!.log.setLogTestConfig(testConfig)
        try {
            for (i in 0..22) {
                environment!!.executeInTransaction { txn: Transaction ->
                    val store1 = environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
                    val store2 = environment!!.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn)
                    val store3 = environment!!.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn)
                    for (i1 in 0..12) {
                        store1.put(
                            txn,
                            IntegerBinding.intToCompressedEntry(i1),
                            IntegerBinding.intToCompressedEntry(i1)
                        )
                        store2.put(
                            txn,
                            IntegerBinding.intToCompressedEntry(i1),
                            IntegerBinding.intToCompressedEntry(i1)
                        )
                        store3.put(
                            txn,
                            IntegerBinding.intToCompressedEntry(i1),
                            IntegerBinding.intToCompressedEntry(i1)
                        )
                    }
                }
            }
            TestUtil.runWithExpectedException({
                environment!!.executeInTransaction { txn: Transaction ->
                    val store1 = environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
                    val store2 = environment!!.openStore("store2", StoreConfig.WITHOUT_DUPLICATES, txn)
                    val store3 = environment!!.openStore("store3", StoreConfig.WITHOUT_DUPLICATES, txn)
                    for (i in 0..12) {
                        store1.put(
                            txn,
                            IntegerBinding.intToCompressedEntry(i),
                            IntegerBinding.intToCompressedEntry(i)
                        )
                        store2.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
                        store3.put(txn, IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i))
                    }
                }
            }, ExodusException::class.java)
            environment!!.log.setLogTestConfig(null)
            AbstractConfig.suppressConfigChangeListenersForThread()
            try {
                reopenEnvironment()
            } finally {
                AbstractConfig.resumeConfigChangeListenersForThread()
            }
            environment!!.executeInTransaction { txn: Transaction -> environment!!.getAllStoreNames(txn) }
        } finally {
            environment!!.log.setLogTestConfig(null)
        }
    }

    @Test
    fun testUseExistingConfig() {
        val expectedConfig = StoreConfig.WITHOUT_DUPLICATES
        val name = "testDatabase"
        var txn: Transaction = environment!!.beginTransaction()
        environment!!.openStore(name, expectedConfig, txn)
        txn.commit()
        txn = environment!!.beginTransaction()
        val store: Store = environment!!.openStore(name, StoreConfig.USE_EXISTING, txn)
        Assert.assertEquals(expectedConfig, store.config)
        txn.commit()
    }

    @Test
    fun testWriteDataToSeveralFiles() {
        setLogFileSize(16)
        environment!!.environmentConfig.setGcEnabled(true)
        val expectedConfig = StoreConfig.WITHOUT_DUPLICATES
        val started = System.currentTimeMillis()
        for (j in 0..99) {
            if (System.currentTimeMillis() - started > 30000) break
            println("Cycle $j")
            for (i in 0..99) {
                val txn: Transaction = environment!!.beginTransaction()
                try {
                    do {
                        val name = "testDatabase" + j % 10
                        val store: Store = environment!!.openStore(name, expectedConfig, txn)
                        store.put(txn, StringBinding.stringToEntry("key$i"), StringBinding.stringToEntry("value$i"))
                        txn.revert()
                    } while (!txn.flush())
                } finally {
                    txn.abort()
                }
            }
            Thread.yield()
        }
    }

    @Test
    @TestFor(issue = "XD-590")
    fun issueXD_590_reported() {
        // 1) open store
        val store = environment!!.computeInTransaction { txn: Transaction ->
            environment!!.openStore(
                "store",
                StoreConfig.WITHOUT_DUPLICATES,
                txn
            )
        }
        // 2) store(put) a key 1 , value A1
        environment!!.executeInTransaction { txn: Transaction? ->
            store.put(
                txn!!,
                StringBinding.stringToEntry("key1"),
                StringBinding.stringToEntry("A1")
            )
        }
        // 3) using second transaction : store(put) key 2 value A2,  update key 1 with B1. inside transaction reload ke1 (value=B1 OK)
        environment!!.executeInTransaction { txn: Transaction? ->
            store.put(txn!!, StringBinding.stringToEntry("key2"), StringBinding.stringToEntry("A2"))
            store.put(txn, StringBinding.stringToEntry("key1"), StringBinding.stringToEntry("B1"))
            val value1 = store[txn, StringBinding.stringToEntry("key1")]
            Assert.assertNotNull(value1)
            Assert.assertEquals("B1", StringBinding.entryToString(value1!!))
        }
        // 4) using third transaction : reload key 1 , value is A1 !=B1   !!!!! Error.
        environment!!.executeInTransaction { txn: Transaction? ->
            val value1 = store[txn!!, StringBinding.stringToEntry("key1")]
            Assert.assertNotNull(value1)
            Assert.assertEquals("B1", StringBinding.entryToString(value1!!))
        }
    }

    @Test
    @TestFor(issues = ["XD-594", "XD-717"])
    @Throws(java.lang.Exception::class)
    open fun leakingEnvironment() {
        cleanSubfolders()
        super.tearDown()
        val envRef = WeakReference<Environment>(createAndCloseEnvironment())
        waitForPendingFinalizers(10000)
        Assert.assertNull(envRef.get())
    }


    @Test
    @TestFor(issue = "XD-606")
    open fun mappedFileNotUnmapped() {
        val tempDir = TestUtil.createTempDir()
        try {
            Log.invalidateSharedCache()
            val env = newInstance(
                tempDir,
                EnvironmentConfig().setLogFileSize(1).setLogCachePageSize(1024).setLogCacheShared(false)
            )
            val store = env.computeInTransaction { txn: Transaction ->
                env.openStore(
                    "0", StoreConfig.WITHOUT_DUPLICATES,
                    txn
                )
            }
            env.executeInTransaction { txn: Transaction? ->
                store.put(txn!!, StringBinding.stringToEntry("k"), StringBinding.stringToEntry("v"))
                for (i in 0..199) {
                    store.put(txn, StringBinding.stringToEntry("k$i"), StringBinding.stringToEntry("v$i"))
                }
            }
            Assert.assertEquals("v", env.computeInTransaction { txn: Transaction ->
                StringBinding.entryToString(
                    store[txn, StringBinding.stringToEntry("k")]!!
                )
            })
            env.close()
            val reopenedEnv = newInstance(tempDir, env.environmentConfig)
            val reopenedStore = reopenedEnv.computeInTransaction { txn: Transaction? ->
                reopenedEnv.openStore(
                    "0", StoreConfig.USE_EXISTING,
                    txn!!
                )
            }
            Assert.assertEquals("v", reopenedEnv.computeInTransaction { txn: Transaction ->
                StringBinding.entryToString(
                    reopenedStore[txn, StringBinding.stringToEntry("k")]!!
                )
            })
            reopenedEnv.close()
            Assert.assertTrue(File(tempDir, getLogFilename(0)).renameTo(File(tempDir, getLogFilename(0x1000000))))
        } finally {
            deleteRecursively(tempDir)
        }
    }


    @Test(expected = ExodusException::class)
    @TestFor(issue = "XD-628")
    fun readCloseRace() {
        val store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)
        environment!!.executeInTransaction { txn: Transaction ->
            for (i in 0..9999) {
                store.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
            }
        }
        environment!!.environmentConfig.setEnvCloseForcedly(true)
        environment!!.log.clearCache()
        environment!!.executeInReadonlyTransaction { txn: Transaction ->
            store.openCursor(
                txn
            ).use { cursor ->
                val latch = Latch.create()
                try {
                    latch.acquire()
                    Thread {
                        environment!!.close()
                        latch.release()
                    }.start()
                    latch.acquire()
                } catch (ignore: InterruptedException) {
                }
                while (cursor.next) {
                    Assert.assertNotNull(cursor.key)
                    Assert.assertNotNull(cursor.value)
                }
            }
        }
    }

    @Test
    @TestFor(issue = "XD-682")
    fun cursorOnFlushedTxn() {
        val store = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)
        environment!!.executeInTransaction { txn: Transaction? ->
            store.put(txn!!, IntegerBinding.intToEntry(0), StringBinding.stringToEntry(0.toString()))
            store.put(txn, IntegerBinding.intToEntry(1), StringBinding.stringToEntry(1.toString()))
        }
        environment!!.executeInTransaction { txn: Transaction ->
            store.openCursor(txn).use { cursor ->
                Assert.assertTrue(cursor.next)
                Assert.assertEquals(0, IntegerBinding.entryToInt(cursor.key).toLong())
                store.put(txn, IntegerBinding.intToEntry(2), StringBinding.stringToEntry(2.toString()))
                Assert.assertTrue(txn.flush())
                TestUtil.runWithExpectedException({ cursor.next }, ExodusException::class.java)
                TestUtil.runWithExpectedException({ cursor.prev }, ExodusException::class.java)
                TestUtil.runWithExpectedException({ cursor.key }, ExodusException::class.java)
                TestUtil.runWithExpectedException({ cursor.value }, ExodusException::class.java)
            }
        }
    }

    @Test
    @Throws(InterruptedException::class, IOException::class)
    open fun testSharedCache() {
        environment!!.environmentConfig.setLogCacheShared(true)
        reopenEnvironment()
        val additionalEnvironments: MutableSet<Environment> = HashSet()
        try {
            environment!!.environmentConfig.setGcEnabled(false)
            val numberOfEnvironments = 200
            for (i in 0 until numberOfEnvironments) {
                val rwPair = createReaderWriter(
                    "sub$i"
                )
                additionalEnvironments.add(
                    newEnvironmentInstance(
                        create(rwPair.first, rwPair.second),
                        EnvironmentConfig()
                    )
                )
            }
            val threads = arrayOfNulls<Thread>(numberOfEnvironments)
            println("create data concurrently")
            // create data concurrently
            var i = 0
            for (env in additionalEnvironments) {
                threads[i++] = Thread {
                    val txn = env.beginTransaction()
                    try {
                        val store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
                        for (i12 in 0..9999) {
                            val kv = IntegerBinding.intToEntry(i12)
                            store.put(txn, kv, kv)
                        }
                    } catch (e: java.lang.Exception) {
                        txn.abort()
                    }
                    txn.commit()
                }
            }
            for (thread in threads) {
                thread!!.start()
            }
            for (thread in threads) {
                thread!!.join()
            }
            println("read data concurrently")
            // read data concurrently
            i = 0
            for (env in additionalEnvironments) {
                threads[i++] = Thread {
                    val txn = env.beginTransaction()
                    try {
                        val store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn)
                        for (i1 in 0..9999) {
                            val bi = store[txn, IntegerBinding.intToEntry(i1)]
                            Assert.assertNotNull(bi)
                            Assert.assertEquals(i1.toLong(), IntegerBinding.entryToInt(bi!!).toLong())
                        }
                    } finally {
                        txn.abort()
                    }
                }
            }
            for (thread in threads) {
                thread!!.start()
            }
            for (thread in threads) {
                thread!!.join()
            }
            println("closing environments")
        } finally {
            for (env in additionalEnvironments) {
                env.close()
            }
        }
    }

    @Test
    @TestFor(issue = "XD-770")
    fun alterBalancePolicy() {
        val store = arrayOf(openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES))
        environment!!.executeInTransaction { txn: Transaction? ->
            var i = 0
            while (i < 10000) {
                store[0].put(txn!!, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
                i += 2
            }
        }
        environment!!.executeInReadonlyTransaction { txn: Transaction? ->
            var i = 0
            while (i < 10000) {
                Assert.assertEquals(
                    StringBinding.stringToEntry(i.toString()),
                    store[0][txn!!, IntegerBinding.intToEntry(i)]
                )
                i += 2
            }
        }
        val config = environment!!.environmentConfig
        config.setTreeMaxPageSize(config.treeMaxPageSize / 4)
        reopenEnvironment()
        store[0] = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)
        environment!!.executeInTransaction { txn: Transaction? ->
            var i = 1
            while (i < 10000) {
                store[0].put(txn!!, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
                i += 2
            }
        }
        environment!!.executeInReadonlyTransaction { txn: Transaction? ->
            for (i in 0..9999) {
                Assert.assertEquals(
                    StringBinding.stringToEntry(i.toString()),
                    store[0][txn!!, IntegerBinding.intToEntry(i)]
                )
            }
        }
    }

    @Test
    @TestFor(issue = "XD-770")
    fun alterBalancePolicy2() {
        alterBalancePolicy(0.25f)
    }

    @Test
    @TestFor(issue = "XD-770")
    fun alterBalancePolicy3() {
        alterBalancePolicy(4f)
    }

    @Test
    @TestFor(issue = "XD-770")
    open fun avoidEmptyPages() {
        val store = openStoreAutoCommit("new_store", StoreConfig.WITH_DUPLICATES)
        val count = 1000
        environment!!.executeInTransaction { txn ->
            for (i in 0 until count) {
                store.put(
                    txn,
                    IntegerBinding.intToEntry(i % 600),
                    StringBinding.stringToEntry(i.toString())
                )
            }
        }
        environment!!.executeInTransaction { txn ->
            val rnd = Random()
            for (i in 0..1999) {
                for (j in 0 until count / 2) {
                    store.put(
                        txn,
                        IntegerBinding.intToEntry(rnd.nextInt(600)),
                        StringBinding.stringToEntry(i.toString())
                    )
                }
                for (j in 0 until count / 2) {
                    store.delete(txn, IntegerBinding.intToEntry(rnd.nextInt(600)))
                }
                store.openCursor(txn).use { cursor ->
                    cursor.next
                    var prev = IntegerBinding.entryToInt(cursor.key)
                    store.openCursor(txn).use { c -> Assert.assertNotNull(c.getSearchKey(cursor.key)) }
                    while (cursor.next) {
                        val next = IntegerBinding.entryToInt(cursor.key)
                        Assert.assertTrue(prev <= next)
                        prev = next
                        store.openCursor(txn)
                            .use { c -> Assert.assertNotNull(c.getSearchKey(cursor.key)) }
                    }
                }
            }
        }
    }

    private fun alterBalancePolicy(pageSizeMultiple: Float) {
        val store = arrayOf(openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES))
        val count = 20000
        environment!!.executeInTransaction { txn: Transaction? ->
            for (i in 0 until count) {
                store[0].put(txn!!, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
            }
        }
        environment!!.executeInReadonlyTransaction { txn: Transaction? ->
            for (i in 0 until count) {
                Assert.assertEquals(
                    StringBinding.stringToEntry(i.toString()),
                    store[0][txn!!, IntegerBinding.intToEntry(i)]
                )
            }
        }
        val config = environment!!.environmentConfig
        config.setTreeMaxPageSize((config.treeMaxPageSize * pageSizeMultiple).toInt())
        reopenEnvironment()
        store[0] = openStoreAutoCommit("new_store", StoreConfig.WITHOUT_DUPLICATES)
        environment!!.executeInTransaction { txn: Transaction? ->
            for (i in 0 until count / 5) {
                store[0].delete(txn!!, IntegerBinding.intToEntry(i))
            }
            for (i in 0 until count / 5) {
                store[0].put(txn!!, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(""))
            }
            val rnd = Random()
            for (i in 0 until count / 3) {
                store[0].delete(txn!!, IntegerBinding.intToEntry(rnd.nextInt(count / 4)))
                store[0].put(txn, IntegerBinding.intToEntry(rnd.nextInt(count / 4)), StringBinding.stringToEntry(""))
            }
            for (i in 0 until count / 2) {
                store[0].delete(txn!!, IntegerBinding.intToEntry(i))
            }
        }
        environment!!.executeInReadonlyTransaction { txn: Transaction? ->
            store[0]
                .openCursor(txn!!).use { cursor ->
                    var i: Int = count / 2
                    while (i < count) {
                        if (!cursor.next) break
                        Assert.assertEquals(i.toString(), IntegerBinding.intToEntry(i), cursor.key)
                        ++i
                    }
                    Assert.assertEquals(count.toLong(), i.toLong())
                }
        }
    }

    @Throws(IOException::class)
    private fun createReaderWriter(subfolder: String): Pair<DataReader, DataWriter> {
        val parent = envDirectory
        var child = subfolders[subfolder]
        if (child == null) {
            child = File(parent, subfolder)
            if (child.exists()) throw IOException("SubDirectory already exists $subfolder")
            if (!child.mkdirs()) {
                throw IOException("Failed to create directory $subfolder for tests.")
            }
            subfolders[subfolder] = child
        }
        val reader = FileDataReader(child)
        return Pair(
            reader, AsyncFileDataWriter(reader)
        )
    }

    @Throws(Exception::class)
    override fun tearDown() {
        cleanSubfolders()
        super.tearDown()
    }

    private fun cleanSubfolders() {
        for ((_, file) in subfolders) {
            deleteRecursively(file)
            deleteFile(file)
        }
    }

    @Throws(java.lang.Exception::class)
    protected open fun createAndCloseEnvironment(): EnvironmentImpl? {
        val rw = createRW()
        val env = newEnvironmentInstance(
            create(rw.first, rw.second), EnvironmentConfig().setGcUtilizationFromScratch(true)
        )
        env.close()
        return env
    }

    companion object {
        private fun waitForPendingFinalizers(@Suppress("SameParameterValue") timeoutMillis: Long) {
            val started = System.currentTimeMillis()
            val ref = WeakReference(Any())
            while (ref.get() != null && System.currentTimeMillis() - started < timeoutMillis) {
                System.gc()
                Thread.yield()
            }
        }
    }
}
