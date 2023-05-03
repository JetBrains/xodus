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
import jetbrains.exodus.core.execution.LatchJob
import jetbrains.exodus.env.Environments.newContextualInstance
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.log.LogConfig.Companion.create
import jetbrains.exodus.tree.btree.BTreeBase
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.IOUtil.deleteRecursively
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestName

class TransactionTest : EnvironmentTestsBase() {
    @JvmField
    @Rule
    var name = TestName()
    override fun createEnvironment() {
        val methodName = name.methodName
        environment =
            if (methodName.contains("XD_471") || methodName.contains("XD_478") || methodName.contains("testAfter")) newEnvironmentInstance(
                create(
                    reader!!, writer!!
                )
            ) else newContextualEnvironmentInstance(create(reader!!, writer!!))
    }

    @Test
    fun testCurrentTransaction() {
        val env = environment as ContextualEnvironment
        var txn = env.beginTransaction()
        Assert.assertEquals(txn, env.currentTransaction)
        txn.abort()
        Assert.assertNull(env.currentTransaction)
        txn = env.beginTransaction()
        Assert.assertEquals(txn, env.currentTransaction)
        val txn1 = env.beginTransaction()
        Assert.assertEquals(txn1, env.currentTransaction)
        txn1.commit()
        txn.commit()
        Assert.assertNull(env.currentTransaction)
    }

    @Test
    fun testCommitTwice() {
        TestUtil.runWithExpectedException({
            val txn: Transaction = environment!!.beginTransaction()
            txn.commit()
            txn.commit()
        }, TransactionFinishedException::class.java)
    }

    @Test
    fun testAbortTwice() {
        TestUtil.runWithExpectedException({
            val txn: Transaction = environment!!.beginTransaction()
            txn.abort()
            txn.abort()
        }, TransactionFinishedException::class.java)
    }

    @Test
    fun testNestedTransactions() {
        val txn: Transaction = environment!!.beginTransaction()
        val txn1: Transaction = environment!!.beginTransaction()
        TestUtil.runWithExpectedException({ txn.commit() }, ExodusException::class.java)
        txn1.commit()
        txn.commit()
    }

    @Test
    fun testAtomicity() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        val entry1 = StringBinding.stringToEntry("1")
        store.put(txn, entry1, entry1)
        val entry2 = StringBinding.stringToEntry("2")
        store.put(txn, entry2, entry2)
        txn.commit()
        // all changes should be placed in single snapshot
        assertLoggableTypes(
            log,
            0,
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.BOTTOM_ROOT.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.LEAF.toInt(),
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt()
        )
    }

    @Test
    fun testAbort() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        val entry1 = StringBinding.stringToEntry("1")
        store.put(txn, entry1, entry1)
        val entry2 = StringBinding.stringToEntry("2")
        store.put(txn, entry2, entry2)
        txn.abort()
        // no changes should be written since transaction was not committed
        assertLoggableTypes(
            log,
            0,
            BTreeBase.BOTTOM_ROOT.toInt(),
            DatabaseRoot.DATABASE_ROOT_TYPE.toInt()
        )
    }

    @Test
    fun testReadCommitted() {
        val env: Environment? = environment
        val key: ByteIterable = StringBinding.stringToEntry("key")
        var txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, key, StringBinding.stringToEntry("value"))
        var t = env.beginTransaction()
        Assert.assertNull(store[t, key])
        t.commit()
        txn.commit()
        txn = env.beginTransaction()
        store.put(txn, key, StringBinding.stringToEntry("value1"))
        t = env.beginTransaction()
        assertNotNullStringValue(store, key, "value")
        t.commit()
        txn.commit()
        assertNotNullStringValue(store, key, "value1")
    }

    @Test
    fun testReadUncommitted() {
        val env: Environment? = environment
        val key: ByteIterable = StringBinding.stringToEntry("key")
        var txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.commit()
        txn = env.beginTransaction()
        store.put(txn, key, StringBinding.stringToEntry("value"))
        assertNotNullStringValue(store, key, "value")
        txn.commit()
    }

    @Test
    fun testRepeatableRead() {
        val env: Environment? = environment
        val key: ByteIterable = StringBinding.stringToEntry("key")
        var txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, key, StringBinding.stringToEntry("value"))
        assertNotNullStringValue(store, key, "value")
        txn.commit()
        assertNotNullStringValue(store, key, "value")
        txn = env.beginTransaction()
        assertNotNullStringValue(store, key, "value")
        executeParallelTransaction { txn1: Transaction? ->
            store.put(
                txn1!!,
                key,
                StringBinding.stringToEntry("value1")
            )
        }
        assertNotNullStringValue(store, key, "value")
        txn.abort()
        assertNotNullStringValue(store, key, "value1")
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTransactionSafeJob() {
        val bTrue = booleanArrayOf(false)
        val bFalse = booleanArrayOf(true)
        val txn: Transaction = environment!!.beginTransaction()
        val txn1: Transaction = environment!!.beginTransaction()
        environment!!.executeTransactionSafeTask { bTrue[0] = true }
        environment!!.executeTransactionSafeTask { bFalse[0] = false }
        Thread.sleep(500)
        Assert.assertFalse(bTrue[0])
        Assert.assertTrue(bFalse[0])
        txn1.abort()
        Thread.sleep(500)
        Assert.assertFalse(bTrue[0])
        Assert.assertTrue(bFalse[0])
        txn.abort()
        Thread.sleep(500)
        Assert.assertTrue(bTrue[0])
        Assert.assertFalse(bFalse[0])
    }

    @Test
    fun testFlush() {
        val ok = booleanArrayOf(true)
        val env: Environment? = environment
        val key1: ByteIterable = StringBinding.stringToEntry("key1")
        val key2: ByteIterable = StringBinding.stringToEntry("key2")
        var txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.commit()
        txn = env.beginTransaction()
        store.put(txn, key1, StringBinding.stringToEntry("value1"))
        executeParallelTransaction { txn12: Transaction? ->
            try {
                assertEmptyValue(txn12, store, key1)
                assertEmptyValue(txn12, store, key2)
            } catch (t: Throwable) {
                ok[0] = false
            }
        }
        txn.flush()
        store.put(txn, key2, StringBinding.stringToEntry("value2"))
        executeParallelTransaction { txn1: Transaction? ->
            try {
                assertNotNullStringValue(txn1, store, key1, "value1")
                assertEmptyValue(txn1, store, key2)
            } catch (t: Throwable) {
                ok[0] = false
            }
        }
        txn.flush()
        txn.abort()
        Assert.assertTrue(ok[0])
        assertNotNullStringValue(store, key1, "value1")
        assertNotNullStringValue(store, key2, "value2")
    }

    @Test
    fun testRevert() {
        val env: Environment? = environment
        val key1: ByteIterable = StringBinding.stringToEntry("key1")
        val key2: ByteIterable = StringBinding.stringToEntry("key2")
        var txn = env!!.beginTransaction()
        val store = env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.commit()
        txn = env.beginTransaction()
        store.put(txn, key1, StringBinding.stringToEntry("value1"))
        executeParallelTransaction { txn1: Transaction? ->
            store.put(
                txn1!!,
                key2,
                StringBinding.stringToEntry("value2")
            )
        }
        assertNotNullStringValue(store, key1, "value1")
        assertEmptyValue(store, key2)
        txn.revert()
        assertEmptyValue(store, key1)
        assertNotNullStringValue(store, key2, "value2")
        txn.abort()
    }

    @Test(expected = ReadonlyTransactionException::class)
    fun testExecuteInReadonlyTransaction() {
        val env = environment
        env!!.executeInReadonlyTransaction { txn: Transaction? ->
            env.openStore(
                "WTF",
                StoreConfig.WITHOUT_DUPLICATES,
                txn!!
            )
        }
    }

    @Test(expected = ReadonlyTransactionException::class)
    @TestFor(issue = "XD-447")
    fun test_XD_447() {
        val env = environment
        val ec = env!!.environmentConfig
        ec.setEnvIsReadonly(true)
        ec.setEnvReadonlyEmptyStores(true)
        env.executeInTransaction { txn: Transaction? ->
            val store = env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn!!)
            val wtfEntry = StringBinding.stringToEntry("WTF")
            store.put(txn, wtfEntry, wtfEntry)
        }
    }

    @Test(expected = ReadonlyTransactionException::class)
    @TestFor(issue = "XD-447")
    fun test_XD_447_() {
        val env = environment
        val ec = env!!.environmentConfig
        ec.setEnvIsReadonly(true)
        ec.setEnvReadonlyEmptyStores(true)
        env.executeInTransaction { txn: Transaction? ->
            val store = env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn!!)
            store.delete(txn, StringBinding.stringToEntry("WTF"))
        }
    }

    private inner class TestFailFastException : RuntimeException()

    @Test(expected = TestFailFastException::class)
    fun testNoFailFast() {
        val env = environment
        val ec = env!!.environmentConfig
        ec.setEnvIsReadonly(true)
        ec.setEnvFailFastInReadonly(false)
        env.executeInTransaction { txn: Transaction? ->
            val store = env.openStore("WTF", StoreConfig.WITHOUT_DUPLICATES, txn!!)
            val wtfEntry = StringBinding.stringToEntry("WTF")
            store.put(txn, wtfEntry, wtfEntry)
            throw TestFailFastException()
        }
    }

    @Test
    @TestFor(issue = "XD-401")
    @Throws(Exception::class)
    fun test_XD_401() {
        val env: Environment? = environment
        val store = env!!.computeInTransaction { txn: Transaction? ->
            env.openStore(
                "store",
                StoreConfig.WITH_DUPLICATES,
                txn!!
            )
        }
        val txn = env.beginTransaction()
        val started = txn.startTime
        store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("value"))
        Thread.sleep(200)
        try {
            Assert.assertTrue(txn.flush())
            Assert.assertTrue(txn.startTime > started + 150)
            store.put(txn, StringBinding.stringToEntry("key"), StringBinding.stringToEntry("new value"))
            Thread.sleep(200)
            txn.revert()
            Assert.assertTrue(txn.startTime > started + 350)
        } finally {
            txn.abort()
        }
    }

    @Test
    @TestFor(issue = "XD-471")
    fun test_XD_471() {
        val env: Environment? = environment
        val txn = arrayOf<Transaction?>(null)
        DeferredIO.getJobProcessor().waitForLatchJob(object : LatchJob() {
            override fun execute() {
                txn[0] = env!!.beginTransaction()
                release()
            }
        }, 100)
        val tx = txn[0]
        Assert.assertNotNull(tx)
        tx!!.abort()
    }

    @Test
    @TestFor(issue = "XD-471")
    fun test_XD_471_() {
        val env: Environment? = environment
        val txn = arrayOf<Transaction?>(null)
        DeferredIO.getJobProcessor().waitForLatchJob(object : LatchJob() {
            override fun execute() {
                txn[0] = env!!.beginReadonlyTransaction()
                release()
            }
        }, 100)
        val tx = txn[0]
        Assert.assertNotNull(tx)
        tx!!.abort()
    }

    @Test(expected = ExodusException::class)
    fun testAfterCommit() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
            .put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.commit()
        env.openStore("new store1", StoreConfig.WITHOUT_DUPLICATES, txn)
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterCommit2() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.commit()
        store.delete(txn, IntegerBinding.intToEntry(0))
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterCommit3() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.commit()
        txn.commit()
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterCommit4() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.commit()
        txn.abort()
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterAbort() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
            .put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.abort()
        env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterAbort2() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.flush()
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.abort()
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterAbort3() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.flush()
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.abort()
        txn.abort()
        Assert.fail()
    }

    @Test(expected = ExodusException::class)
    fun testAfterAbort4() {
        val env: Environment? = environment
        val txn = env!!.beginTransaction()
        val store = env.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
        txn.flush()
        store.put(txn, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        txn.abort()
        txn.commit()
        Assert.fail()
    }

    @Test
    @TestFor(issue = "XD-477")
    fun test_XD_477() {
        environment!!.environmentConfig.setEnvTxnReplayTimeout(500L)
        environment!!.executeInTransaction { txn: Transaction ->
            environment!!.openStore("new store", StoreConfig.WITHOUT_DUPLICATES, txn)
            environment!!.executeInTransaction { txn1: Transaction ->
                environment!!.openStore(
                    "new store 2",
                    StoreConfig.WITHOUT_DUPLICATES,
                    txn1
                )
            }
            txn.flush()
            Assert.assertFalse(txn.isExclusive)
            txn.revert()
            Assert.assertFalse(txn.isExclusive)
            // here transaction is idempotent and not exclusive
            try {
                Thread.sleep(600)
            } catch (ignore: InterruptedException) {
                Thread.currentThread().interrupt()
            }
            txn.revert()
            Assert.assertFalse(txn.isExclusive)
        }
    }

    @Test
    @TestFor(issue = "XD-478")
    fun test_XD_478() {
        val env = environment
        val store = env!!.computeInTransaction { txn: Transaction? ->
            env.openStore(
                "store",
                StoreConfig.WITHOUT_DUPLICATES,
                txn!!
            )
        }
        val txn = env.beginTransaction()
        try {
            Assert.assertFalse(
                store.exists(
                    txn,
                    StringBinding.stringToEntry("key"),
                    StringBinding.stringToEntry("value")
                )
            )
            env.executeInTransaction { tx: Transaction? ->
                store.put(
                    tx!!,
                    StringBinding.stringToEntry("key"),
                    StringBinding.stringToEntry("value")
                )
            }
            txn.revert()
            Assert.assertTrue(
                store.exists(
                    txn,
                    StringBinding.stringToEntry("key"),
                    StringBinding.stringToEntry("value")
                )
            )
        } finally {
            txn.abort()
        }
    }

    @Test
    @TestFor(issue = "XD-480") // the test will hang if XD-480 is not fixed
    fun testSuspendGCInTxn() {
        set1KbFileWithoutGC()
        val env = environment
        env!!.environmentConfig.setGcEnabled(true)
        env.environmentConfig.setGcMinUtilization(90)
        env.environmentConfig.setGcStartIn(0)
        val store = env.computeInTransaction(
            TransactionalComputable<Store> { txn: Transaction? ->
                env.openStore(
                    "new store",
                    StoreConfig.WITHOUT_DUPLICATES,
                    txn!!
                )
            })
        env.executeInTransaction {
            for (i in 0..29) {
                env.executeInTransaction { txn: Transaction? ->
                    store.put(
                        txn!!, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(
                            i
                        )
                    )
                    store.put(txn, IntegerBinding.intToEntry(i / 2), IntegerBinding.intToEntry(i))
                }
            }
        }
        env.executeInTransaction {
            try {
                Thread.sleep(500)
            } catch (ignore: InterruptedException) {
                Thread.currentThread().interrupt()
            }
            env.suspendGC()
        }
    }

    @Test
    @TestFor(issue = "XD-789")
    @Throws(InterruptedException::class)
    fun testExpiration() {
        val dir = TestUtil.createTempDir()
        try {
            testTxnExpirationTimeout(
                newInstance(
                    dir,
                    EnvironmentConfig().setEnvMonitorTxnsExpirationTimeout(1000).setEnvCloseForcedly(true)
                )
            )
        } finally {
            deleteRecursively(dir)
        }
    }

    @Test
    @TestFor(issue = "XD-789")
    @Throws(InterruptedException::class)
    fun testExpirationContextual() {
        val dir = TestUtil.createTempDir()
        try {
            testTxnExpirationTimeout(
                newContextualInstance(
                    dir,
                    EnvironmentConfig().setEnvMonitorTxnsExpirationTimeout(1000).setEnvCloseForcedly(true)
                )
            )
        } finally {
            deleteRecursively(dir)
        }
    }

    @Test
    @TestFor(question = "https://stackoverflow.com/questions/64203125/check-active-transactions-within-a-xodus-environment")
    fun testForcedCloseWithinTxn() {
        environment!!.environmentConfig.setEnvCloseForcedly(true)
        val txn = environment!!.beginReadonlyTransaction()
        environment!!.executeTransactionSafeTask { environment!!.executeInExclusiveTransaction { environment!!.close() } }
        Assert.assertTrue(environment!!.isOpen)
        txn.abort()
        Assert.assertFalse(environment!!.isOpen)
    }

    @Throws(InterruptedException::class)
    private fun testTxnExpirationTimeout(env: Environment) {
        env.use {
            val txn = it.beginTransaction()
            Thread.sleep(5000)
            Assert.assertTrue(txn.isFinished)
        }
    }
}
