/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.gc

import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.*
import org.junit.Assert
import org.junit.Test

open class GarbageCollectorTest : EnvironmentTestsBase() {

    @Test
    fun dummyCleanWholeLog() {
        envDirectory
        val gc = env.gc
        gc.cleanEntireLog()
        gc.doCleanFile(env.log.highFileAddress)
    }

    @Test
    fun updateSameKeyWithoutDuplicates() {
        set1KbFileWithoutGC()
        val key = StringBinding.stringToEntry("key")
        val store = openStoreAutoCommit("updateSameKey")
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun updateSameKeyWithDuplicates() {
        set1KbFileWithoutGC()
        val key = StringBinding.stringToEntry("key")
        val store = openStoreAutoCommit("updateSameKey", getStoreConfig(true))
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun updateSameKeyDeleteWithoutDuplicates() {
        set1KbFileWithoutGC()
        val key = StringBinding.stringToEntry("key")
        val store = openStoreAutoCommit("updateSameKey")
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        deleteAutoCommit(store, key)
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun updateSameKeyDeleteWithDuplicates() {
        set1KbFileWithoutGC()
        val key = StringBinding.stringToEntry("key")
        val store = openStoreAutoCommit("updateSameKey", getStoreConfig(true))
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        deleteAutoCommit(store, key)
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun reopenDbAfterGc() {
        set1KbFileWithoutGC()
        val key = StringBinding.stringToEntry("key")
        var store: Store = openStoreAutoCommit("updateSameKey")
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        Assert.assertEquals(1, countAutoCommit(store))

        env.gc.cleanEntireLog()

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING)
        Assert.assertEquals(1, countAutoCommit(store))

        reopenEnvironment()

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING)
        Assert.assertEquals(1, countAutoCommit(store))
    }

    @Test
    fun reopenDbAfterGcWithBackgroundCleaner() {
        set1KbFileWithoutGC()
        env.environmentConfig.isGcEnabled = true // enable background GC
        val key = StringBinding.stringToEntry("key")
        var store: Store = openStoreAutoCommit("updateSameKey")
        for (i in 0..999) {
            putAutoCommit(store, key, key)
        }
        Assert.assertEquals(1, countAutoCommit(store))

        env.gc.cleanEntireLog()

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING)
        Assert.assertEquals(1, countAutoCommit(store))

        reopenEnvironment()

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING)
        Assert.assertEquals(1, countAutoCommit(store))
    }

    @Test
    @Throws(Exception::class)
    fun reopenDbAfterGcWithBackgroundCleanerCyclic() {
        for (i in 0..7) {
            reopenDbAfterGcWithBackgroundCleaner()
            tearDown()
            setUp()
        }
    }

    @Test
    fun fillDuplicatesThenDeleteAlmostAllOfThem() {
        set1KbFileWithoutGC()

        var store = openStoreAutoCommit("duplicates", getStoreConfig(true))
        for (i in 0..31) {
            for (j in 0..31) {
                putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(j))
            }
        }
        for (i in 0..31) {
            val txn = env.beginTransaction()
            try {
                store.openCursor(txn).use { cursor ->
                    Assert.assertNotNull(cursor.getSearchKeyRange(IntegerBinding.intToEntry(i)))
                    var j = 0
                    while (j < 32) {
                        cursor.deleteCurrent()
                        ++j
                        cursor.next
                    }
                }
                store.put(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(100))
            } finally {
                txn.commit()
            }
        }
        env.gc.cleanEntireLog()

        reopenEnvironment()

        store = openStoreAutoCommit("duplicates", getStoreConfig(true))
        for (i in 0..31) {
            val it = getAutoCommit(store, IntegerBinding.intToEntry(i))
            Assert.assertNotNull(it)
            Assert.assertEquals(100, IntegerBinding.entryToInt(it).toLong())
        }
    }

    @Test
    fun fillDuplicatesWithoutDuplicates() {
        set1KbFileWithoutGC()
        var dups = openStoreAutoCommit("duplicates", getStoreConfig(true))
        putAutoCommit(dups, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0))
        putAutoCommit(dups, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(0))
        putAutoCommit(dups, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(1))
        val nodups = openStoreAutoCommit("no duplicates")
        for (i in 0..999) {
            putAutoCommit(nodups, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(i))
        }

        env.gc.cleanEntireLog()

        reopenEnvironment()

        dups = openStoreAutoCommit("duplicates", getStoreConfig(true))
        Assert.assertNotNull(getAutoCommit(dups, IntegerBinding.intToEntry(0)))
        Assert.assertNotNull(getAutoCommit(dups, IntegerBinding.intToEntry(1)))
        Assert.assertNull(getAutoCommit(dups, IntegerBinding.intToEntry(2)))
    }

    @Test
    fun truncateStore() {
        set2KbFileWithoutGC() // patricia root loggable with 250+ children can't fit one kb
        val store = openStoreAutoCommit("store")
        for (i in 0..999) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
        }
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.executeInTransaction { txn -> env.truncateStore("store", txn) }

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun removeStore() {
        set2KbFileWithoutGC() // patricia root loggable with 250+ children can't fit one kb
        val store = openStoreAutoCommit("store")
        for (i in 0..999) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
        }
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.executeInTransaction { txn -> env.removeStore("store", txn) }

        env.gc.cleanEntireLog()

        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    fun removeStoreCreateStoreGet() {
        set2KbFileWithoutGC() // patricia root loggable with 250+ children can't fit one kb
        var store: Store = openStoreAutoCommit("store")
        val count = 500
        for (i in 0 until count) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
        }
        Assert.assertTrue(env.log.numberOfFiles > 1)

        env.gc.cleanEntireLog()

        env.executeInTransaction { txn -> env.removeStore("store", txn) }

        store = openStoreAutoCommit("store")
        for (i in 0 until count) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
        }

        env.gc.cleanEntireLog()

        for (i in 0 until count) {
            Assert.assertEquals(
                i.toLong(),
                IntegerBinding.entryToInt(getAutoCommit(store, IntegerBinding.intToEntry(i))).toLong()
            )
        }
    }

    @Test
    fun xd98() {
        setLogFileSize(64)
        env.environmentConfig.isGcEnabled = false
        val store = openStoreAutoCommit("store")
        val builder = StringBuilder()
        for (i in 0..99) {
            builder.append("01234567890123456789012345678901234567890123456789")
            val value = StringBinding.stringToEntry(builder.toString())
            putAutoCommit(store, IntegerBinding.intToEntry(0), value)
        }
        env.gc.cleanEntireLog()
        Assert.assertEquals(1L, env.log.numberOfFiles)
    }

    @Test
    @TestFor(issue = "XD-780")
    fun `stackoverflow-com-questions-56662998`() {
        env.environmentConfig.run {
            gcStartIn = 0
            gcFileMinAge = 1
            gcFilesDeletionDelay = 0
            gcMinUtilization = 90
        }
        setLogFileSize(1)
        val store = openStoreAutoCommit("store")
        env.executeInExclusiveTransaction { txn ->
            for (i in 1..500) {
                store.putRight(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
            }
        }
        Assert.assertTrue(env.log.numberOfFiles > 3L)
        env.executeInExclusiveTransaction { txn ->
            store.openCursor(txn).use { cursor ->
                cursor.forEach { deleteCurrent() }
            }
        }
        env.gc()
        env.gc.waitForPendingGC()
        Assert.assertTrue(env.log.numberOfFiles <= 3L)
    }

    @Test
    @TestFor(issue = "XD-780")
    fun `stackoverflow-com-questions-56662998+`() {
        env.environmentConfig.run {
            gcStartIn = 0
            gcFileMinAge = 1
            gcFilesDeletionDelay = 0
            gcMinUtilization = 90
        }
        setLogFileSize(1)
        val store = openStoreAutoCommit("store")
        env.executeInExclusiveTransaction { txn ->
            for (i in 1..500) {
                store.putRight(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
            }
        }
        Assert.assertTrue(env.log.numberOfFiles > 3L)
        env.executeInExclusiveTransaction { txn ->
            env.truncateStore("store", txn)
        }
        env.gc()
        env.gc.waitForPendingGC()
        Assert.assertTrue(env.log.numberOfFiles <= 3L)
    }

    protected fun openStoreAutoCommit(name: String): StoreImpl {
        return openStoreAutoCommit(name, getStoreConfig(false)) as StoreImpl
    }

    protected open fun getStoreConfig(hasDuplicates: Boolean): StoreConfig {
        return if (hasDuplicates) StoreConfig.WITH_DUPLICATES else StoreConfig.WITHOUT_DUPLICATES
    }

    private fun createStore(name: String, keys: Int): Long {
        val txn = env.beginTransaction()
        val store = env.openStore(name, getStoreConfig(false), txn)
        for (i in 0 until keys) {
            store.put(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
        }
        val result = env.log.highAddress
        txn.commit()
        return result
    }
}
