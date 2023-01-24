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
package jetbrains.exodus.gc

import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.EnvironmentTestsBase
import jetbrains.exodus.env.StoreConfig
import org.junit.Assert
import org.junit.Test

open class GarbageCollectorInterleavingTest : EnvironmentTestsBase() {

    protected open val storeConfig: StoreConfig
        get() = StoreConfig.WITHOUT_DUPLICATES

    protected open val recordsNumber: Int
        get() = 37

    @Test
    @Throws(InterruptedException::class)
    fun testSimple() {
        set1KbFileWithoutGC()

        val log = env.log
        val fileSize = log.fileLengthBound

        fill("updateSameKey")
        Assert.assertEquals(1L, log.numberOfFiles)
        fill("updateSameKey")

        Assert.assertEquals(2L, log.numberOfFiles) // but ends in second one

        fill("another")

        Assert.assertEquals(3L, log.numberOfFiles) // make cleaning of second file possible

        env.gc.doCleanFile(fileSize) // clean second file

        Thread.sleep(300)
        env.gc.testDeletePendingFiles()

        Assert.assertEquals(3L, log.numberOfFiles) // half of tree written out from second file

        env.gc.doCleanFile(0) // clean first file

        Thread.sleep(300)
        env.gc.testDeletePendingFiles()

        Assert.assertEquals(2L, log.numberOfFiles) // first file contained only garbage

        check("updateSameKey")
        check("another")
    }

    private fun fill(table: String) {
        val val0 = StringBinding.stringToEntry("val0")
        env.executeInTransaction { txn ->
            val store = env.openStore(table, storeConfig, txn)
            for (i in 0 until recordsNumber) {
                val key = StringBinding.stringToEntry("key $i")
                store.put(txn, key, val0)
            }
        }
    }

    private fun check(table: String) {
        val val0 = StringBinding.stringToEntry("val0")
        env.executeInReadonlyTransaction { txn ->
            val store = env.openStore(table, storeConfig, txn)
            for (i in 0 until recordsNumber) {
                val key = StringBinding.stringToEntry("key $i")
                Assert.assertTrue(store.exists(txn, key, val0))
            }
        }
    }
}
