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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogTests.Companion.adjustedLogFileSize
import jetbrains.exodus.log.NullLoggable.create
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.ExpiredLoggableCollection.Companion.newInstance
import org.junit.Assert
import org.junit.Test

class BTreeReclaimSpecialTest : BTreeTestBase() {
    override fun createLogConfig(): LogConfig {
        val result = super.createLogConfig()
        result!!.setFileSize(1)
        return result
    }

    @Test
    fun testStartAddress() {
        val fileSize: Long = log!!.fileLengthBound
        val adjustedFileSize = adjustedLogFileSize(fileSize, log!!.cachePageSize)
        log!!.beginWrite()
        for (l in 1 until adjustedFileSize) { // fill all file except for one byte with nulls
            val expired = newInstance(log!!)
            log!!.write(create(), expired)
        }
        log!!.flush()
        log!!.endWrite()
        Assert.assertEquals(1, log!!.numberOfFiles)
        Assert.assertTrue(log!!.highAddress < fileSize)
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        val key: ArrayByteIterable = key("K")
        for (i in 0..COUNT) {
            treeMutable!!.put(key, v(i))
        }
        var saved = saveTree()
        reloadMutableTree(saved)
        Assert.assertEquals(4, log!!.numberOfFiles)
        val address = 0L
        log!!.forgetFile(address)
        log!!.removeFile(address) // emulate gc of first file
        var loggables: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(
            log!!.getFileAddress(fileSize * 2)
        )
        treeMutable!!.reclaim(loggables.next(), loggables) // reclaim third file
        saved = saveTree()
        reloadMutableTree(saved)
        log!!.forgetFile(fileSize * 2)
        log!!.removeFile(fileSize * 2) // remove reclaimed file
        loggables =
            log!!.getLoggableIterator(log!!.getFileAddress(fileSize))
        treeMutable!!.reclaim(loggables.next(), loggables) // reclaim second file
        saved = saveTree()
        reloadMutableTree(saved)
        Assert.assertTrue(log!!.numberOfFiles > 2) // make sure that some files were added
        log!!.forgetFile(fileSize)
        log!!.removeFile(fileSize) // remove reclaimed file
        treeMutable!!.openCursor().use { cursor ->
            Assert.assertTrue(cursor.next) // access minimum key
        }
    }

    @Test
    fun testDups() {
        treeMutable = BTreeEmpty(log!!, true, 1).getMutableCopy()
        treeMutable!!.put(key("k"), value("v0"))
        treeMutable!!.put(key("k"), value("v1"))
        val firstAddress = saveTree()
        reloadMutableTree(firstAddress)
        treeMutable!!.put(key("k"), value("v2"))
        treeMutable!!.put(key("k"), value("v3"))
        saveTree()
        var loggables: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(0)
        treeMutable!!.reclaim(loggables.next(), loggables)
        loggables = log!!.getLoggableIterator(firstAddress)
        loggables.next()
        treeMutable!!.reclaim(loggables.next(), loggables)
    }

    private fun reloadMutableTree(address: Long) {
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, address, true, 1).getMutableCopy()
    }

    companion object {
        private const val COUNT = 145
    }
}
