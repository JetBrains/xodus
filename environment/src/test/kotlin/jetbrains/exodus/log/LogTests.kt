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
package jetbrains.exodus.log

import jetbrains.exodus.*
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getLong
import jetbrains.exodus.log.NullLoggable.create
import jetbrains.exodus.tree.ExpiredLoggableCollection.Companion.newInstance
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test

open class LogTests : LogTestsBase() {
    @Test
    @Ignore
    fun testWrite() {
        initLog(6, 1024) // file size must be multiple of 3 to avoid alignment
        val logFileSizeInBytes = adjustedLogFileSize(log.fileLengthBound, 1024)
        for (i in 0..99) {
            log.beginWrite()
            val expired = newInstance(log)
            for (j in 0 until logFileSizeInBytes) {
                Assert.assertEquals(
                    expectedAddress(3 * (j + i * logFileSizeInBytes), 1024),
                    log.write(DUMMY_LOGGABLE, expired)
                )
            }
            log.endWrite()
            Assert.assertEquals(
                (3 * (i + 1)).toLong(),
                log.numberOfFiles.toInt().toLong()
            ) // each DUMMY_LOGGABLE takes 3 bytes
        }
    }

    @Test
    fun testHighestPage() {
        initLog(1, 1024)
        val emptyLoggable: Loggable = create()
        log.beginWrite()
        for (i in 0..1023) {
            val expired = newInstance(log)
            log.write(emptyLoggable, expired)
        }
        log.flush()
        log.endWrite()
        closeLog()
        initLog(1, 1024)
    }

    @Test
    @Ignore
    fun testWrite2() {
        initLog(111, 1024) // file size must be multiple of 3 to avoid alignment
        log.beginWrite()
        for (j in 0 until adjustedLogFileSize((111 * 1024).toLong(), 1024)) {
            val expired = newInstance(log)
            Assert.assertEquals(
                expectedAddress(3L * j, 1024),
                log.write(DUMMY_LOGGABLE, expired).toInt().toLong()
            )
        }
        log.flush()
        log.endWrite()
        Assert.assertEquals(3, log.numberOfFiles.toInt().toLong()) // each DUMMY_LOGGABLE takes 3 bytes
    }

    @Test
    fun testRemoveFile() {
        initLog(1, 1024)
        log.beginWrite()
        for (j in 0 until adjustedLogFileSize(1024, 1024) * 99) {
            val expired = newInstance(log)
            log.write(create(), expired)
        }
        log.flush()
        log.forgetFiles(longArrayOf(0, 1024, 8192, 32768))
        log.endWrite()
        log.removeFile(0)
        log.removeFile(1024)
        log.removeFile(8192)
        log.removeFile(32768)
        Assert.assertEquals(95, log.numberOfFiles.toInt().toLong()) // null loggable should take only one byte
    }

    @Test
    fun testRemoveFileInvalidAddress() {
        initLog(1, 1024)
        log.beginWrite()
        for (j in 0 until adjustedLogFileSize(1024, 1024) * 10) {
            val expired = newInstance(log)
            log.write(create(), expired)
        }
        log.flush()
        log.endWrite()
        TestUtil.runWithExpectedException({ log.removeFile(1111) }, ExodusException::class.java)
    }

    @Test
    fun testRemoveFileNonexistentAddress() {
        initLog(1, 1024)
        log.beginWrite()
        for (j in 0 until adjustedLogFileSize(1024, 1024) * 10) {
            val expired = newInstance(log)
            log.write(create(), expired)
        }
        log.flush()
        log.endWrite()
        TestUtil.runWithExpectedException({ log.removeFile((1024 * 10).toLong()) }, ExodusException::class.java)
    }

    @Test
    fun testPaddingWithNulls() {
        initLog(1, 1024)
        log.beginWrite()
        for (i in 0..99) {
            val expired = newInstance(log)
            log.write(DUMMY_LOGGABLE, expired)
            log.doPadWithNulls(expired)
            Assert.assertEquals((i + 1).toLong(), log.getWrittenFilesSize().toLong())
        }
        log.flush()
        log.endWrite()
    }

    @Test
    fun testWriteNulls() {
        initLog(1, 1024)
        log.beginWrite()
        for (j in 0 until adjustedLogFileSize(1024, 1024) * 99) {
            val expired = newInstance(log)
            log.write(create(), expired)
        }
        log.flush()
        log.endWrite()
        Assert.assertEquals(99, log.numberOfFiles.toInt().toLong()) // null loggable should take only one byte
    }

    @Test
    fun testAutoAlignment() {
        initLog(1, 1024)
        log.beginWrite()
        for (i in 0 until adjustedLogFileSize(1024, 1024) - 1) {
            val expired = newInstance(log)
            log.write(create(), expired)
        }
        val expired = newInstance(log)
        // here auto-alignment should happen
        val dummyAddress = log.write(DUMMY_LOGGABLE, expired)
        log.flush()
        log.endWrite()
        Assert.assertEquals(2, log.numberOfFiles.toInt().toLong())
        // despite the fact that DUMMY_LOGGABLE size + 1023 nulls should result in 1025 bytes,
        // we should actually get 1026 (one kb + 2) due to automatic alignment
        Assert.assertEquals(1027, log.highAddress.toInt().toLong())
        // by the same reason, address of dummy should be at the beginning of the second file
        Assert.assertEquals(1024, dummyAddress.toInt().toLong())
    }

    @Test
    fun testWriteReadSameAddress() {
        initLog(1, 1024)
        for (i in 0..99) {
            log.beginWrite()
            val expired = newInstance(log)
            val dummyAddress = log.write(DUMMY_LOGGABLE, expired)
            log.flush()
            log.endWrite()
            Assert.assertEquals(expectedAddress(i * 3L, 1024), dummyAddress)
            Assert.assertEquals(dummyAddress, log.read(dummyAddress).address)
        }
    }

    @Test
    fun testAutoAlignment2() {
        initLog(1, 1024)
        // one kb loggable can't be placed in a single file of one kb size
        TestUtil.runWithExpectedException({
            log.beginWrite()
            val expired = newInstance(log)
            log.write(ONE_KB_LOGGABLE, expired)
        }, TooBigLoggableException::class.java)
    }

    @Test
    fun testReadUnknownLoggableType() {
        log.beginWrite()
        val expired = newInstance(log)
        log.write(DUMMY_LOGGABLE, expired)
        log.flush()
        log.endWrite()
        log.read(0)
    }

    @Test
    fun testWriteImmediateRead() {
        testWriteImmediateRead(1, 1024)
    }

    @Test
    fun testWriteImmediateRead2() {
        testWriteImmediateRead(4, 1024 * 4)
    }

    @Test
    fun testWriteImmediateRead3() {
        testWriteImmediateRead(16, 1024 * 16)
    }

    @Test
    fun testWriteImmediateRead4() {
        testWriteImmediateRead(2, 1024)
    }

    @Test
    fun testWriteSequentialRead() {
        testWriteSequentialRead(1, 1024)
    }

    @Test
    fun testWriteSequentialRead2() {
        testWriteSequentialRead(4, 1024 * 4)
    }

    @Test
    fun testWriteSequentialRead3() {
        testWriteSequentialRead(16, 1024 * 16)
    }

    @Test
    fun testWriteSequentialRead4() {
        testWriteSequentialRead(2, 1024)
    }

    @Test
    fun testWriteRandomRead() {
        testWriteRandomRead(1, 1024)
    }

    @Test
    fun testWriteRandomRead2() {
        testWriteRandomRead(4, 1024 * 4)
    }

    @Test
    fun testWriteRandomRead3() {
        testWriteRandomRead(16, 1024 * 16)
    }

    @Test
    fun testWriteRandomRead4() {
        testWriteRandomRead(2, 1024)
    }

    @Test
    fun testAllLoggablesIterator() {
        initLog(4, 1024 * 4)
        val count = 10
        log.beginWrite()
        for (i in 0 until count) {
            writeData(getIterable(i.toLong()))
        }
        log.flush()
        log.endWrite()
        val it: Iterator<RandomAccessLoggable> = log.getLoggableIterator(0)
        var i = 0
        while (it.hasNext()) {
            val l: Loggable = it.next()
            Assert.assertEquals((4 * i++).toLong(), l.address)
            Assert.assertEquals(126, l.type.toLong())
            Assert.assertEquals(1, l.dataLength.toLong())
        }
        Assert.assertEquals(count.toLong(), i.toLong())
    }

    private fun writeData(iterable: ByteIterable): Long {
        val expired = newInstance(log)
        return log.write(126.toByte(), Loggable.NO_STRUCTURE_ID, iterable, expired)
    }

    @Test
    fun testAllRandomAccessLoggablesIterator() {
        initLog(4, 1024 * 4)
        val count = 10
        log.beginWrite()
        for (i in 0 until count) {
            writeData(getIterable(i.toLong()))
        }
        log.flush()
        log.endWrite()
        val it: Iterator<RandomAccessLoggable> = log.getLoggableIterator(0)
        var i = 0
        while (it.hasNext()) {
            val l: Loggable = it.next()
            Assert.assertEquals((4 * i++).toLong(), l.address)
            Assert.assertEquals(126, l.type.toLong())
            Assert.assertEquals(1, l.dataLength.toLong())
        }
        Assert.assertEquals(count.toLong(), i.toLong())
    }

    private fun testWriteImmediateRead(fileSize: Int, pageSize: Int) {
        initLog(fileSize.toLong(), pageSize)
        val count = 50000
        for (i in 0 until count) {
            log.beginWrite()
            val addr = writeData(getIterable(i.toLong()))
            log.flush()
            log.endWrite()
            Assert.assertEquals(
                i.toLong(), getLong(log.read(addr).data).toInt()
                    .toLong()
            )
        }
    }

    private fun testWriteSequentialRead(fileSize: Int, pageSize: Int) {
        initLog(fileSize.toLong(), pageSize)
        val count = 50000
        val addrs = LongArrayList()
        log.beginWrite()
        for (i in 0 until count) {
            addrs.add(writeData(getIterable(i.toLong())))
        }
        log.flush()
        log.endWrite()
        for (i in 0 until count) {
            Assert.assertEquals(
                i.toLong(), getLong(log.read(addrs[i]).data).toInt()
                    .toLong()
            )
        }
    }

    private fun testWriteRandomRead(fileSize: Int, pageSize: Int) {
        initLog(fileSize.toLong(), pageSize)
        val count = 50000
        val addrs = LongHashMap<Int>()
        log.beginWrite()
        for (i in 0 until count) {
            val addr = writeData(getIterable(i.toLong()))
            addrs[addr] = Integer.valueOf(i)
        }
        log.flush()
        log.endWrite()
        for (addr in addrs.keys) {
            Assert.assertEquals(
                (addrs[addr] as Int).toLong(),
                getLong(log.read(addr).data).toInt().toLong()
            )
            log.read(addr)
        }
    }

    companion object {
        private val DUMMY_LOGGABLE: Loggable = 1.toByte().createNoDataLoggable()
        private val ONE_KB_LOGGABLE: Loggable = createOneKbLoggable()
        private fun Byte.createNoDataLoggable(): TestLoggable {
            return TestLoggable(this, ByteIterable.EMPTY, Loggable.NO_STRUCTURE_ID)
        }

        fun expectedAddress(written: Long, pageSize: Int): Long {
            val adjustedPageSize = pageSize - BufferedDataWriter.HASH_CODE_SIZE
            val reminder = written % adjustedPageSize
            val pages = (written - reminder) / adjustedPageSize
            return pages * pageSize + reminder
        }

        @JvmStatic
        fun adjustedLogFileSize(fileSize: Long, pageSize: Int): Long {
            val pages = fileSize / pageSize
            return (pageSize - BufferedDataWriter.HASH_CODE_SIZE) * pages
        }
    }
}
