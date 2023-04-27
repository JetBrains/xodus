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
package jetbrains.exodus.backup

import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.crypto.cryptBlocksMutable
import jetbrains.exodus.log.BufferedDataWriter
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.*

class FileDescriptorInputStream(
    private val fileInputStream: FileInputStream, private val fileAddress: Long, private val pageSize: Int,
    backupFileSize: Long, storedDataFilesSize: Long,
    log: Log, cipherProvider: StreamCipherProvider?,
    cipherKey: ByteArray?, cipherBasicIV: Long
) : InputStream() {
    private val backupFileSize: Long
    private val storedDataFilesSize: Long
    private val page: ByteArray = ByteArray(pageSize)
    private val log: Log
    private var pagePosition: Int
    private var position = 0
    private val cipherProvider: StreamCipherProvider?
    private val cipherKey: ByteArray?
    private val cipherBasicIV: Long

    init {
        //backup always contains only full pages
        this.backupFileSize = backupFileSize
        this.storedDataFilesSize = storedDataFilesSize
        this.log = log
        this.cipherProvider = cipherProvider
        this.cipherKey = cipherKey
        this.cipherBasicIV = cipherBasicIV
        pagePosition = Int.MAX_VALUE
    }

    @Throws(IOException::class)
    override fun read(): Int {
        if (position >= backupFileSize) {
            return -1
        }
        if (pagePosition < pageSize) {
            val datum = page[pagePosition].toInt()
            pagePosition++
            position++
            return datum
        }
        if (readPage()) {
            val datum = page[0].toInt()
            pagePosition = 1
            position++
            return datum
        }
        return -1
    }

    @Throws(IOException::class)
    override fun read(out: ByteArray, off: Int, len: Int): Int {
        if (off < 0 || len < 0 || out.size < off + len) {
            throw IndexOutOfBoundsException()
        }
        if (len == 0) {
            return 0
        }
        if (position >= backupFileSize) {
            return -1
        }
        if (pagePosition >= pageSize) {
            if (!readPage()) {
                return -1
            }
        }
        val resultSize = (pageSize - pagePosition).coerceAtMost(len)
        System.arraycopy(page, pagePosition, out, off, resultSize)
        pagePosition += resultSize
        position += resultSize
        return resultSize
    }

    @Throws(IOException::class)
    override fun close() {
        fileInputStream.close()
        super.close()
    }

    @Throws(IOException::class)
    override fun available(): Int {
        val available = backupFileSize - position
        return if (available <= Int.MAX_VALUE) {
            available.toInt()
        } else 0
    }

    @Throws(IOException::class)
    private fun readPage(): Boolean {
        val toRead = pageSize.toLong().coerceAtMost(storedDataFilesSize - position).toInt()
        assert(toRead >= 0)
        var read = 0
        while (read < toRead) {
            val r = fileInputStream.read(page, read, toRead - read)
            if (r == -1) {
                return if (read == 0) {
                    false
                } else {
                    break
                }
            }
            read += r
        }
        val pageAddress = fileAddress + position
        if (read < pageSize) {
            Arrays.fill(page, read, pageSize - BufferedDataWriter.HASH_CODE_SIZE, 0x80.toByte())
            if (cipherProvider != null) {
                assert(cipherKey != null)
                cryptBlocksMutable(
                    cipherProvider, cipherKey!!,
                    cipherBasicIV, pageAddress, page, read, pageSize - BufferedDataWriter.HASH_CODE_SIZE - read,
                    LogUtil.LOG_BLOCK_ALIGNMENT
                )
            }
            BufferedDataWriter.updatePageHashCode(page)
        }
        BufferedDataWriter.checkPageConsistency(pageAddress, page, pageSize, log)
        pagePosition = 0
        return true
    }
}
