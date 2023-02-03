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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.TestUtil
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.SharedOpenFilesCache
import org.junit.Assert
import java.io.File

interface ReplicatedLogTestMixin {
    companion object {
        const val openFiles = 16
        const val bucket = "logfiles"
    }

    fun Log.writeData(iterable: ByteIterable): Long {
        return write(127.toByte(), Loggable.NO_STRUCTURE_ID, iterable)
    }

    fun File.createLog(fileSize: Long, releaseLock: Boolean = false, modifyConfig: LogConfig.() -> Unit = {}): Log {
        SharedOpenFilesCache.setSize(openFiles)
        return with(LogConfig().setFileSize(fileSize)) {
            setLocation(this@createLog.canonicalPath)
            setReaderWriterProvider(DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER)
            modifyConfig()
            Log(this).also {
                if (releaseLock) { // s3mock can't open xd.lck on Windows otherwise
                    writer.release()
                }
            }
        }
    }

    fun newTmpFile(): File {
        return File(TestUtil.createTempDir(), bucket).also {
            it.mkdirs()
        }
    }

    fun checkLog(log: Log, highAddress: Long, count: Long, startAddress: Long = 0, startIndex: Long = 0) {
        log.use {
            Assert.assertEquals(highAddress, log.highAddress)
            val loggables = log.getLoggableIterator(startAddress)
            var i = startIndex
            while (loggables.hasNext()) {
                val loggable = loggables.next()
                if (!NullLoggable.isNullLoggable(loggable)) { // padding possible
                    Assert.assertEquals(127, loggable.type.toInt())
                    val value = CompressedUnsignedLongByteIterable.getLong(loggable.data)
                    val expectedLength = CompressedUnsignedLongByteIterable.getIterable(i).length
                    Assert.assertEquals(i, value)
                    Assert.assertEquals(expectedLength, loggable.dataLength) // length increases since `i` is part of payload
                    i++
                }
            }

            Assert.assertEquals(count, i - startIndex)
        }
    }

    fun writeToLog(sourceLog: Log, count: Long, startIndex: Long = 0) {
        sourceLog.beginWrite()
        for (i in startIndex until count + startIndex) {
            sourceLog.writeData(CompressedUnsignedLongByteIterable.getIterable(i))
        }
        sourceLog.flush()
        sourceLog.endWrite()
    }
}
