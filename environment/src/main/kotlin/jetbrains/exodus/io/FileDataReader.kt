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
package jetbrains.exodus.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

class FileDataReader(val dir: File) : DataReader, KLogging() {

    companion object : KLogging()

    private var log: Log? = null

    internal var usedWithWatcher = false

    override fun getBlocks(): Iterable<Block> {
        val files = LogUtil.listFileAddresses(dir)
        files.sort()
        return toBlocks(files)
    }

    override fun getBlocks(fromAddress: Long): Iterable<Block> {
        val files = LogUtil.listFileAddresses(fromAddress, dir)
        files.sort()
        return toBlocks(files)
    }


    override fun close() {
        try {
            SharedOpenFilesCache.instance.removeDirectory(dir)
        } catch (e: IOException) {
            throw ExodusException("Can't close all files", e)
        }
    }

    fun setLog(log: Log) {
        this.log = log
    }

    override fun getLocation(): String {
        return dir.path
    }

    private fun toBlocks(files: LongArrayList) =
        files.toArray().asSequence().map { address -> FileBlock(address, this) }.asIterable()

    class FileBlock(private val address: Long, private val reader: FileDataReader) :
        File(reader.dir, LogUtil.getLogFilename(address)), Block {

        override fun getAddress() = address

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            try {
                val log = reader.log
                val immutable = log?.isImmutableFile(address) ?: !canWrite()
                val filesCache = SharedOpenFilesCache.instance
                val file =
                    if (immutable && !reader.usedWithWatcher) filesCache.getCachedFile(this) else filesCache.openFile(
                        this
                    )
                file.use { f ->
                    f.seek(position)

                    return readFully(f, output, offset, count)
                }
            } catch (e: IOException) {
                throw ExodusException("Can't read file $absolutePath", e)
            }
        }

        private fun readFully(file: RandomAccessFile, output: ByteArray, offset: Int, size: Int): Int {
            var read = 0

            while (read < size) {
                val r = file.read(output, offset + read, size - read)
                if (r == -1) {
                    break
                }
                read += r
            }

            return read
        }

        override fun refresh() = this
    }
}
