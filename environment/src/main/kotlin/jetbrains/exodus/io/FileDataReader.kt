/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.util.SharedRandomAccessFile
import mu.KLogging
import java.io.File
import java.io.IOException

class FileDataReader(val dir: File) : DataReader {

    private var useNio: Boolean = false
    private var log: Log? = null

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

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val file = File(dir, LogUtil.getLogFilename(blockAddress))
        removeFileFromFileCache(file)
        setWritable(file)
        val deleted = if (rbt == RemoveBlockType.Delete) file.delete() else renameFile(file)
        if (!deleted) {
            throw ExodusException("Failed to delete " + file.absolutePath)
        } else if (logger.isInfoEnabled) {
            logger.info("Deleted file " + file.absolutePath)
        }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        val file = FileBlock(blockAddress)
        removeFileFromFileCache(file)
        setWritable(file)
        try {
            SharedRandomAccessFile(file, "rw").use { f -> f.setLength(length) }
            if (logger.isInfoEnabled) {
                logger.info("Truncated file " + file.absolutePath + " to length = " + length)
            }
        } catch (e: IOException) {
            throw ExodusException("Failed to truncate file " + file.absolutePath, e)
        }

    }

    override fun close() {
        try {
            SharedOpenFilesCache.getInstance().removeDirectory(dir)
            if (useNio) {
                SharedMappedFilesCache.getInstance().removeDirectory(dir)
            }
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

    override fun getBlock(address: Long): Block {
        return FileBlock(address)
    }

    internal fun useNio(freePhysicalMemoryThreshold: Long) {
        useNio = true
        SharedMappedFilesCache.createInstance(freePhysicalMemoryThreshold)
    }

    private fun removeFileFromFileCache(file: File) {
        try {
            SharedOpenFilesCache.getInstance().removeFile(file)
            if (useNio) {
                SharedMappedFilesCache.getInstance().removeFileBuffer(file)
            }
        } catch (e: IOException) {
            throw ExodusException(e)
        }
    }

    private fun toBlocks(files: LongArrayList) =
            files.toArray().asSequence().map { address -> FileBlock(address) }.asIterable()

    private inner class FileBlock(private val address: Long) :
            File(dir, LogUtil.getLogFilename(address)), Block {

        override fun getAddress() = address

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            try {
                SharedOpenFilesCache.getInstance().getCachedFile(this).use { f ->
                    if (useNio &&
                            /* only read-only (immutable) files can be mapped */
                            (log != null && log!!.isImmutableFile(address) || log == null && !canWrite())) {
                        try {
                            SharedMappedFilesCache.getInstance().getFileBuffer(f).use { mappedBuffer ->
                                val buffer = mappedBuffer.buffer
                                buffer.position(position.toInt())
                                buffer.get(output, offset, count)
                                return count
                            }
                        } catch (t: Throwable) {
                            // if we failed to read mapped file, then try ordinary RandomAccessFile.read()
                            if (logger.isWarnEnabled) {
                                logger.warn("Failed to transfer bytes from memory mapped file", t)
                            }
                        }

                    }
                    f.seek(position)
                    return f.read(output, offset, count)
                }
            } catch (e: IOException) {
                throw ExodusException("Can't read file $absolutePath", e)
            }
        }
    }

    companion object : KLogging() {

        private const val DELETED_FILE_EXTENSION = ".del"

        private fun renameFile(file: File): Boolean {
            val name = file.name
            return file.renameTo(File(file.parent,
                    name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION))
        }

        private fun setWritable(file: File) {
            if (file.exists() && !file.setWritable(true)) {
                throw ExodusException("Failed to set writable " + file.absolutePath)
            }
        }
    }
}