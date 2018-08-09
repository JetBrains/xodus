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
import jetbrains.exodus.OutOfDiskSpaceException
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants
import jetbrains.exodus.util.SharedRandomAccessFile
import mu.KLogging
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.ClosedChannelException
import java.nio.channels.FileChannel

open class FileDataWriter @JvmOverloads constructor(private val reader: FileDataReader, lockId: String? = null) : AbstractDataWriter() {

    companion object : KLogging() {

        private const val DELETED_FILE_EXTENSION = ".del"

        private fun renameFile(file: File): Boolean {
            val name = file.name
            return file.renameTo(File(file.parent,
                    name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION))
        }

        private fun forceSync(file: RandomAccessFile) {
            try {
                val channel = file.channel
                channel.force(false)
            } catch (e: ClosedChannelException) {
                // ignore
            } catch (ioe: IOException) {
                if (file.channel.isOpen) {
                    throw ExodusException(ioe)
                }
            }
        }

        private fun setWritable(file: File) {
            if (file.exists() && !file.setWritable(true)) {
                throw ExodusException("Failed to set writable " + file.absolutePath)
            }
        }
    }

    private var dirChannel: FileChannel? = null
    private val lockingManager: LockingManager
    private var file: RandomAccessFile? = null
    private var useNio = false

    init {
        var channel: FileChannel? = null
        if (!JVMConstants.IS_ANDROID) {
            try {
                channel = FileChannel.open(reader.dir.toPath())
                // try to force as XD-698 requires
                channel.force(false)
            } catch (e: IOException) {
                channel = null
                warnCantFsyncDirectory()
            }
        }
        dirChannel = channel
        lockingManager = LockingManager(reader.dir, lockId)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        try {
            (this.file ?: throw ExodusException("Can't write, FileDataWriter is closed")).write(b, off, len)
        } catch (ioe: IOException) {
            if (lockingManager.usableSpace < len) {
                throw OutOfDiskSpaceException(ioe)
            }
            throw ExodusException("Can't write", ioe)
        }
    }

    override fun lock(timeout: Long): Boolean {
        return lockingManager.lock(timeout)
    }

    override fun release(): Boolean {
        return lockingManager.release()
    }

    override fun lockInfo(): String? {
        return lockingManager.lockInfo()
    }

    override fun syncImpl() {
        val file = this.file
        if (file != null) {
            forceSync(file)
        }
    }

    override fun closeImpl() {
        try {
            (this.file ?: throw ExodusException("Can't close already closed FileDataWriter")).close()
            this.file = null
        } catch (e: IOException) {
            throw ExodusException("Can't close FileDataWriter", e)
        }
    }

    override fun clearImpl() {
        for (file in LogUtil.listFiles(reader.dir)) {
            if (!file.canWrite()) {
                setWritable(file)
            }
            if (file.exists() && !file.delete()) {
                throw ExodusException("Failed to delete $file")
            }
        }
    }

    override fun openOrCreateBlockImpl(address: Long, length: Long): Block {
        val result = FileDataReader.FileBlock(address, reader)
        try {
            val file = RandomAccessFile(result.apply {
                if (!canWrite()) {
                    setWritable(this)
                }
            }, "rw")
            file.seek(length)
            if (length != file.length()) {
                file.setLength(length)
                forceSync(file)
            }
            this.file = file
            return result
        } catch (ioe: IOException) {
            throw ExodusException(ioe)
        }
    }

    override fun syncDirectory() {
        dirChannel?.apply {
            try {
                force(false)
            } catch (e: IOException) {
                // just warn as XD-698 requires
                warnCantFsyncDirectory()
            }
        }
    }

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val file = FileDataReader.FileBlock(blockAddress, reader)
        removeFileFromFileCache(file)
        setWritable(file)
        val deleted = if (rbt == RemoveBlockType.Delete) file.delete() else renameFile(file)
        if (!deleted) {
            throw ExodusException("Failed to delete " + file.absolutePath)
        } else if (FileDataReader.logger.isInfoEnabled) {
            FileDataReader.logger.info("Deleted file " + file.absolutePath)
        }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        val file = FileDataReader.FileBlock(blockAddress, reader)
        removeFileFromFileCache(file)
        setWritable(file)
        try {
            SharedRandomAccessFile(file, "rw").use { f -> f.setLength(length) }
            if (FileDataReader.logger.isInfoEnabled) {
                FileDataReader.logger.info("Truncated file " + file.absolutePath + " to length = " + length)
            }
        } catch (e: IOException) {
            throw ExodusException("Failed to truncate file " + file.absolutePath, e)
        }
    }

    private fun warnCantFsyncDirectory() {
        this.dirChannel = null
        logger.warn("Can't open directory channel. Log directory fsync won't be performed.")
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
}
