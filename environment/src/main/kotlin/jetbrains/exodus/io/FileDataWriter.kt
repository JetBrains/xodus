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
import jetbrains.exodus.OutOfDiskSpaceException
import jetbrains.exodus.io.FileDataReader.FileBlock
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants
import jetbrains.exodus.util.SharedRandomAccessFile
import jetbrains.exodus.util.UnsafeHolder
import mu.KLogging
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.ClosedChannelException
import java.nio.channels.FileChannel

open class FileDataWriter @JvmOverloads constructor(private val reader: FileDataReader, lockId: String? = null) : AbstractDataWriter() {

    private var dirChannel: FileChannel? = null
    private val lockingManager: LockingManager
    private var file: RandomAccessFile? = null
    private var block: FileBlock? = null
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

    override fun write(b: ByteArray, off: Int, len: Int): Block {
        try {
            ensureFile("Can't write, FileDataWriter is closed").write(b, off, len)
        } catch (ioe: IOException) {
            if (lockingManager.usableSpace < len) {
                throw OutOfDiskSpaceException(ioe)
            }
            throw ioe
        }
        return block ?: throw ExodusException("Can't write, FileDataWriter is closed")
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
        file?.let { forceSync(it) }
    }

    override fun closeImpl() {
        ensureFile("Can't close already closed FileDataWriter").close()
        file = null
        dirChannel?.close()
        dirChannel = null
        block = null
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

    override fun openOrCreateBlockImpl(address: Long, length: Long) =
        FileBlock(address, reader).also { openOrCreateFile(it, length) }


    override fun syncDirectory() {
        dirChannel?.let { channel ->
            try {
                channel.asUninterruptible().force(false)
            } catch (e: IOException) {
                // just warn as XD-698 requires
                warnCantFsyncDirectory()
            }
        }
    }

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val file = FileBlock(blockAddress, reader)
        removeFileFromFileCache(file)
        setWritable(file)
        val deleted = if (rbt == RemoveBlockType.Delete) file.delete() else renameFile(file)
        if (!deleted) {
            throw ExodusException("Failed to delete ${file.absolutePath}")
        } else {
            logger.info { "Deleted file ${file.absolutePath}" }
        }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        val file = FileBlock(blockAddress, reader)
        removeFileFromFileCache(file)
        setWritable(file)
        SharedRandomAccessFile(file, "rw").use { f -> f.setLength(length) }
        logger.info { "Truncated file ${file.absolutePath} to length = $length" }
    }

    private fun warnCantFsyncDirectory() {
        this.dirChannel = null
        warnIfWindows
    }

    private fun removeFileFromFileCache(file: File) {
        SharedOpenFilesCache.getInstance().removeFile(file)
        if (useNio) {
            SharedMappedFilesCache.getInstance().removeFileBuffer(file)
        }
    }

    private fun openOrCreateFile(block: FileBlock, length: Long): RandomAccessFile {
        val result = RandomAccessFile(block.apply {
            if (!canWrite()) {
                setWritable(this)
            }
        }, "rw")
        result.seek(length)
        if (length != result.length()) {
            result.setLength(length)
            forceSync(result)
        }
        file = result
        this.block = block
        return result
    }

    private fun ensureFile(errorMsg: String): RandomAccessFile {
        file?.let { file ->
            if (file.channel.isOpen) {
                return file
            }
            return openOrCreateFile(block.notNull, file.length()).also { file.close() }
        }
        throw ExodusException(errorMsg)
    }

    companion object : KLogging() {

        private const val DELETED_FILE_EXTENSION = ".del"
        private val warnIfWindows by lazy {
            logger.warn("Can't open directory channel. Log directory fsync won't be performed.")
        }
        private val setUninterruptibleMethod =
            if (JVMConstants.IS_JAVA9_OR_HIGHER) {
                UnsafeHolder.doPrivileged {
                    try {
                        Class.forName("sun.nio.ch.FileChannelImpl").getDeclaredMethod("setUninterruptible").apply {
                            isAccessible = true
                            logger.info { "Uninterruptible file channel will be used" }
                        }
                    } catch (t: Throwable) {
                        logger.info(t) { "Interruptible file channel will be used" }
                            null
                        }
                    }
                } else {
                    null
                }

        private fun FileChannel.asUninterruptible(): FileChannel {
            setUninterruptibleMethod?.invoke(this)
            return this
        }

        fun renameFile(file: File): Boolean {
            val name = file.name
            return file.renameTo(File(file.parent,
                    name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION))
        }

        private fun forceSync(file: RandomAccessFile) {
            try {
                file.channel.asUninterruptible().force(false)
            } catch (e: ClosedChannelException) {
                // ignore
            } catch (ioe: IOException) {
                if (file.channel.isOpen) {
                    throw ioe
                }
            }
        }

        private fun setWritable(file: File) {
            if (file.exists() && !file.setWritable(true)) {
                throw ExodusException("Failed to set writable " + file.absolutePath)
            }
        }
    }
}