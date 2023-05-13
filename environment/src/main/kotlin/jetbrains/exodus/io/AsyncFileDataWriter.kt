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
import jetbrains.exodus.core.dataStructures.LongIntPair
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.io.FileDataReader.FileBlock
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants.IS_ANDROID
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

class AsyncFileDataWriter @JvmOverloads constructor(private val reader: FileDataReader, lockId: String? = null) :
    AbstractDataWriter() {
    private var dirChannel: AsynchronousFileChannel?
    private var channel: AsynchronousFileChannel? = null
    private var block: FileBlock? = null
    private val lockingManager: LockingManager
    private var position: Long = 0

    init {
        lockingManager = LockingManager(reader.dir, lockId)
        var channel: AsynchronousFileChannel? = null
        if (!IS_ANDROID) {
            try {
                channel = AsynchronousFileChannel.open(reader.dir.toPath())
                // try to force as XD-698 requires
                channel.force(false)
            } catch (e: IOException) {
                channel = null
                warnCantFsyncDirectory()
            }
        }
        dirChannel = channel
    }

    override fun write(b: ByteArray, off: Int, len: Int): Block {
        val pair = asyncWrite(b, off, len)
        try {
            pair.getSecond().get()
        } catch (e: InterruptedException) {
            if (e.cause is IOException) {
                if (lockingManager.getUsableSpace() < len) {
                    throw OutOfDiskSpaceException(e)
                }
            }
            throw ExodusException("Can not write into file.", e)
        } catch (e: ExecutionException) {
            if (e.cause is IOException) {
                if (lockingManager.getUsableSpace() < len) {
                    throw OutOfDiskSpaceException(e)
                }
            }
            throw ExodusException("Can not write into file.", e)
        }
        return block!!
    }

    override fun asyncWrite(b: ByteArray, off: Int, len: Int): Pair<Block, CompletableFuture<LongIntPair>> {
        val channel: AsynchronousFileChannel = try {
            ensureChannel("Can't write, AsyncFileDataWriter is closed")
        } catch (e: IOException) {
            if (lockingManager.getUsableSpace() < len) {
                throw OutOfDiskSpaceException(e)
            }
            throw ExodusException("Can not write into file.", e)
        }
        val buffer = ByteBuffer.wrap(b, off, len)
        val future = CompletableFuture<LongIntPair>()
        channel.write<Void?>(
            buffer, position, null, WriteCompletionHandler(
                buffer, future,
                lockingManager, channel, position, block!!.address, len
            )
        )
        position += len.toLong()
        return Pair(block, future)
    }

    override fun position(): Long {
        return position
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
        try {
            channel!!.force(false)
        } catch (e: IOException) {
            throw ExodusException("Can not synchronize file " + block!!.absolutePath, e)
        }
    }

    override fun closeImpl() {
        try {
            ensureChannel("Can't close already closed " + AsyncFileDataWriter::class.java.name).close()
            if (dirChannel != null) {
                dirChannel!!.force(false)
            }
            channel = null
            dirChannel = null
            block = null
        } catch (e: IOException) {
            throw ExodusException("Can not close file " + block!!.absolutePath, e)
        }
    }

    override fun clearImpl() {
        for (file in LogUtil.listFiles(reader.dir)) {
            if (!file.canWrite()) {
                if (!file.setWritable(true)) {
                    throw ExodusException("File " + file.absolutePath + " is protected from writes.")
                }
            }
            if (file.exists() && !file.delete()) {
                throw ExodusException("Failed to delete " + file.absolutePath)
            }
        }
    }

    override fun openOrCreateBlockImpl(address: Long, length: Long): Block {
        val block = FileBlock(address, reader)
        try {
            openOrCreateChannel(block, length)
        } catch (e: IOException) {
            throw ExodusException("Channel can not be created for the file " + block.absolutePath, e)
        }
        return block
    }

    override fun syncDirectory() {
        try {
            if (dirChannel != null) {
                dirChannel!!.force(false)
            }
        } catch (e: IOException) {
            warnCantFsyncDirectory()
        }
    }

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val block = FileBlock(blockAddress, reader)
        removeFileFromFileCache(block)
        if (block.exists() && !block.setWritable(true)) {
            throw ExodusException("File " + block.absolutePath + " is protected from write.")
        }
        val deleted: Boolean = if (rbt == RemoveBlockType.Delete) {
            block.delete()
        } else {
            renameFile(block)
        }
        if (!deleted) {
            throw ExodusException("Failed to delete file " + block.absolutePath)
        } else {
            logger.info("Deleted file " + block.absolutePath)
        }
    }

    @Deprecated("")
    override fun truncateBlock(blockAddress: Long, length: Long) {
        val block = FileBlock(blockAddress, reader)
        removeFileFromFileCache(block)
        if (block.exists() && !block.setWritable(true)) {
            throw ExodusException("File " + block.absolutePath + " is protected from write.")
        }
        try {
            RandomAccessFile(block, "rw").use { file -> file.setLength(length) }
        } catch (e: FileNotFoundException) {
            throw ExodusException("File " + block.absolutePath + " was not found", e)
        } catch (e: IOException) {
            throw ExodusException("Can not truncate file " + block.absolutePath, e)
        }
        logger.info("Truncated file " + block.absolutePath + " to length = " + length)
    }

    private fun warnCantFsyncDirectory() {
        dirChannel = null
        logger.warn("Can't open directory channel. Log directory fsync won't be performed.")
    }

    @Throws(IOException::class)
    private fun ensureChannel(errorMessage: String): AsynchronousFileChannel {
        val channel = this.channel
        if (channel != null) {
            return if (channel.isOpen) {
                channel
            } else openOrCreateChannel(block, Files.size(block!!.toPath()))
        }
        throw ExodusException(errorMessage)
    }

    @Throws(IOException::class)
    private fun openOrCreateChannel(
        fileBlock: FileBlock?,
        length: Long
    ): AsynchronousFileChannel {
        val blockPath = fileBlock!!.toPath()
        if (!fileBlock.exists()) {
            Files.createFile(blockPath)
        }
        if (!fileBlock.canWrite()) {
            if (!fileBlock.setWritable(true)) {
                throw ExodusException("File " + fileBlock.absolutePath + " is protected from writes and can not be used.")
            }
        }
        val position = Files.getAttribute(blockPath, "size") as Long
        if (position != length) {
            throw ExodusException("Invalid size for the file " + blockPath.toAbsolutePath())
        }
        val channel = AsynchronousFileChannel.open(
            blockPath,
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE
        )
        block = fileBlock
        this.channel = channel
        this.position = position
        return channel
    }

    fun lockFilePath(): String {
        return lockingManager.lockFilePath()
    }

    internal class WriteCompletionHandler internal constructor(
        private val buffer: ByteBuffer,
        private val future: CompletableFuture<LongIntPair>,
        private val lockingManager: LockingManager,
        private val channel: AsynchronousFileChannel, private val position: Long,
        private val address: Long, private val len: Int
    ) :
        CompletionHandler<Int?, Void?> {
        override fun completed(result: Int?, attachment: Void?) {
            if (buffer.remaining() > 0) {
                channel.write<Void?>(buffer, buffer.position() + position, null, this)
                return
            }
            future.complete(LongIntPair(address + position, len))
        }

        override fun failed(exc: Throwable, attachment: Void?) {
            if (lockingManager.getUsableSpace() < len) {
                future.completeExceptionally(OutOfDiskSpaceException(exc))
            } else {
                future.completeExceptionally(exc)
            }
        }
    }

    companion object {
        const val DELETED_FILE_EXTENSION = ".del"
        private val logger = LoggerFactory.getLogger(AsyncFileDataWriter::class.java)
        fun renameFile(file: File): Boolean {
            val name = file.name
            return file.renameTo(
                File(
                    file.parentFile,
                    name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION
                )
            )
        }

        private fun removeFileFromFileCache(file: File) {
            try {
                SharedOpenFilesCache.getInstance().removeFile(file)
            } catch (e: IOException) {
                throw ExodusException("Can not remove file " + file.absolutePath, e)
            }
        }
    }
}
