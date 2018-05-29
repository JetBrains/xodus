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
import mu.KLogging
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.ClosedChannelException
import java.nio.channels.FileChannel

open class FileDataWriter @JvmOverloads constructor(private val dir: File, lockId: String? = null) : AbstractDataWriter() {

    private var dirChannel: FileChannel? = null
    private val lockingManager: LockingManager
    private var file: RandomAccessFile? = null

    init {
        file = null
        var channel: FileChannel? = null
        if (!JVMConstants.IS_ANDROID) {
            try {
                channel = FileChannel.open(dir.toPath())
                // try to force as XD-698 requires
                channel.force(false)
            } catch (e: IOException) {
                channel = null
                warnCantFsyncDirectory()
            }
        }
        dirChannel = channel
        lockingManager = LockingManager(dir, lockId)
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
        for (file in LogUtil.listFiles(dir)) {
            if (!file.canWrite()) {
                setWritable(file)
            }
            if (file.exists() && !file.delete()) {
                throw ExodusException("Failed to delete $file")
            }
        }
    }

    override fun openOrCreateBlockImpl(address: Long, length: Long) {
        try {
            val result = RandomAccessFile(File(dir, LogUtil.getLogFilename(address)), "rw")
            result.seek(length)
            if (length != result.length()) {
                result.setLength(length)
                forceSync(result)
            }
            file = result
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

    private fun warnCantFsyncDirectory() {
        this.dirChannel = null
        logger.warn("Can't open directory channel. Log directory fsync won't be performed.")
    }

    companion object : KLogging() {

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
}
