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
package jetbrains.exodus.crypto.convert

import jetbrains.exodus.ExodusException
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.Backupable
import jetbrains.exodus.backup.VirtualFileDescriptor
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.LogUtil.LOG_FILE_EXTENSION

fun ScytaleEngine.encryptBackupable(source: Backupable) {
    val strategy = source.backupStrategy
    strategy.beforeBackup()

    try {
        encryptFiles(strategy)
    } catch (t: Throwable) {
        strategy.onError(t);
        throw ExodusException.toExodusException(t, "Encrypted backup failed")
    } finally {
        strategy.afterBackup()
    }
}

fun ScytaleEngine.encryptFiles(strategy: BackupStrategy) {
    start()
    use {
        for (descriptor in strategy.contents) {
            if (strategy.isInterrupted) {
                break
            }
            if (descriptor.hasContent()) {
                val fileSize = Math.min(descriptor.fileSize, strategy.acceptFile(descriptor))
                if (fileSize > 0L) {
                    appendFile(it, descriptor, fileSize)
                }
            }
        }
        it.put(EndChunk)
    }
}

fun getFileIV(fd: VirtualFileDescriptor): Pair<Long, Boolean> {
    val name = fd.name
    if (name.endsWith(LOG_FILE_EXTENSION)) {
        return LogUtil.getAddress(name) to true
    }
    val path = fd.path
    val blobsLocation = path.indexOf("blobs")
    if (blobsLocation >= 0) { // TODO: improve this naming hacks in favor of BackupStrategy?
        val blobExtensionStart = name.indexOf(".")
        if (blobExtensionStart == 2 || blobExtensionStart == 1) {
            val tokens = path.substring(blobsLocation + 5 until path.length).split("\\", "/").filter { it.isNotEmpty() }.asReversed()
            try {
                var result = Integer.parseInt(name.substring(0, blobExtensionStart), 16).toLong()
                var shift = 0
                tokens.forEach {
                    shift += java.lang.Byte.SIZE
                    result += (Integer.parseInt(it, 16).toLong() shl shift)
                }
                return -result to false
            } catch (e: NumberFormatException) {
            }
        }
    }
    throw IllegalArgumentException("Bad virtual file descriptor ${fd.path} ${fd.name}")
}

private val NO_IV = 0L to false

private fun appendFile(out: ScytaleEngine, fd: VirtualFileDescriptor, fileSize: Long) {
    if (!fd.hasContent()) {
        throw IllegalArgumentException("Provided source is not a file: " + fd.path)
    }

    val canBeEncrypted = fd.canBeEncrypted()
    val iv = if (canBeEncrypted) {
        getFileIV(fd)
    } else {
        NO_IV
    }
    val header = FileHeader(fd.path, fd.name, fileSize, fd.timeStamp, iv.first, iv.second, canBeEncrypted)
    out.put(header)
    val input = fd.inputStream
    try {
        var totalRead: Long = 0
        while (totalRead < fileSize) {
            val buffer = out.alloc() // new buffer must be employed until chunk is processed
            var read = input.read(buffer)
            if (read < 0) {
                break
            }
            if (read > 0) {
                read = Math.min(fileSize - totalRead, read.toLong()).toInt()
                out.put(FileChunk(header, read, buffer))
                totalRead += read
            }
        }
    } finally {
        if (fd.shouldCloseStream()) {
            input.close()
        }
    }
}
