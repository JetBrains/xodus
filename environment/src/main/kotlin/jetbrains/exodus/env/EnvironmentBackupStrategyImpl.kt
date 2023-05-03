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
package jetbrains.exodus.env

import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.FileDescriptorInputStream
import jetbrains.exodus.backup.VirtualFileDescriptor
import jetbrains.exodus.log.DataCorruptionException
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.StartupMetadata
import jetbrains.exodus.util.IOUtil.listFiles
import java.io.*

internal class EnvironmentBackupStrategyImpl(private val environment: EnvironmentImpl) : BackupStrategy() {
    private var highAddress: Long = 0
    private var fileLastAddress: Long = 0
    private var fileLengthBound: Long = 0
    private var rootAddress: Long = 0
    private val pageSize = environment.environmentConfig.logCachePageSize
    private var startupMetadataWasSent = false

    override fun beforeBackup() {
        environment.suspendGC()
        val highAndRootAddress = environment.flushSyncAndFillPagesWithNulls()
        highAddress = highAndRootAddress[0]
        rootAddress = highAndRootAddress[1]
        fileLengthBound = environment.log.fileLengthBound
        fileLastAddress = highAddress / fileLengthBound * fileLengthBound
    }

    override fun getContents(): Iterable<VirtualFileDescriptor> {
        environment.flushAndSync()
        return object : Iterable<VirtualFileDescriptor> {
            private val files = listFiles(File(environment.location))
            private var i = 0
            private var next: VirtualFileDescriptor? = null
            override fun iterator(): Iterator<VirtualFileDescriptor> {
                return object : MutableIterator<VirtualFileDescriptor> {
                    override fun hasNext(): Boolean {
                        if (next != null) {
                            return true
                        }
                        while (i < files.size) {
                            val file = files[i++]
                            if (file.isFile) {
                                val fileSize = file.length()
                                val logFileName = file.name
                                if (fileSize != 0L && logFileName.endsWith(LogUtil.LOG_FILE_EXTENSION)) {
                                    val fileAddress = LogUtil.getAddress(file.name)
                                    if (fileLastAddress < fileAddress) {
                                        break
                                    }
                                    if (fileLastAddress > fileAddress && fileSize < fileLengthBound) {
                                        DataCorruptionException.raise(
                                            "Size of the file is less than expected. {expected : " +
                                                    fileLengthBound + ", actual : " + fileSize + " }",
                                            environment.log, fileAddress
                                        )
                                    }
                                    val updatedFileSize = fileSize.coerceAtMost(highAddress - fileAddress)
                                    next = object : FileDescriptor(file, "", updatedFileSize) {
                                        @Throws(IOException::class)
                                        override fun getInputStream(): InputStream {
                                            return FileDescriptorInputStream(
                                                FileInputStream(file),
                                                fileAddress, pageSize, getFileSize(),
                                                highAddress - fileAddress,
                                                environment.log, environment.cipherProvider,
                                                environment.cipherKey, environment.cipherBasicIV
                                            )
                                        }
                                    }
                                    return true
                                }
                            }
                        }
                        if (!startupMetadataWasSent) {
                            startupMetadataWasSent = true
                            val metadataContent = StartupMetadata.serialize(
                                0, EnvironmentImpl.CURRENT_FORMAT_VERSION, rootAddress,
                                environment.log.cachePageSize,
                                environment.log.fileLengthBound,
                                true
                            )
                            next = object : FileDescriptor(
                                File(StartupMetadata.FIRST_FILE_NAME),
                                "", metadataContent.remaining().toLong()
                            ) {
                                override fun getInputStream(): InputStream {
                                    return ByteArrayInputStream(
                                        metadataContent.array(),
                                        metadataContent.arrayOffset(), metadataContent.remaining()
                                    )
                                }

                                override fun shouldCloseStream(): Boolean {
                                    return false
                                }

                                override fun hasContent(): Boolean {
                                    return true
                                }

                                override fun getTimeStamp(): Long {
                                    return System.currentTimeMillis()
                                }

                                override fun canBeEncrypted(): Boolean {
                                    return false
                                }
                            }
                            return true
                        }
                        return false
                    }

                    override fun next(): VirtualFileDescriptor {
                        if (!hasNext()) {
                            throw NoSuchElementException()
                        }
                        val result = next
                        next = null
                        return result!!
                    }

                    override fun remove() {
                        throw UnsupportedOperationException()
                    }
                }
            }
        }
    }

    override fun isEncrypted(): Boolean {
        return environment.environmentConfig.cipherKey != null
    }

    override fun afterBackup() {
        environment.resumeGC()
    }
}
