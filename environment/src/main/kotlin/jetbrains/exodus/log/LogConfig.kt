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

import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.io.AsyncFileDataReaderWriterProvider
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.io.inMemory.MemoryDataReaderWriterProvider

class LogConfig {
    private var location: String? = null
    private var readerWriterProvider: String? = null
    private var readerWriterProviderInstance: DataReaderWriterProvider? = null
    private var fileSize: Long = 0
    private var lockTimeout: Long = 0
    private var memoryUsage: Long = 0
    private var memoryUsagePercentage = 0
    private var reader: DataReader? = null
    private var writer: DataWriter? = null
    private var isDurableWrite = false
    private var isSharedCache = false
    private var isNonBlockingCache = false
    private var cacheUseSoftReferences = false
    private var cacheGenerationCount = 0
    private var cachePageSize = 0
    private var cacheOpenFilesCount = 0
    private var isCleanDirectoryExpected = false
    private var isClearInvalidLog = false
    private var isWarmup = false
    private var syncPeriod: Long = 0
    private var isFullFileReadonly = false
    private var streamCipherProvider: StreamCipherProvider? = null
    private var cipherKey: ByteArray? = null
    private var cipherBasicIV: Long = 0
    private var lockIgnored = false
    private var useV1Format: Boolean
    private var checkPagesAtRuntime: Boolean

    init {
        useV1Format = EnvironmentConfig.DEFAULT.useVersion1Format
        checkPagesAtRuntime = EnvironmentConfig.DEFAULT.checkPagesAtRuntime
    }

    fun setLocation(location: String): LogConfig {
        this.location = location
        return this
    }

    fun setReaderWriterProvider(provider: String): LogConfig {
        readerWriterProvider = provider
        return this
    }

    fun setFileSize(fileSize: Long): LogConfig {
        this.fileSize = fileSize
        return this
    }

    @Suppress("unused")
    fun setLockTimeout(lockTimeout: Long): LogConfig {
        this.lockTimeout = lockTimeout
        return this
    }


    @Suppress("unused")
    fun setMemoryUsage(memUsage: Long): LogConfig {
        memoryUsage = memUsage
        return this
    }

    fun getMemoryUsagePercentage(): Int {
        if (memoryUsagePercentage == 0) {
            memoryUsagePercentage = 50
        }
        return memoryUsagePercentage
    }

    fun getFileSize(): Long {
        if (fileSize == 0L) {
            fileSize = DEFAULT_FILE_SIZE.toLong()
        }

        return fileSize
    }

    fun getCacheOpenFilesCount() : Int {
        if (cacheOpenFilesCount == 0) {
            cacheOpenFilesCount = EnvironmentConfig.DEFAULT.logCacheOpenFilesCount
        }

        return cacheOpenFilesCount
    }

    fun getMemoryUsage() : Long = memoryUsage

    fun isWarmup() : Boolean {
        return isWarmup
    }

    fun isFullFileReadonly() : Boolean {
        return isFullFileReadonly
    }

    fun isCleanDirectoryExpected() : Boolean {
        return isCleanDirectoryExpected
    }

    fun isClearInvalidLog() : Boolean {
        return isClearInvalidLog
    }

    fun isDurableWrite() : Boolean = isDurableWrite

    fun isLockIgnored() : Boolean = lockIgnored

    fun setCheckPagesAtRuntime(checkPagesAtRuntime: Boolean) : LogConfig {
        this.checkPagesAtRuntime = checkPagesAtRuntime
        return this
    }


    @Suppress("unused")
    fun setMemoryUsagePercentage(memoryUsagePercentage: Int): LogConfig {
        this.memoryUsagePercentage = memoryUsagePercentage
        return this
    }

    fun getReader(): DataReader? {
        if (reader == null) {
            createReaderWriter()
        }
        return reader
    }

    fun getCipherBasicIv(): Long {
        return cipherBasicIV
    }

    fun getCipherKey(): ByteArray? {
        return cipherKey
    }

    fun getStreamCipherProvider(): StreamCipherProvider? {
        return streamCipherProvider
    }

    fun isCheckPagesAtRuntime() = checkPagesAtRuntime

    fun getLockTimeout() = lockTimeout

    @Deprecated("")
    fun setReader(reader: DataReader): LogConfig {
        this.reader = reader
        return this
    }

    fun getWriter(): DataWriter? {
        if (writer == null) {
            createReaderWriter()
        }
        return writer
    }

    @Deprecated("")
    fun setWriter(writer: DataWriter): LogConfig {
        this.writer = writer
        return this
    }

    fun setReaderWriter(
        reader: DataReader,
        writer: DataWriter
    ): LogConfig {
        this.reader = reader
        this.writer = writer
        return this
    }

    fun setDurableWrite(durableWrite: Boolean): LogConfig {
        isDurableWrite = durableWrite
        return this
    }

    @Suppress("unused")
    fun setSharedCache(sharedCache: Boolean): LogConfig {
        isSharedCache = sharedCache
        return this
    }

    fun setNonBlockingCache(nonBlockingCache: Boolean): LogConfig {
        isNonBlockingCache = nonBlockingCache
        return this
    }

    @Suppress("unused")
    fun setCacheUseSoftReferences(cacheUseSoftReferences: Boolean): LogConfig {
        this.cacheUseSoftReferences = cacheUseSoftReferences
        return this
    }

    fun getCacheGenerationCount(): Int {
        if (cacheGenerationCount == 0) {
            cacheGenerationCount = EnvironmentConfig.DEFAULT.logCacheGenerationCount
        }
        return cacheGenerationCount
    }

    @Suppress("unused")
    fun setCacheGenerationCount(cacheGenerationCount: Int): LogConfig {
        this.cacheGenerationCount = cacheGenerationCount
        return this
    }

    fun getCachePageSize(): Int {
        if (cachePageSize == 0) {
            cachePageSize = LogCache.MINIMUM_PAGE_SIZE
        }
        return cachePageSize
    }

    fun setCachePageSize(cachePageSize: Int): LogConfig {
        this.cachePageSize = cachePageSize
        return this
    }


    @Suppress("unused")
    fun setCacheOpenFilesCount(cacheOpenFilesCount: Int): LogConfig {
        this.cacheOpenFilesCount = cacheOpenFilesCount
        return this
    }

    @Suppress("unused")
    fun setCleanDirectoryExpected(cleanDirectoryExpected: Boolean): LogConfig {
        isCleanDirectoryExpected = cleanDirectoryExpected
        return this
    }

    @Suppress("unused")
    fun setClearInvalidLog(clearInvalidLog: Boolean): LogConfig {
        isClearInvalidLog = clearInvalidLog
        return this
    }

    fun setWarmup(warmup: Boolean): LogConfig {
        isWarmup = warmup
        return this
    }

    fun getSyncPeriod(): Long {
        if (syncPeriod == 0L) {
            syncPeriod = EnvironmentConfig.DEFAULT.logSyncPeriod
        }
        return syncPeriod
    }

    fun setSyncPeriod(syncPeriod: Long): LogConfig {
        this.syncPeriod = syncPeriod
        return this
    }

    @Suppress("unused")
    fun setFullFileReadonly(fullFileReadonly: Boolean): LogConfig {
        isFullFileReadonly = fullFileReadonly
        return this
    }

    @Suppress("unused")
    fun setCipherProvider(cipherProvider: StreamCipherProvider?): LogConfig {
        this.streamCipherProvider = cipherProvider
        return this
    }

    fun setCipherKey(cipherKey: ByteArray?): LogConfig {
        this.cipherKey = cipherKey
        return this
    }

    fun setCipherBasicIV(basicIV: Long): LogConfig {
        cipherBasicIV = basicIV
        return this
    }

    fun getReaderWriterProvider(): DataReaderWriterProvider {
        if (readerWriterProviderInstance == null && readerWriterProvider != null) {
            readerWriterProviderInstance = DataReaderWriterProvider.getProvider(readerWriterProvider!!)
            if (readerWriterProviderInstance == null) {
                readerWriterProviderInstance =
                    when (readerWriterProvider) {
                        DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER -> AsyncFileDataReaderWriterProvider()
                        DataReaderWriterProvider.IN_MEMORY_READER_WRITER_PROVIDER -> MemoryDataReaderWriterProvider()
                        else -> throw InvalidSettingException("Unknown DataReaderWriterProvider: $readerWriterProvider")
                    }
            }
        }
        return readerWriterProviderInstance!!
    }

    fun setUseV1Format(useV1Format: Boolean) {
        this.useV1Format = useV1Format
    }

    fun useV1Format(): Boolean {
        return useV1Format
    }

    private fun createReaderWriter() {
        val location = location
            ?: throw InvalidSettingException("Location for DataReader and DataWriter is not specified")
        val provider = getReaderWriterProvider()
        val readerWriter = provider.newReaderWriter(location)
        reader = readerWriter.getFirst()
        writer = readerWriter.getSecond()
    }

    companion object {
        private const val DEFAULT_FILE_SIZE = 1024 // in kilobytes

        @JvmStatic
        fun create(reader: DataReader, writer: DataWriter): LogConfig {
            return LogConfig().setReaderWriter(reader, writer)
        }
    }
}