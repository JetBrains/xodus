/**
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


import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.crypto.cryptBlocksMutable
import jetbrains.exodus.io.*
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.IdGenerator
import mu.KLogging
import java.io.Closeable
import java.util.concurrent.Semaphore
import kotlin.experimental.xor
import kotlin.text.StringBuilder

class Log(val config: LogConfig, expectedEnvironmentVersion: Int) : Closeable {

    val created = System.currentTimeMillis()

    @JvmField
    internal val cache: LogCache

    private val writeBoundarySemaphore: Semaphore

    @Volatile
    var isClosing: Boolean = false
        private set

    var identity: Int = 0
        private set

    private val reader: DataReader = config.reader
    private val dataWriter: DataWriter = config.writer

    private val writer: BufferedDataWriter

    private val blockListeners = ArrayList<BlockListener>(2)
    private val readBytesListeners = ArrayList<ReadBytesListener>(2)

    private var startupMetadata: StartupMetadata

    val isClossedCorrectly: Boolean
        get() = startupMetadata.isCorrectlyClosed

    /** Size of single page in log cache. */
    var cachePageSize: Int

    /**
     * Indicate whether it is needed to perform migration to the format which contains
     * hash codes of content inside of the pages.
     */
    val formatWithHashCodeIsUsed: Boolean

    /** Size of a single file of the log in bytes.
     * @return size of a single log file in bytes.
     */
    val fileLengthBound: Long

    @Deprecated("for tests only")
    private var testConfig: LogTestConfig? = null

    val location: String
        get() = reader.location

    val numberOfFiles: Long
        get() = writer.numberOfFiles().toLong()

    /**
     * Returns addresses of log files from the newest to the oldest ones.
     */
    val allFileAddresses: LongArray
        get() = writer.allFiles()

    val highAddress: Long
        get() = writer.highAddress

    val writtenHighAddress: Long
        get() = writer.currentHighAddress

    val lowFileAddress: Long
        get() {
            val result = writer.minimumFile
            return result ?: Loggable.NULL_ADDRESS
        }

    val highFileAddress: Long
        get() = getFileAddress(highAddress)

    val diskUsage: Long
        get() {
            val allFiles = writer.allFiles()
            val highAddress = writer.highAddress

            val filesCount = allFiles.size

            return if (filesCount == 0) 0L else (filesCount - 1) * fileLengthBound + getFileSize(
                allFiles[filesCount - 1],
                highAddress
            )
        }

    val cacheHitRate: Float
        get() = cache.hitRate()

    val isReadOnly: Boolean
        get() = rwIsReadonly

    private val nullPage: ByteArray

    @Volatile
    private var rwIsReadonly: Boolean

    init {
        tryLock()
        try {
            rwIsReadonly = config.readerWriterProvider?.isReadonly ?: false

            val fileLength = config.fileSize * 1024L

            val metadata = if (reader is FileDataReader) {
                StartupMetadata.open(reader, rwIsReadonly)
            } else {
                StartupMetadata.createStub(config.cachePageSize, expectedEnvironmentVersion, fileLength)
            }

            var needToPerformMigration = false

            if (metadata != null) {
                startupMetadata = metadata
            } else {
                startupMetadata = StartupMetadata.createStub(
                    config.cachePageSize, expectedEnvironmentVersion,
                    fileLength
                )
                needToPerformMigration = reader.blocks.iterator().hasNext()
            }

            if (config.cachePageSize != startupMetadata.pageSize) {
                logger.warn(
                    "Environment $location was created with cache page size equals to " +
                            "${startupMetadata.pageSize} but provided page size is ${config.cachePageSize} " +
                            "page size will be updated to ${startupMetadata.pageSize}"
                )

                config.cachePageSize = startupMetadata.pageSize
            }

            if (fileLength != startupMetadata.fileLengthBoundary) {
                logger.warn(
                    "Environment $location was created with maximum files size equals to " +
                            "${startupMetadata.fileLengthBoundary} but provided file size is $fileLength " +
                            "file size will be updated to ${startupMetadata.fileLengthBoundary}"
                )
                config.fileSize = startupMetadata.fileLengthBoundary / 1024
            }

            fileLengthBound = startupMetadata.fileLengthBoundary

            if (fileLengthBound % config.cachePageSize != 0L) {
                throw InvalidSettingException("File size should be a multiple of cache page size.")
            }

            cachePageSize = startupMetadata.pageSize

            if (expectedEnvironmentVersion != startupMetadata.environmentFormatVersion) {
                throw ExodusException(
                    "For environment $location expected format version is $expectedEnvironmentVersion " +
                            "but  data are stored using version ${startupMetadata.environmentFormatVersion}"
                )
            }

            val blockSetMutable = BlockSet.Immutable(fileLength).beginWrite()
            if (!rwIsReadonly && !startupMetadata.isCorrectlyClosed || needToPerformMigration) {
                if (!startupMetadata.isCorrectlyClosed) {
                    logger.error(
                        "Environment located at ${reader.location} has been closed incorrectly. " +
                                "Data check routine is started to assess data consistency ..."
                    )
                }

                checkLogConsistency(blockSetMutable)

                if (!startupMetadata.isCorrectlyClosed) {
                    logger.error("Data check is completed for environment ${reader.location}.")
                }
            } else {
                val blockIterator = reader.blocks.iterator()

                while (blockIterator.hasNext()) {
                    val block = blockIterator.next()
                    blockSetMutable.add(block.address, block)
                }
            }

            val blockSetImmutable = blockSetMutable.endWrite()

            val memoryUsage = config.memoryUsage
            val nonBlockingCache = config.isNonBlockingCache
            val useSoftReferences = config.cacheUseSoftReferences
            val generationCount = config.cacheGenerationCount

            cache = if (memoryUsage != 0L) {
                if (config.isSharedCache)
                    getSharedCache(
                        memoryUsage,
                        cachePageSize,
                        nonBlockingCache,
                        useSoftReferences,
                        generationCount
                    )
                else
                    SeparateLogCache(memoryUsage, cachePageSize, nonBlockingCache, useSoftReferences, generationCount)
            } else {
                val memoryUsagePercentage = config.memoryUsagePercentage
                if (config.isSharedCache)
                    getSharedCache(
                        memoryUsagePercentage, cachePageSize, nonBlockingCache, useSoftReferences,
                        generationCount
                    )
                else
                    SeparateLogCache(
                        memoryUsagePercentage, cachePageSize, nonBlockingCache, useSoftReferences,
                        generationCount
                    )
            }

            val writeBoundary = (fileLength / cachePageSize).toInt()
            writeBoundarySemaphore = if (config.isSharedCache) {
                getSharedWriteBoundarySemaphore(writeBoundary)
            } else {
                Semaphore(writeBoundary)
            }

            DeferredIO.getJobProcessor()
            isClosing = false

            val lastFileAddress = blockSetMutable.maximum
            updateLogIdentity()

            val page: ByteArray
            val highAddress: Long

            if (lastFileAddress == null) {
                page = ByteArray(cachePageSize)
                highAddress = 0
            } else {
                blockSetMutable.getBlock(lastFileAddress)
                val lastBlock = blockSetMutable.getBlock(lastFileAddress)
                val lastFileLength = lastBlock.length()
                val currentHighAddress = lastFileAddress + lastFileLength
                val highPageAddress = getHighPageAddress(currentHighAddress)
                val highPageContent = ByteArray(cachePageSize)


                if (currentHighAddress > 0) {
                    readFromBlock(lastBlock, highPageAddress, highPageContent, currentHighAddress)
                }

                page = highPageContent
                highAddress = currentHighAddress
            }

            if (reader is FileDataReader) {
                reader.setLog(this)
            }

            val maxWriteBoundary = (fileLengthBound / cachePageSize).toInt()
            this.writer = BufferedDataWriter(
                this,
                dataWriter,
                !needToPerformMigration,
                getSharedWriteBoundarySemaphore(maxWriteBoundary),
                maxWriteBoundary, blockSetImmutable, highAddress, page, config.syncPeriod
            )



            if (lastFileAddress != null && (
                        !startupMetadata.isCorrectlyClosed || needToPerformMigration)
            ) {
                // here we should check whether last loggable is written correctly
                val lastFileLoggables = LoggableIterator(this, lastFileAddress)
                while (lastFileLoggables.hasNext()) {
                    val loggable = lastFileLoggables.next()
                    val dataLength = if (NullLoggable.isNullLoggable(loggable)) 0 else loggable.dataLength
                    if (dataLength > 0) {
                        // if not null loggable read all data to the end
                        val data = loggable.data.iterator()
                        for (i in 0 until dataLength) {
                            if (!data.hasNext()) {
                                throw ExodusException(
                                    "Can't read loggable fully" + LogUtil.getWrongAddressErrorMessage(
                                        data.address,
                                        fileLengthBound
                                    )
                                )
                            }
                            data.next()
                        }
                    } else if (dataLength < 0) {
                        DataCorruptionException.raise(
                            "Negative loggable length, database is corrupted", this,
                            loggable.address
                        )
                    }
                    val approvedHighAddressCandidate = loggable.end()
                    if (approvedHighAddressCandidate > highAddress) {
                        DataCorruptionException.raise(
                            "Loggable end is out of bounds of the log",
                            this, loggable.address
                        )
                    }
                }
            }

            if (needToPerformMigration) {
                val writtenInPage = highAddress and (cachePageSize - 1).toLong()

                if (writtenInPage > 0) {
                    padWholePageWithNulls()
                }
            }

            formatWithHashCodeIsUsed = !needToPerformMigration
            nullPage = BufferedDataWriter.generateNullPage(cachePageSize)

            if (config.isWarmup) {
                warmup()
            }
        } catch (ex: RuntimeException) {
            release()
            throw ex
        }
    }

    private fun padWholePageWithNulls() {
        beginWrite()
        beforeWrite()
        try {
            writer.padWholePageWithNulls()
            writer.flush()
        } finally {
            endWrite()
        }
    }

    fun dataSpaceLeftInPage(address: Long): Int {
        val pageAddress = (cachePageSize - 1).toLong().inv() and address
        val writtenSpace = address - pageAddress

        assert(writtenSpace >= 0 && writtenSpace < cachePageSize - BufferedDataWriter.LOGGABLE_DATA)
        return cachePageSize - BufferedDataWriter.LOGGABLE_DATA - writtenSpace.toInt()
    }

    fun switchToReadOnlyMode() {
        rwIsReadonly = true
    }

    fun updateStartUpDbRoot(rootAddress: Long) {
        startupMetadata.rootAddress = rootAddress
    }

    fun getStartUpDbRoot(): Long {
        return startupMetadata.rootAddress
    }

    private fun checkLogConsistency(blockSetMutable: BlockSet.Mutable) {
        val blockIterator = reader.blocks.iterator()
        if (!blockIterator.hasNext()) {
            return
        }
        if (config.isCleanDirectoryExpected) {
            throw ExodusException("Clean log is expected")
        }

        val clearInvalidLog = config.isClearInvalidLog
        var hasNext: Boolean
        do {
            val block = blockIterator.next()
            val address = block.address
            val blockLength = block.length()
            var clearLogReason: String? = null
            // if it is not the last file and its size is not as expected
            hasNext = blockIterator.hasNext()
            if (blockLength > fileLengthBound || hasNext && blockLength != fileLengthBound) {
                clearLogReason =
                    "Unexpected file length" + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound)
            }
            // if the file address is not a multiple of fileLengthBound
            if (clearLogReason == null && address != getFileAddress(address)) {
                if (rwIsReadonly || !clearInvalidLog) {
                    throw ExodusException(
                        "Unexpected file address " +
                                LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(
                            address,
                            fileLengthBound
                        )
                    )
                }
                clearLogReason = "Unexpected file address " +
                        LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound)
            }

            if (clearLogReason != null) {
                if (rwIsReadonly || !clearInvalidLog) {
                    throw ExodusException(clearLogReason)
                }
                logger.error("Clearing log due to: $clearLogReason")
                blockSetMutable.clear()
                dataWriter.clear()
                break
            }

            val page = ByteArray(cachePageSize)
            for (pageAddress in 0 until blockLength step cachePageSize.toLong()) {
                if (formatWithHashCodeIsUsed) {
                    val read = block.read(page, block.address + pageAddress, 0, cachePageSize)
                    if (hasNext || !rwIsReadonly) {
                        if (read != cachePageSize) {
                            DataCorruptionException.raise(
                                "Page is broken. Expected and actual" +
                                        " page sizes are different. {expected: $cachePageSize , actual: $read}",
                                this,
                                pageAddress
                            )
                        }

                        BufferedDataWriter.checkPageConsistency(
                            pageAddress,
                            page,
                            cachePageSize,
                            this
                        )
                    }
                }
            }

            blockSetMutable.add(address, block)
        } while (hasNext)
    }


    fun beginWrite(): Long {
        return writer.beforeWrite()
    }

    fun endWrite(): Long {
        return writer.endWrite()
    }

    fun needsToBeSynchronized(): Boolean {
        return writer.needsToBeSynchronized()
    }

    fun getFileAddress(address: Long): Long {
        return address - address % fileLengthBound
    }

    fun getNextFileAddress(fileAddress: Long): Long {
        val files = writer.getFilesFrom(fileAddress)

        if (files.hasNext()) {
            val result = files.nextLong()
            if (result != fileAddress) {
                throw ExodusException("There is no file by address $fileAddress")
            }
            if (files.hasNext()) {
                return files.nextLong()
            }
        }

        return Loggable.NULL_ADDRESS
    }

    private fun isLastFileAddress(address: Long, highAddress: Long): Boolean {
        return getFileAddress(address) == getFileAddress(highAddress)
    }

    fun isLastWrittenFileAddress(address: Long): Boolean {
        return getFileAddress(address) == getFileAddress(writtenHighAddress)
    }

    fun adjustLoggableAddress(address: Long, offset: Long): Long {
        if (!formatWithHashCodeIsUsed) {
            return address + offset
        }

        val cachePageReminderMask = (cachePageSize - 1).toLong()
        val writtenInPage = address and cachePageReminderMask
        val pageAddress = address and (cachePageReminderMask.inv())

        val adjustedPageSize = cachePageSize - BufferedDataWriter.LOGGABLE_DATA
        val writtenSincePageStart = writtenInPage + offset
        val fullPages = writtenSincePageStart / adjustedPageSize

        return pageAddress + writtenSincePageStart + fullPages * BufferedDataWriter.LOGGABLE_DATA
    }


    fun hasAddress(address: Long): Boolean {
        val fileAddress = getFileAddress(address)
        val files = writer.getFilesFrom(fileAddress)
        val highAddress = writer.highAddress

        if (!files.hasNext()) {
            return false
        }
        val leftBound = files.nextLong()
        return leftBound == fileAddress && leftBound + getFileSize(leftBound, highAddress) > address
    }

    fun hasAddressRange(from: Long, to: Long): Boolean {
        var fileAddress = getFileAddress(from)
        val files = writer.getFilesFrom(fileAddress)
        val highAddress = writer.highAddress

        do {
            if (!files.hasNext() || files.nextLong() != fileAddress) {
                return false
            }
            fileAddress += getFileSize(fileAddress, highAddress)
        } while (fileAddress in (from + 1)..to)

        return true
    }

    @JvmOverloads
    fun getFileSize(fileAddress: Long, highAddress: Long = writer.highAddress): Long {
        // readonly files (not last ones) all have the same size
        return if (!isLastFileAddress(fileAddress, highAddress)) {
            fileLengthBound
        } else getLastFileSize(fileAddress, highAddress)
    }

    private fun getLastFileSize(fileAddress: Long, highAddress: Long): Long {
        val result = highAddress % fileLengthBound
        return if (result == 0L && highAddress != fileAddress) {
            fileLengthBound
        } else result
    }

    fun getCachedPage(pageAddress: Long): ByteArray {
        return cache.getPage(this, writer, pageAddress)
    }

    fun getPageIterable(pageAddress: Long): ArrayByteIterable {
        return cache.getPageIterable(this, writer, pageAddress, formatWithHashCodeIsUsed)
    }

    fun addBlockListener(listener: BlockListener) {
        synchronized(blockListeners) {
            blockListeners.add(listener)
        }
    }

    fun addReadBytesListener(listener: ReadBytesListener) {
        synchronized(readBytesListeners) {
            readBytesListeners.add(listener)
        }
    }

    /**
     * Reads a random access loggable by specified address in the log.
     *
     * @param address - location of a loggable in the log.
     * @return instance of a loggable.
     */
    fun read(address: Long): RandomAccessLoggable {
        return read(readIteratorFrom(address), address)
    }

    fun getWrittenLoggableType(address: Long, max: Byte): Byte {
        val pageOffset = address and (cachePageSize - 1).toLong()
        val pageAddress = address - pageOffset

        var page = writer.getCurrentlyWritten(pageAddress)
        if (page == null) {
            page = getCachedPage(pageAddress)
        }

        val type = page[pageOffset.toInt()] xor 0x80.toByte()
        if (type > max) {
            throw ExodusException("Invalid loggable type : $type")
        }

        return type
    }

    @JvmOverloads
    fun read(it: ByteIteratorWithAddress, address: Long = it.address): RandomAccessLoggable {
        val type = it.next() xor 0x80.toByte()
        return if (NullLoggable.isNullLoggable(type)) {
            NullLoggable.create(address, adjustLoggableAddress(address, 1))
        } else read(type, it, address)
    }

    /**
     * Just like [.read] reads loggable which never can be a [NullLoggable].
     *
     * @return a loggable which is not[NullLoggable]
     */
    fun readNotNull(it: ByteIteratorWithAddress, address: Long): RandomAccessLoggable {
        return read(it.next() xor 0x80.toByte(), it, address)
    }

    private fun read(type: Byte, it: ByteIteratorWithAddress, address: Long): RandomAccessLoggable {
        val structureId = CompressedUnsignedLongByteIterable.getInt(it)
        val dataLength = CompressedUnsignedLongByteIterable.getInt(it)
        val dataAddress = it.address

        if (dataLength > 0 && it.availableInCurrentPage(dataLength)) {
            val end = dataAddress + dataLength

            return SinglePageLoggable(
                address, end,
                type, structureId, dataAddress, it.currentPage,
                it.offset, dataLength
            )
        }

        val data = MultiPageByteIterableWithAddress(dataAddress, dataLength, this)

        return MultiPageLoggable(
            address,
            type, data, dataLength, structureId, this
        )
    }

    fun getLoggableIterator(startAddress: Long): LoggableIterator {
        return LoggableIterator(this, startAddress)
    }

    fun tryWrite(type: Byte, structureId: Int, data: ByteIterable): Long {
        // allow new file creation only if new file starts loggable
        val result = writeContinuously(type, structureId, data)
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls()
        }
        return result
    }

    /**
     * Writes a loggable to the end of the log padding the log with nulls if necessary.
     * So auto-alignment guarantees the loggable to be placed in a single file.
     *
     * @param loggable - loggable to write.
     * @return address where the loggable was placed.
     */
    fun write(loggable: Loggable): Long {
        return write(loggable.type, loggable.structureId, loggable.data)
    }

    fun write(type: Byte, structureId: Int, data: ByteIterable): Long {
        // allow new file creation only if new file starts loggable
        var result = writeContinuously(type, structureId, data)
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls()
            result = writeContinuously(type, structureId, data)
            if (result < 0) {
                throw TooBigLoggableException()
            }
        }
        return result
    }

    /**
     * Returns the first loggable in the log of specified type.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    fun getFirstLoggableOfType(type: Int): Loggable? {
        val files = writer.getFilesFrom(0)

        while (files.hasNext()) {
            val fileAddress = files.nextLong()
            val it = getLoggableIterator(fileAddress)

            while (it.hasNext()) {
                val loggable = it.next()

                if (loggable == null || loggable.address >= fileAddress + fileLengthBound) {
                    break
                }

                if (loggable.type.toInt() == type) {
                    return loggable
                }
            }
        }
        return null
    }

    /**
     * Returns the last loggable in the log of specified type.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    fun getLastLoggableOfType(type: Int): Loggable? {
        var result: Loggable? = null
        val files = writer.allFiles()

        for (fileAddress in files) {
            val it = getLoggableIterator(fileAddress)

            while (it.hasNext()) {
                val loggable = it.next()

                if (loggable == null || loggable.address >= fileAddress + fileLengthBound) {
                    break
                }

                if (loggable.type.toInt() == type) {
                    result = loggable
                }
            }
        }

        return result
    }

    /**
     * Returns the last loggable in the log of specified type which address is less than beforeAddress.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    fun getLastLoggableOfTypeBefore(type: Int, beforeAddress: Long): Loggable? {
        var result: Loggable? = null
        for (fileAddress in writer.allFiles()) {
            if (fileAddress >= beforeAddress) {
                continue
            }

            val it = getLoggableIterator(fileAddress)
            while (it.hasNext()) {
                val loggable = it.next() ?: break
                val address = loggable.address
                if (address >= beforeAddress || address >= fileAddress + fileLengthBound) {
                    break
                }
                if (loggable.type.toInt() == type) {
                    result = loggable
                }
            }
        }
        return result
    }

    fun isImmutableFile(fileAddress: Long): Boolean {
        return fileAddress + fileLengthBound <= writer.highAddress
    }

    fun flush() {
        writer.flush()
        if (config.isDurableWrite) {
            sync()
        }
    }

    fun sync() {
        writer.sync()
    }

    override fun close() {
        isClosing = true

        if (!rwIsReadonly) {
            val highAddress = writer.highAddress
            if (formatWithHashCodeIsUsed && (highAddress.toInt() and (cachePageSize - 1)) != 0) {
                beginWrite()
                try {
                    beforeWrite()

                    //we pad page with nulls to ensure that all pages could be checked on consistency
                    //by hash code which is stored at the end of the page.
                    val written = writer.padPageWithNulls()
                    if (written == 0) {
                        throw ExodusException("Invalid value of tip of the log $highAddress")
                    }

                    writer.flush()
                    writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
                } finally {
                    endWrite()
                }
            }

            sync()

            if (reader is FileDataReader) {
                startupMetadata.closeAndUpdate(reader)
            }
        }

        reader.close()
        writer.close()

        release()
    }

    fun release() {
        if (!config.isLockIgnored) {
            dataWriter.release()
        }
    }

    fun clear() {
        cache.clear()
        reader.close()
        writer.clear()

        updateLogIdentity()
    }

    // for tests only
    @Deprecated("for tests only")
    fun forgetFile(address: Long) {
        beginWrite()
        forgetFiles(longArrayOf(address))
        endWrite()
    }

    @Deprecated("for tests only")
    fun clearCache() {
        cache.clear()
    }

    fun forgetFiles(files: LongArray) {
        writer.forgetFiles(files, fileLengthBound)
    }

    @JvmOverloads
    fun removeFile(
        address: Long,
        rbt: RemoveBlockType = RemoveBlockType.Delete
    ) {
        writer.removeBlock(address, rbt)
    }

    fun notifyBeforeBlockDeleted(block: Block) {
        blockListeners.notifyListeners { it.beforeBlockDeleted(block) }
    }

    fun notifyAfterBlockDeleted(address: Long) {
        blockListeners.notifyListeners { it.afterBlockDeleted(address) }
    }

    fun clearFileFromLogCache(address: Long, offset: Long = 0L) {
        var off = offset
        while (off < fileLengthBound) {
            cache.removePage(this, address + off)
            off += cachePageSize
        }
    }

    fun doPadWithNulls() {
        if (writer.padWithNulls(fileLengthBound, nullPage)) {
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        }
    }

    fun readBytes(output: ByteArray, pageAddress: Long): Int {
        val fileAddress = getFileAddress(pageAddress)
        val files = writer.getFilesFrom(fileAddress)
        val highAddress = writer.highAddress

        if (files.hasNext()) {
            val leftBound = files.nextLong()
            val fileSize = getFileSize(leftBound, highAddress)

            if (leftBound == fileAddress && fileAddress + fileSize > pageAddress) {
                val block = writer.getBlock(fileAddress)
                if (block == null) {
                    BlockNotFoundException.raise(this, pageAddress)
                    return 0
                }

                return readFromBlock(block, pageAddress, output, highAddress)
            }
            if (fileAddress < (writer.minimumFile ?: -1L)) {
                BlockNotFoundException.raise(
                    "Address is out of log space, underflow",
                    this, pageAddress
                )
            }
            if (fileAddress >= (writer.maximumFile ?: -1L)) {
                BlockNotFoundException.raise(
                    "Address is out of log space, overflow",
                    this, pageAddress
                )
            }
        }
        BlockNotFoundException.raise(this, pageAddress)
        return 0
    }

    private fun readFromBlock(
        block: Block,
        pageAddress: Long,
        output: ByteArray,
        highAddress: Long
    ): Int {
        val readBytes = block.read(
            output,
            pageAddress - block.address, 0, output.size
        )
        val checkConsistency = config.isCheckPagesAtRuntime &&
                formatWithHashCodeIsUsed &&
                (!rwIsReadonly || pageAddress < (highAddress and
                        ((cachePageSize - 1).inv()).toLong()))
        if (checkConsistency) {
            if (readBytes < cachePageSize) {
                DataCorruptionException.raise(
                    "Page size less than expected. " +
                            "{actual : $readBytes, expected $cachePageSize }.", this, pageAddress
                )
            }

            BufferedDataWriter.checkPageConsistency(pageAddress, output, cachePageSize, this)
        }

        val cipherProvider = config.cipherProvider
        if (cipherProvider != null) {
            val encryptedBytes = if (readBytes < cachePageSize) {
                readBytes
            } else {
                if (formatWithHashCodeIsUsed) {
                    cachePageSize - BufferedDataWriter.HASH_CODE_SIZE
                } else {
                    cachePageSize
                }
            }

            cryptBlocksMutable(
                cipherProvider, config.cipherKey, config.cipherBasicIV,
                pageAddress, output, 0, encryptedBytes, LogUtil.LOG_BLOCK_ALIGNMENT
            )
        }
        notifyReadBytes(output, readBytes)
        return readBytes
    }

    fun getWrittenFilesSize(): Int {
        return writer.filesSize
    }

    /**
     * Returns iterator which reads raw bytes of the log starting from specified address.
     *
     * @param address
     * @return instance of ByteIterator
     */
    fun readIteratorFrom(address: Long): DataIterator {
        return DataIterator(this, address)
    }

    private fun tryLock() {
        if (!config.isLockIgnored) {
            val lockTimeout = config.lockTimeout
            if (!dataWriter.lock(lockTimeout)) {
                val exceptionMessage = StringBuilder()
                exceptionMessage.append(
                    "Can't acquire environment lock after "
                ).append(lockTimeout).append(" ms.\n\n Lock owner info: \n").append(dataWriter.lockInfo())
                if (dataWriter is FileDataWriter) {
                    exceptionMessage.append("\n Lock file path: ").append(dataWriter.lockFilePath())
                }
                throw ExodusException(exceptionMessage.toString())
            }
        }
    }

    fun getHighPageAddress(highAddress: Long): Long {
        var alignment = highAddress.toInt() and cachePageSize - 1
        if (alignment == 0 && highAddress > 0) {
            alignment = cachePageSize
        }
        return highAddress - alignment // aligned address
    }

    fun writeContinuously(type: Byte, structureId: Int, data: ByteIterable): Long {
        if (rwIsReadonly) {
            throw ExodusException("Environment is working in read-only mode. No writes are allowed")
        }

        var result = beforeWrite()

        val isNull = NullLoggable.isNullLoggable(type)
        var recordLength = 1

        if (isNull) {
            writer.write(type xor 0x80.toByte(), result)
        } else {
            val structureIdIterable = CompressedUnsignedLongByteIterable.getIterable(structureId.toLong())
            val dataLength = data.length
            val dataLengthIterable = CompressedUnsignedLongByteIterable.getIterable(dataLength.toLong())
            recordLength += structureIdIterable.length
            recordLength += dataLengthIterable.length
            recordLength += dataLength

            val leftInPage =
                cachePageSize - (result.toInt() and (cachePageSize - 1)) - BufferedDataWriter.LOGGABLE_DATA
            val delta = if (leftInPage in 1 until recordLength && recordLength < (cachePageSize shr 4)) {
                leftInPage + BufferedDataWriter.LOGGABLE_DATA
            } else {
                0
            }

            if (!writer.fitsIntoSingleFile(fileLengthBound, recordLength + delta)) {
                return -1L
            }

            if (delta > 0) {
                val written = writer.padPageWithNulls()
                assert(written == delta)
                result += written
                assert(result % cachePageSize.toLong() == 0L)
            }

            writer.write(type xor 0x80.toByte(), result)

            writeByteIterable(writer, structureIdIterable)
            writeByteIterable(writer, dataLengthIterable)
            if (dataLength > 0) {
                writeByteIterable(writer, data)
            }

        }

        writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        return result
    }

    private fun beforeWrite(): Long {
        val result = writer.currentHighAddress

        // begin of test-only code
        @Suppress("DEPRECATION") val testConfig = this.testConfig
        if (testConfig != null) {
            val maxHighAddress = testConfig.maxHighAddress
            if (maxHighAddress in 0..result) {
                throw ExodusException("Can't write more than $maxHighAddress")
            }
        }
        // end of test-only code

        writer.openNewFileIfNeeded(fileLengthBound, this)
        return result
    }

    /**
     * Sets LogTestConfig.
     * Is destined for tests only, please don't set a not-null value in application code.
     */
    @Deprecated("for tests only")
    fun setLogTestConfig(testConfig: LogTestConfig?) {
        @Suppress("DEPRECATION")
        this.testConfig = testConfig
    }

    fun notifyBlockCreated(block: Block) {
        blockListeners.notifyListeners { it.blockCreated(block) }
    }

    fun notifyBlockModified(block: Block) {
        blockListeners.notifyListeners { it.blockModified(block) }
    }

    fun mutableBlocksUnsafe(): BlockSet.Mutable {
        return writer.mutableBlocksUnsafe()
    }

    fun updateBlockSetHighAddressUnsafe(prevHighAddress: Long, highAddress: Long, blockSet: BlockSet.Immutable) {
        writer.updateBlockSetHighAddressUnsafe(prevHighAddress, highAddress, blockSet)
    }

    private fun notifyReadBytes(bytes: ByteArray, count: Int) {
        readBytesListeners.notifyListeners { it.bytesRead(bytes, count) }
    }

    private inline fun <reified T> List<T>.notifyListeners(call: (T) -> Unit): Array<T> {
        val listeners = synchronized(this) {
            this.toTypedArray()
        }
        listeners.forEach { call(it) }
        return listeners
    }

    private fun updateLogIdentity() {
        identity = identityGenerator.nextId()
    }

    companion object : KLogging() {
        private val identityGenerator = IdGenerator()

        @Volatile
        private var sharedCache: SharedLogCache? = null

        @Volatile
        private var sharedWriteBoundarySemaphore: Semaphore? = null

        /**
         * For tests only!!!
         */
        @JvmStatic
        @Deprecated("for tests only")
        fun invalidateSharedCache() {
            synchronized(Log::class.java) {
                sharedCache = null
            }
        }

        private fun getSharedWriteBoundarySemaphore(writeBoundary: Int): Semaphore {
            var result = sharedWriteBoundarySemaphore
            if (result == null) {
                synchronized(Log::class.java) {
                    sharedWriteBoundarySemaphore = Semaphore(writeBoundary)
                    result = sharedWriteBoundarySemaphore
                }
            }

            return result.notNull
        }

        private fun getSharedCache(
            memoryUsage: Long,
            pageSize: Int,
            nonBlocking: Boolean,
            useSoftReferences: Boolean,
            cacheGenerationCount: Int
        ): LogCache {
            var result = sharedCache
            if (result == null) {
                synchronized(Log::class.java) {
                    if (sharedCache == null) {
                        sharedCache = SharedLogCache(
                            memoryUsage, pageSize, nonBlocking, useSoftReferences,
                            cacheGenerationCount
                        )
                    }
                    result = sharedCache
                }
            }
            return result.notNull.also { cache ->
                checkCachePageSize(pageSize, cache)
                checkUseSoftReferences(useSoftReferences, cache)
            }
        }

        private fun getSharedCache(
            memoryUsagePercentage: Int,
            pageSize: Int,
            nonBlocking: Boolean,
            useSoftReferences: Boolean,
            cacheGenerationCount: Int
        ): LogCache {
            var result = sharedCache
            if (result == null) {
                synchronized(Log::class.java) {
                    if (sharedCache == null) {
                        sharedCache = SharedLogCache(
                            memoryUsagePercentage, pageSize, nonBlocking, useSoftReferences,
                            cacheGenerationCount
                        )
                    }
                    result = sharedCache
                }
            }
            return result.notNull.also { cache ->
                checkCachePageSize(pageSize, cache)
                checkUseSoftReferences(useSoftReferences, cache)
            }
        }

        private fun checkCachePageSize(pageSize: Int, cache: LogCache) {
            if (cache.pageSize != pageSize) {
                throw ExodusException(
                    "SharedLogCache was created with page size ${cache.pageSize}" +
                            " and then requested with page size $pageSize. EnvironmentConfig.LOG_CACHE_PAGE_SIZE is set manually."
                )
            }
        }

        private fun checkUseSoftReferences(useSoftReferences: Boolean, cache: SharedLogCache) {
            if (cache.useSoftReferences != useSoftReferences) {
                throw ExodusException(
                    "SharedLogCache was created with useSoftReferences = ${cache.useSoftReferences}" +
                            " and then requested with useSoftReferences = $useSoftReferences. EnvironmentConfig.LOG_CACHE_USE_SOFT_REFERENCES is set manually."
                )
            }
        }

        /**
         * Writes byte iterator to the log returning its length.
         *
         * @param writer   a writer
         * @param iterable byte iterable to write.
         * @return
         */
        private fun writeByteIterable(writer: BufferedDataWriter, iterable: ByteIterable) {
            val length = iterable.length

            if (iterable is ArrayByteIterable) {
                val bytes = iterable.baseBytes
                val offset = iterable.baseOffset()

                if (length == 1) {
                    writer.write(bytes[0], -1)
                } else {
                    writer.write(bytes, offset, length)
                }
            } else if (length >= 3) {
                val bytes = iterable.baseBytes
                val offset = iterable.baseOffset()

                writer.write(bytes, offset, length)
            } else {
                val iterator = iterable.iterator()
                writer.write(iterator.next(), -1)
                if (length == 2) {
                    writer.write(iterator.next(), -1)
                }
            }
        }
    }
}
