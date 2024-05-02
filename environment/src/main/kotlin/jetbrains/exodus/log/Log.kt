/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
import jetbrains.exodus.crypto.InvalidCipherParametersException
import jetbrains.exodus.crypto.cryptBlocksMutable
import jetbrains.exodus.env.DatabaseRoot
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.io.*
import jetbrains.exodus.io.inMemory.MemoryDataReader
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.IdGenerator
import mu.KLogging
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.Semaphore
import kotlin.experimental.xor
import kotlin.math.min

class Log(val config: LogConfig, expectedEnvironmentVersion: Int) : Closeable, CacheDataProvider {

    val created = System.currentTimeMillis()

    @JvmField
    var cache: LogCache

    private val writeBoundarySemaphore: Semaphore

    @Volatile
    var isClosing: Boolean = false
        private set

    private var identity: Int = 0

    private val reader: DataReader = config.reader
    private val dataWriter: DataWriter = config.writer

    private val writer: BufferedDataWriter
    private var writeThread: Thread? = null

    private val blockListeners = ArrayList<BlockListener>(2)
    private val readBytesListeners = ArrayList<ReadBytesListener>(2)

    private var startupMetadata: StartupMetadata

    val isClosedCorrectly: Boolean
        get() = startupMetadata.isCorrectlyClosed

    var restoredFromBackup: Boolean = false
        private set

    /** Size of single page in log cache. */
    var cachePageSize: Int

    /**
     * Indicate whether it is needed to perform migration to the format which contains
     * hash codes of content inside the pages.
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

    val highReadAddress: Long
        get() {
            return if (writeThread == Thread.currentThread()) {
                writer.currentHighAddress
            } else {
                writer.highAddress
            }
        }

    val lowFileAddress: Long
        get() {
            val result = writer.minimumFile
            return result ?: Loggable.NULL_ADDRESS
        }

    val highFileAddress: Long
        get() = getFileAddress(highReadAddress)

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
            rwIsReadonly = false

            var fileLength = config.fileSize * 1024L

            val logContainsBlocks = reader.blocks.iterator().hasNext()
            val metadata = if (reader is FileDataReader) {
                StartupMetadata.open(
                    reader, config.cachePageSize, expectedEnvironmentVersion,
                    fileLength, logContainsBlocks
                )
            } else {
                StartupMetadata.createStub(
                    config.cachePageSize, !logContainsBlocks,
                    expectedEnvironmentVersion, fileLength
                )
            }

            var needToPerformMigration = false

            if (metadata != null) {
                startupMetadata = metadata
            } else {
                startupMetadata = StartupMetadata.createStub(
                    config.cachePageSize, !logContainsBlocks, expectedEnvironmentVersion,
                    fileLength
                )
                needToPerformMigration = logContainsBlocks
            }

            val backupLocation = Path.of(location).resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME)
            if (!needToPerformMigration && !startupMetadata.isCorrectlyClosed) {
                if (Files.exists(backupLocation)) {
                    logger.info("Database $location : trying to restore from dynamic backup...")
                    val backupMetadataBuffer = ByteBuffer.allocate(BackupMetadata.FILE_SIZE)
                    FileChannel.open(backupLocation, StandardOpenOption.READ).use { channel ->
                        while (backupMetadataBuffer.remaining() > 0) {
                            val r = channel.read(backupMetadataBuffer)
                            if (r == -1) {
                                throw IOException("Unexpected end of file")
                            }
                        }
                    }

                    backupMetadataBuffer.rewind()
                    val backupMetadata = BackupMetadata.deserialize(
                        backupMetadataBuffer,
                        startupMetadata.currentVersion, startupMetadata.isUseZeroFile
                    )
                    Files.deleteIfExists(backupLocation)

                    if (backupMetadata == null || backupMetadata.rootAddress < 0 ||
                        (backupMetadata.lastFileOffset % backupMetadata.pageSize.toLong()) != 0L
                    ) {
                        logger.warn("Backup is not stored correctly for database $location.")
                    } else {
                        val lastFileName = LogUtil.getLogFilename(backupMetadata.lastFileAddress)
                        val lastSegmentFile = Path.of(location).resolve(lastFileName)

                        if (!Files.exists(lastSegmentFile)) {
                            logger.warn("Backup is not stored correctly for database $location.")
                        } else {
                            logger.info(
                                "Found backup. " +
                                        "Database $location will be restored till file $lastFileName, " +
                                        " file length will be set too " +
                                        "${backupMetadata.lastFileOffset}. DB root address ${backupMetadata.rootAddress}"
                            )

                            SharedOpenFilesCache.invalidate()
                            try {
                                SharedOpenFilesCache.invalidate()
                                dataWriter.close()

                                val blocks = TreeMap<Long, FileDataReader.FileBlock>()
                                val blockIterator = reader.blocks.iterator()
                                while (blockIterator.hasNext()) {
                                    val block = blockIterator.next()
                                    blocks[block.address] = block as FileDataReader.FileBlock
                                }

                                logger.info("Files in database: $location")
                                val blockAddressIterator = blocks.keys.iterator()
                                while (blockAddressIterator.hasNext()) {
                                    val address = blockAddressIterator.next()
                                    logger.info(LogUtil.getLogFilename(address))
                                }

                                copyFilesInRestoreTempDir(
                                    blocks.tailMap(
                                        backupMetadata.lastFileAddress,
                                        true
                                    ).values.iterator()
                                )

                                val blocksToTruncateIterator = blocks.tailMap(
                                    backupMetadata.lastFileAddress,
                                    false
                                ).values.iterator()

                                truncateFile(
                                    lastSegmentFile.toFile(),
                                    backupMetadata.lastFileOffset,
                                    null
                                )

                                if (blocksToTruncateIterator.hasNext()) {
                                    val blockToDelete = blocksToTruncateIterator.next()
                                    logger.info("File ${LogUtil.getLogFilename(blockToDelete.address)} will be deleted.")


                                    if (!blockToDelete.canWrite()) {
                                        if (!blockToDelete.setWritable(true)) {
                                            throw ExodusException(
                                                "Can not write into file " + blockToDelete.absolutePath
                                            )
                                        }
                                    }

                                    Files.deleteIfExists(Path.of(blockToDelete.toURI()))
                                }

                                backupMetadata.alterMetadata(startupMetadata)
                                startupMetadata = backupMetadata
                                restoredFromBackup = true
                            } catch (ex: Exception) {
                                logger.error(
                                    "Failed to restore database $location from dynamic backup. ",
                                    ex
                                )
                            }
                        }
                    }
                }
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
                fileLength = startupMetadata.fileLengthBoundary
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


            var logWasChanged = false

            formatWithHashCodeIsUsed = !needToPerformMigration

            var tmpLeftovers = false
            if (reader is FileDataReader) {
                LogUtil.listTlcFiles(File(location)).use {
                    var count = 0

                    it.forEach { path ->
                        Files.deleteIfExists(path)
                        count++
                    }

                    if (count > 0) {
                        logger.error(
                            "Temporary files which are used during environment" +
                                    " auto-recovery have been found, typically it indicates that recovery routine finished " +
                                    "incorrectly, triggering database check."
                        )
                        tmpLeftovers = true
                    }
                }
            }

            var blockSetMutable = BlockSet.Immutable(fileLength).beginWrite()
            val blockIterator = reader.blocks.iterator()

            while (blockIterator.hasNext()) {
                val block = blockIterator.next()
                blockSetMutable.add(block.address, block)
            }

            var incorrectLastSegmentSize = false
            if (!needToPerformMigration && blockSetMutable.size() > 0) {
                val lastAddress = blockSetMutable.maximum!!

                val lastBlock = blockSetMutable.getBlock(lastAddress)
                val lastFileLength = lastBlock.length()
                if (lastFileLength and (cachePageSize - 1).toLong() > 0) {
                    logger.error(
                        "Unexpected size of the last segment $lastBlock , " +
                                "segment size should be quantified by page size. Segment size $lastFileLength. " +
                                "Page size $cachePageSize"
                    )
                    incorrectLastSegmentSize = true
                }
            }

            val checkDataConsistency = if (!rwIsReadonly) {
                if (reader is FileDataReader) {
                    if (!startupMetadata.isCorrectlyClosed || tmpLeftovers
                        || incorrectLastSegmentSize
                    ) {
                        logger.warn(
                            "Environment located at $location has been closed incorrectly. " +
                                    "Data check routine is started to assess data consistency ..."
                        )
                        true
                    } else if (needToPerformMigration) {
                        logger.warn(
                            "Environment located at $location has been created with different version. " +
                                    "Data check routine is started to assess data consistency ..."
                        )
                        true
                    } else if (config.isForceDataCheckOnStart) {
                        logger.warn(
                            "Environment located at $location will be checked for data consistency " +
                                    "due to the forceDataCheckOnStart option is set to true."
                        )
                        true
                    } else {
                        false
                    }
                } else if (reader is MemoryDataReader) {
                    true
                } else {
                    throw IllegalStateException("Unknown reader type $reader")
                }
            } else {
                false
            }

            if (checkDataConsistency) {
                blockSetMutable = BlockSet.Immutable(fileLength).beginWrite()
                logWasChanged = checkLogConsistencyAndUpdateRootAddress(blockSetMutable)

                logger.info("Data check is completed for environment $location.")
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
                    SeparateLogCache(
                        memoryUsage,
                        cachePageSize,
                        nonBlockingCache,
                        useSoftReferences,
                        generationCount
                    )
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

                if (!dataWriter.isOpen) {
                    dataWriter.openOrCreateBlock(lastFileAddress, lastBlock.length())
                }

                val lastFileLength = lastBlock.length()
                val currentHighAddress = lastFileAddress + lastFileLength
                val highPageAddress = getHighPageAddress(currentHighAddress)
                val highPageContent = ByteArray(cachePageSize)


                if (currentHighAddress > 0) {
                    readFromBlock(lastBlock, highPageAddress, highPageContent, currentHighAddress)
                }

                page = highPageContent
                highAddress = currentHighAddress

                if (lastFileLength == fileLengthBound) {
                    dataWriter.close()
                }
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

            val writtenInPage = highAddress and (cachePageSize - 1).toLong()
            nullPage = BufferedDataWriter.generateNullPage(cachePageSize)

            if (!rwIsReadonly && writtenInPage > 0) {
                logger.warn(
                    "Page ${(highAddress and (cachePageSize - 1).toLong())} is not written completely, fixing it. " +
                            "Environment : $location, file : ${
                                LogUtil.getLogFilename(
                                    getFileAddress(
                                        highAddress
                                    )
                                )
                            }."
                )

                if (needToPerformMigration) {
                    padWholePageWithNulls()
                } else {
                    padPageWithNulls()
                }

                logWasChanged = true
            }

            if (logWasChanged) {
                logger.info("Log $location content was changed during the initialization, data sync is performed.")

                beginWrite()
                try {
                    writer.sync()
                } finally {
                    endWrite()
                }
            }

            if (config.isWarmup) {
                warmup()
            }

            Files.deleteIfExists(backupLocation)

            if (needToPerformMigration) {
                switchToReadOnlyMode()
            }
        } catch (ex: RuntimeException) {
            release()
            throw ex
        }
    }

    private fun copyFilesInRestoreTempDir(files: Iterator<FileDataReader.FileBlock>) {
        try {
            val tempDir = createTempRestoreDirectoryWithDate()
            logger.info("Database $location - copying files into the temp directory $tempDir before data restore.")

            while (files.hasNext()) {
                val file = files.next()
                logger.info("Database $location - file $file is copied into the temp directory $tempDir")
                Files.copy(file.toPath(), tempDir.resolve(file.toPath().fileName))
            }

            logger.info("Database $location - copying of files into the temp directory $tempDir is completed.")
        } catch (e: Exception) {
            logger.error("Error during backup of broken files", e)
        }
    }

    private fun createTempRestoreDirectoryWithDate(): Path {
        val dtf = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss")
        val now = LocalDateTime.now()
        val date = dtf.format(now)

        val dirName = "restore-$date-"
        return Files.createTempDirectory(dirName).toAbsolutePath()
    }


    private fun checkLogConsistencyAndUpdateRootAddress(
        blockSetMutable: BlockSet.Mutable,
    ): Boolean {
        val newRootAddress = checkLogConsistency(
            blockSetMutable,
            startupMetadata.rootAddress
        )

        var logWasChanged = false
        if (newRootAddress != Long.MIN_VALUE) {
            startupMetadata.rootAddress = newRootAddress
            logWasChanged = true

            SharedOpenFilesCache.invalidate()

            dataWriter.close()
            val lastBlockAddress = blockSetMutable.maximum

            if (lastBlockAddress != null) {
                val lastBlock = blockSetMutable.getBlock(lastBlockAddress)
                dataWriter.openOrCreateBlock(lastBlockAddress, lastBlock.length())
            }
        }

        return logWasChanged
    }

    private fun padWholePageWithNulls() {
        beginWrite()
        try {
            writer.padWholePageWithNulls()
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        } finally {
            endWrite()
        }
    }

    private fun padPageWithNulls() {
        beginWrite()
        try {
            writer.padPageWithNulls()
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        } finally {
            endWrite()
        }
    }

    fun padPageWithNulls(expiredLoggables: ExpiredLoggableCollection) {
        beginWrite()
        try {
            val currentAddress = writer.currentHighAddress
            val written = writer.padPageWithNulls()

            if (written > 0) {
                expiredLoggables.add(currentAddress, written)
            }

            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        } finally {
            endWrite()
        }
    }

    fun dataSpaceLeftInPage(address: Long): Int {
        val pageAddress = (cachePageSize - 1).toLong().inv() and address
        val writtenSpace = address - pageAddress

        assert(writtenSpace >= 0 && writtenSpace < cachePageSize - BufferedDataWriter.HASH_CODE_SIZE)
        return cachePageSize - BufferedDataWriter.HASH_CODE_SIZE - writtenSpace.toInt()
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

    private fun checkLogConsistency(
        blockSetMutable: BlockSet.Mutable,
        loadedDbRootAddress: Long
    ): Long {
        var blockIterator = reader.blocks.iterator()
        if (!blockIterator.hasNext()) {
            return Long.MIN_VALUE
        }

        if (config.isCleanDirectoryExpected) {
            throw ExodusException("Clean log is expected")
        }

        var blocks = TreeMap<Long, Block>()

        blockLoop@ while (true) {
            while (blockIterator.hasNext()) {
                val block = blockIterator.next()
                blocks[block.address] = block

                if (block is FileDataReader.FileBlock && block.length() > fileLengthBound && !blockIterator.hasNext()) {
                    logger.warn(
                        "File ${LogUtil.getLogFilename(block.address)} is too large, " +
                                "it will be truncated to $fileLengthBound bytes."
                    )

                    val location = Path.of(this.location)

                    val nextAddress = block.address + fileLengthBound
                    val nextBlocName = LogUtil.getLogFilename(nextAddress)
                    val blockLength = block.length()

                    val blockPath = Path.of(block.path)
                    val blocName = block.name

                    val nextBlockPath = location.resolve(nextBlocName)
                    val delBlockPath = location.resolve(
                        blocName.substring(0, blocName.length - LogUtil.LOG_FILE_EXTENSION_LENGTH)
                                + AsyncFileDataWriter.DELETED_FILE_EXTENSION
                    )

                    val firstFile = Files.createTempFile(location, "split-1", ".txd")
                    val secondFile = Files.createTempFile(location, "split-2", ".txd")

                    if (!block.setWritable(true)) {
                        logger.error("Can't set writable permission for file ${block.path}")
                    }

                    FileChannel.open(Path.of(block.path)).use { original ->
                        FileChannel.open(firstFile, StandardOpenOption.WRITE).use {
                            original.transferTo(0, fileLengthBound, it)
                            it.force(true)
                        }

                        FileChannel.open(secondFile, StandardOpenOption.WRITE).use {
                            original.transferTo(fileLengthBound, blockLength - fileLengthBound, it)
                            it.force(true)
                        }
                    }

                    try {
                        Files.move(blockPath, delBlockPath, StandardCopyOption.ATOMIC_MOVE)
                    } catch (ex: AtomicMoveNotSupportedException) {
                        Files.move(blockPath, delBlockPath)
                    }

                    try {
                        Files.move(
                            firstFile, blockPath, StandardCopyOption.ATOMIC_MOVE
                        )
                    } catch (ex: AtomicMoveNotSupportedException) {
                        Files.move(
                            firstFile, blockPath
                        )
                    }

                    try {
                        Files.move(
                            secondFile, nextBlockPath, StandardCopyOption.ATOMIC_MOVE
                        )
                    } catch (ex: AtomicMoveNotSupportedException) {
                        Files.move(
                            secondFile, nextBlockPath
                        )
                    }

                    if (!firstFile.toFile().setReadOnly()) {
                        logger.error("Error during setting of read only property for file $firstFile")
                    }

                    blockIterator = reader.blocks.iterator()
                    blocks = TreeMap<Long, Block>()

                    continue@blockLoop
                }
            }

            break
        }

        apply {
            logger.info("Files found in directory $location ...")
            logger.info("------------------------------------------------------")
            val blockAddressIterator = blocks.keys.iterator()
            while (blockAddressIterator.hasNext()) {
                val address = blockAddressIterator.next()
                logger.info(LogUtil.getLogFilename(address))
            }
            logger.info("------------------------------------------------------")
        }

        val blocksToTruncateLimit = 2

        val clearInvalidLog = config.isClearInvalidLog
        var dbRootAddress = Long.MIN_VALUE
        var dbRootEndAddress = Long.MIN_VALUE

        var loggablesProcessed = 0
        try {
            var counter = 1
            var blocksLimit = 100
            val blocksStep = 100

            while (true) {
                logger.info("Trying to restore database $location. Attempt $counter")

                val blocksToCheckSize = min(blocks.size, blocksLimit)
                val blocksReverse = blocks.descendingMap()
                val blocksToCheck = TreeMap<Long, Block>()

                for (i: Int in 0 until blocksToCheckSize) {
                    val block = blocksReverse.pollFirstEntry()
                    blocksToCheck[block.key] = block.value
                }

                logger.info("Files to be checked for data consistency : ")
                logger.info("------------------------------------------------------")
                val blockAddressIterator = blocksToCheck.keys.iterator()
                while (blockAddressIterator.hasNext()) {
                    val address = blockAddressIterator.next()
                    logger.info(LogUtil.getLogFilename(address))
                }
                logger.info("------------------------------------------------------")

                try {
                    blockSetMutable.clear()

                    val triple = extractRestoreInformation(
                        blocksToCheck.values.iterator(),
                        blockSetMutable
                    )

                    dbRootAddress = triple.first
                    dbRootEndAddress = triple.second
                    loggablesProcessed = triple.third
                    break
                } catch (e: LogCorruptionException) {
                    dbRootAddress = e.dbRootAddress
                    dbRootEndAddress = e.dbRootEndAddress
                    loggablesProcessed = e.loggablesProcessed

                    if (dbRootAddress > 0) {
                        break
                    }

                    if (blocksLimit < blocks.size) {
                        logger.info("Attempt to restore database $location failed. Increasing the number of files to be checked.")

                        blocksLimit = min(blocksLimit + blocksStep, blocks.size)
                        counter++

                    } else {
                        throw e
                    }
                } catch (e: Exception) {
                    throw e
                }
            }

        } catch (exception: Exception) {
            logger.warn("Error during verification of database $location", exception)

            SharedOpenFilesCache.invalidate()

            try {
                if (clearInvalidLog) {
                    clearDataCorruptionLog(exception, blockSetMutable)
                    return -1
                }

                if (dbRootEndAddress > 0) {
                    val endBlockAddress = getFileAddress(dbRootEndAddress)
                    val blocksToTruncate = blocks.tailMap(endBlockAddress, true)


                    if (!config.isProceedDataRestoreAtAnyCost && blocksToTruncate.size > blocksToTruncateLimit) {
                        throw DataCorruptionException(
                            "Database $location is corrupted, " +
                                    "but to be restored till the valid state ${blocksToTruncate.size} " +
                                    "segments out of ${blocks.size} " +
                                    "should be truncated. Segment size $fileLengthBound bytes. Data restore is aborted. " +
                                    "To proceed data restore with such data loss " +
                                    "please restart database with parameter " +
                                    " ${EnvironmentConfig.LOG_PROCEED_DATA_RESTORE_AT_ANY_COST} set to true"
                        )
                    }

                    if (reader is FileDataReader) {
                        @Suppress("UNCHECKED_CAST")
                        copyFilesInRestoreTempDir(
                            (blocksToTruncate.values
                                    as Collection<FileDataReader.FileBlock>).iterator()
                        )
                    }

                    val blocksToTruncateIterator = blocksToTruncate.values.iterator()
                    val endBlock = blocksToTruncateIterator.next()
                    val endBlockLength = dbRootEndAddress % fileLengthBound
                    val endBlockReminder = endBlockLength.toInt() and (cachePageSize - 1)

                    logger.warn(
                        "Data corruption was detected. Reason : \"${exception.message}\". " +
                                "Database '$location' will be truncated till address : $dbRootEndAddress. " +
                                "Name of the file to be truncated : ${
                                    LogUtil.getLogFilename(
                                        endBlockAddress
                                    )
                                }. " +
                                "Initial file size ${endBlock.length()} bytes, final file size $endBlockLength bytes."
                    )

                    if (blocksToTruncate.size > 1) {
                        logger.warn(
                            "The following files will be deleted : " +
                                    blocksToTruncate.keys.asSequence().drop(1)
                                        .joinToString(", ") { LogUtil.getLogFilename(it) }
                        )
                    }


                    if (endBlock is FileDataReader.FileBlock && !endBlock.canWrite()) {
                        if (!endBlock.setWritable(true)) {
                            throw ExodusException(
                                "Can not write into file " + endBlock.absolutePath,
                                exception
                            )
                        }
                    }

                    val position = dbRootEndAddress % fileLengthBound - endBlockReminder
                    val lastPage = if (endBlockReminder > 0) {
                        ByteArray(cachePageSize).also {
                            val read = endBlock.read(it, position, 0, endBlockReminder)
                            if (read != endBlockReminder) {
                                throw ExodusException(
                                    "Can not read segment ${LogUtil.getLogFilename(endBlock.address)}",
                                    exception
                                )
                            }

                            Arrays.fill(it, read, it.size, 0x80.toByte())
                            val cipherProvider = config.cipherProvider
                            if (cipherProvider != null) {
                                cryptBlocksMutable(
                                    cipherProvider, config.cipherKey, config.cipherBasicIV,
                                    endBlock.address + position, it, read, it.size - read,
                                    LogUtil.LOG_BLOCK_ALIGNMENT
                                )
                            }
                            BufferedDataWriter.updatePageHashCode(it)
                            SharedOpenFilesCache.invalidate()
                        }
                    } else {
                        null
                    }

                    when (reader) {
                        is FileDataReader -> {
                            @Suppress("NAME_SHADOWING")
                            val endBlock = endBlock as FileDataReader.FileBlock
                            try {
                                truncateFile(endBlock, position, lastPage)
                            } catch (e: IOException) {
                                logger.error("Error during truncation of file $endBlock", e)
                                throw ExodusException("Can not restore log corruption", exception)
                            }
                        }

                        is MemoryDataReader -> {
                            @Suppress("NAME_SHADOWING")
                            val endBlock = endBlock as MemoryDataReader.MemoryBlock

                            endBlock.truncate(position.toInt())
                            if (lastPage != null) {
                                endBlock.write(lastPage, 0, cachePageSize)
                            }
                        }

                        else -> {
                            throw ExodusException("Invalid reader type : $reader")
                        }
                    }

                    blockSetMutable.add(endBlockAddress, endBlock)

                    if (blocksToTruncateIterator.hasNext()) {
                        val blockToDelete = blocksToTruncateIterator.next()
                        logger.info("File ${LogUtil.getLogFilename(blockToDelete.address)} will be deleted.")
                        blockSetMutable.remove(blockToDelete.address)

                        when (reader) {
                            is FileDataReader -> {
                                @Suppress("NAME_SHADOWING")
                                val blockToDelete = blockToDelete as FileDataReader.FileBlock

                                if (!blockToDelete.canWrite()) {
                                    if (!blockToDelete.setWritable(true)) {
                                        throw ExodusException(
                                            "Can not write into file " + blockToDelete.absolutePath,
                                            exception
                                        )
                                    }
                                }

                                Files.deleteIfExists(Path.of(blockToDelete.toURI()))
                            }

                            is MemoryDataReader -> {
                                reader.memory.removeBlock(blockToDelete.address)
                            }

                            else -> {
                                throw ExodusException("Invalid reader type : $reader")
                            }
                        }
                    }
                } else {
                    if (loggablesProcessed < 3) {
                        logger.error(
                            "Data corruption was detected. Reason : ${exception.message} . Likely invalid cipher key/iv were used. "
                        )

                        blockSetMutable.clear()
                        throw InvalidCipherParametersException()
                    } else {
                        clearDataCorruptionLog(exception, blockSetMutable)
                        return -1
                    }
                }

                rwIsReadonly = false
                logger.info("Data corruption was fixed for environment $location.")

                if (loadedDbRootAddress != dbRootAddress) {
                    logger.warn(
                        "DB root address stored in log metadata and detected are different. " +
                                "Root address will be fixed. Stored address : $loadedDbRootAddress , detected  $dbRootAddress ."
                    )
                }

                return dbRootAddress
            } catch (e: InvalidCipherParametersException) {
                throw e
            } catch (e: DataCorruptionException) {
                throw e
            } catch (e: Exception) {
                logger.error("Error during attempt to restore log $location", e)
                throw ExodusException(exception)
            }
        }

        if (loadedDbRootAddress != dbRootAddress) {
            logger.warn(
                "DB root address stored in log metadata and detected are different. " +
                        "Root address will be fixed. Stored address : $loadedDbRootAddress , detected  $dbRootAddress ."
            )
            return dbRootAddress
        }

        return Long.MIN_VALUE
    }

    private fun extractRestoreInformation(
        fileBlockIterator: MutableIterator<Block>,
        blockSetMutable: BlockSet.Mutable
    ): Triple<Long, Long, Int> {
        var loggablesProcessed = 0
        var dbRootAddress: Long = Long.MIN_VALUE
        var dbRootEndAddress: Long = Long.MIN_VALUE

        try {
            var nextBlockCorruptionMessage: String? = null
            var corruptedFileAddress: Long = 0
            var hasNext: Boolean
            do {
                if (nextBlockCorruptionMessage != null) {
                    DataCorruptionException.raise(
                        nextBlockCorruptionMessage,
                        this,
                        corruptedFileAddress
                    )
                }

                val block = fileBlockIterator.next()
                val startBlockAddress = block.address
                val endBlockAddress = startBlockAddress + fileLengthBound

                logger.info("File ${LogUtil.getLogFilename(startBlockAddress)} is being verified.")

                val blockLength = block.length()
                // if it is not the last file and its size is not as expected
                hasNext = fileBlockIterator.hasNext()

                if (blockLength > fileLengthBound || hasNext && blockLength != fileLengthBound || blockLength == 0L) {
                    nextBlockCorruptionMessage = "Unexpected file length. " +
                            "Expected length : $fileLengthBound, actual file length : $blockLength ."
                    corruptedFileAddress = startBlockAddress

                    if (blockLength == 0L) {
                        continue
                    }
                }

                // if the file address is not a multiple of fileLengthBound
                if (startBlockAddress != getFileAddress(startBlockAddress)) {
                    DataCorruptionException.raise(
                        "Unexpected file address. Expected ${getFileAddress(startBlockAddress)}, actual $startBlockAddress.",
                        this,
                        startBlockAddress
                    )
                }

                val blockDataIterator = BlockDataIterator(
                    this, block, startBlockAddress,
                    formatWithHashCodeIsUsed
                )
                while (blockDataIterator.hasNext()) {
                    val loggableAddress = blockDataIterator.address

                    if (loggableAddress >= endBlockAddress) {
                        break
                    }

                    val loggableType = blockDataIterator.next() xor 0x80.toByte()
                    if (loggableType < 0 && config.isSkipInvalidLoggableType) {
                        continue
                    }

                    checkLoggableType(loggableType, loggableAddress)

                    if (NullLoggable.isNullLoggable(loggableType)) {
                        loggablesProcessed++
                        continue
                    }

                    if (HashCodeLoggable.isHashCodeLoggable(loggableType)) {
                        for (i in 0 until Long.SIZE_BYTES) {
                            blockDataIterator.next()
                        }
                        loggablesProcessed++
                        continue
                    }

                    val structureId = CompressedUnsignedLongByteIterable.getInt(blockDataIterator)
                    checkStructureId(structureId, loggableAddress)

                    val dataLength = CompressedUnsignedLongByteIterable.getInt(blockDataIterator)
                    checkDataLength(dataLength, loggableAddress)

                    if (blockDataIterator.address >= endBlockAddress) {
                        break
                    }

                    if (loggableType == DatabaseRoot.DATABASE_ROOT_TYPE) {
                        if (structureId != Loggable.NO_STRUCTURE_ID) {
                            DataCorruptionException.raise(
                                "Invalid structure id ($structureId) for root loggable.",
                                this, loggableAddress
                            )
                        }

                        val loggableData = ByteArray(dataLength)
                        val dataAddress = blockDataIterator.address

                        for (i in 0 until dataLength) {
                            loggableData[i] = blockDataIterator.next()
                        }

                        val rootLoggable = SinglePageLoggable(
                            loggableAddress,
                            blockDataIterator.address,
                            loggableType,
                            structureId,
                            dataAddress,
                            loggableData, 0, dataLength
                        )

                        val dbRoot = DatabaseRoot(rootLoggable)
                        if (dbRoot.isValid) {
                            dbRootAddress = loggableAddress
                            dbRootEndAddress = blockDataIterator.address
                        } else {
                            DataCorruptionException.raise(
                                "Corrupted database root was found", this,
                                loggableAddress
                            )
                        }
                    } else {
                        for (i in 0 until dataLength) {
                            blockDataIterator.next()
                        }
                    }

                    loggablesProcessed++
                }

                blockSetMutable.add(startBlockAddress, block)
                if (!hasNext && nextBlockCorruptionMessage != null) {
                    DataCorruptionException.raise(
                        nextBlockCorruptionMessage,
                        this,
                        corruptedFileAddress
                    )
                }
            } while (hasNext)
            return Triple(dbRootAddress, dbRootEndAddress, loggablesProcessed)
        } catch (e: Exception) {
            logger.error("Error during verification of database $location", e)
            throw LogCorruptionException(dbRootAddress, dbRootEndAddress, loggablesProcessed, e)
        }

    }

    private fun clearDataCorruptionLog(exception: Exception, blockSetMutable: BlockSet.Mutable) {
        logger.error(
            "Data corruption was detected. Reason : ${exception.message} . " +
                    "Environment $location will be cleared."
        )

        blockSetMutable.clear()
        dataWriter.clear()

        rwIsReadonly = false
    }

    private fun truncateFile(
        endBlock: File,
        position: Long,
        lastPage: ByteArray?
    ) {
        val endBlockBackupPath =
            Path.of(location).resolve(
                endBlock.name.substring(
                    0,
                    endBlock.name.length - LogUtil.LOG_FILE_EXTENSION_LENGTH
                ) +
                        LogUtil.TMP_TRUNCATION_FILE_EXTENSION
            )


        Files.copy(
            Path.of(endBlock.toURI()),
            endBlockBackupPath,
            StandardCopyOption.REPLACE_EXISTING
        )

        RandomAccessFile(endBlockBackupPath.toFile(), "rw").use {
            if (lastPage != null) {
                it.seek(position)
                it.write(lastPage)
                it.setLength(position + cachePageSize)
            } else {
                it.setLength(position)
            }

            it.fd.sync()
        }

        try {
            Files.move(
                endBlockBackupPath, Path.of(endBlock.toURI()),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
            )
        } catch (moveNotSupported: AtomicMoveNotSupportedException) {
            logger.warn(
                "Environment: $location. " +
                        "Atomic move is not supported by the file system and can not be used during " +
                        "log restore. Falling back to the non-atomic move."
            )
            Files.move(
                endBlockBackupPath, Path.of(endBlock.toURI()),
                StandardCopyOption.REPLACE_EXISTING
            )
        }

        RandomAccessFile(endBlock, "rw").use {
            it.fd.sync()
        }

    }

    private fun checkDataLength(dataLength: Int, loggableAddress: Long) {
        if (dataLength < 0) {
            DataCorruptionException.raise(
                "Loggable with negative length was encountered",
                this, loggableAddress
            )
        }

        if (dataLength > fileLengthBound) {
            DataCorruptionException.raise(
                "Loggable with length bigger than allowed value was discovered.",
                this, loggableAddress
            )
        }
    }

    private fun checkStructureId(structureId: Int, loggableAddress: Long) {
        if (structureId < 0) {
            DataCorruptionException.raise(
                "Loggable with negative structure id was encountered",
                this, loggableAddress
            )
        }
    }

    private fun checkLoggableType(loggableType: Byte, loggableAddress: Long) {
        if (loggableType < 0) {
            DataCorruptionException.raise("Loggable with negative type", this, loggableAddress)
        }
    }


    fun beginWrite(): Long {
        writeThread = Thread.currentThread()
        return writer.beginWrite()
    }

    fun endWrite(): Long {
        writeThread = null
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

        val adjustedPageSize = cachePageSize - BufferedDataWriter.HASH_CODE_SIZE
        val writtenSincePageStart = writtenInPage + offset
        val fullPages = writtenSincePageStart / adjustedPageSize

        return pageAddress + writtenSincePageStart + fullPages * BufferedDataWriter.HASH_CODE_SIZE
    }


    fun hasAddress(address: Long): Boolean {
        val fileAddress = getFileAddress(address)
        val files = writer.getFilesFrom(fileAddress)
        val highAddress = highReadAddress

        if (!files.hasNext()) {
            return false
        }
        val leftBound = files.nextLong()
        return leftBound == fileAddress && leftBound + getFileSize(leftBound, highAddress) > address
    }

    fun hasAddressRange(from: Long, to: Long): Boolean {
        var fileAddress = getFileAddress(from)
        val files = writer.getFilesFrom(fileAddress)
        val highAddress = highReadAddress

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
        return cache.getPage(this, pageAddress, -1)
    }

    fun getPageIterable(pageAddress: Long): ArrayByteIterable {
        return cache.getPageIterable(this, pageAddress, formatWithHashCodeIsUsed)
    }

    override fun getIdentity(): Int {
        return identity
    }

    override fun readPage(pageAddress: Long, fileAddress: Long): ByteArray {
        return writer.readPage(pageAddress)
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
        } else if (HashCodeLoggable.isHashCodeLoggable(type)) {
            HashCodeLoggable(address, it.offset, it.currentPage)
        } else {
            read(type, it, address)
        }
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
        checkLoggableType(type, address)

        val structureId = CompressedUnsignedLongByteIterable.getInt(it)
        checkStructureId(structureId, address)

        val dataLength = CompressedUnsignedLongByteIterable.getInt(it)
        checkDataLength(dataLength, address)

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
        return LoggableIterator(this, startAddress, highReadAddress)
    }

    fun tryWrite(
        type: Byte,
        structureId: Int,
        data: ByteIterable,
        expiredLoggables: ExpiredLoggableCollection
    ): Long {
        // allow new file creation only if new file starts loggable
        val result = writeContinuously(type, structureId, data, expiredLoggables)
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls(expiredLoggables)
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
    fun write(loggable: Loggable, expiredLoggables: ExpiredLoggableCollection): Long {
        return write(loggable.type, loggable.structureId, loggable.data, expiredLoggables)
    }

    fun write(
        type: Byte,
        structureId: Int,
        data: ByteIterable,
        expiredLoggables: ExpiredLoggableCollection
    ): Long {
        // allow new file creation only if new file starts loggable
        var result = writeContinuously(type, structureId, data, expiredLoggables)
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls(expiredLoggables)
            result = writeContinuously(type, structureId, data, expiredLoggables)
            if (result < 0) {
                throw TooBigLoggableException()
            }
        }
        return result
    }

    fun isImmutableFile(fileAddress: Long): Boolean {
        return fileAddress + fileLengthBound <= writer.highAddress
    }

    fun flush() {
        if (config.isDurableWrite) {
            sync()
        } else {
            writer.flush()
        }
    }

    fun sync() {
        writer.sync()
        writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
    }

    override fun close() {
        isClosing = true

        if (!rwIsReadonly) {
            val highAddress = writer.highAddress
            beginWrite()
            try {
                if (formatWithHashCodeIsUsed && (highAddress.toInt() and (cachePageSize - 1)) != 0) {
                    //we pad page with nulls to ensure that all pages could be checked on consistency
                    //by hash code which is stored at the end of the page.
                    val written = writer.padPageWithNulls()
                    if (written == 0) {
                        throw ExodusException("Invalid value of tip of the log $highAddress")
                    }

                    writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)

                }

                sync()
            } finally {
                endWrite()
            }

            if (reader is FileDataReader) {
                startupMetadata.closeAndUpdate(reader)
            }
        }

        writer.close(!rwIsReadonly)
        reader.close()

        if (cache is SeparateLogCache) {
            cache.clear()
        }

        val backupLocation = Path.of(location).resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME)
        Files.deleteIfExists(backupLocation)

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

    @Suppress("DeprecatedCallableAddReplaceWith")
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

    fun doPadWithNulls(expiredLoggables: ExpiredLoggableCollection) {
        val address = writer.currentHighAddress
        val written = writer.padWithNulls(fileLengthBound, nullPage)

        if (written > 0) {
            expiredLoggables.add(address, written)
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly)
        }
    }

    fun readBytes(output: ByteArray, pageAddress: Long): Int {
        val fileAddress = getFileAddress(pageAddress)

        val writerThread = Thread.currentThread() == writeThread

        val files = if (writerThread) {
            writer.getWriterFiles(fileAddress)
        } else {
            writer.getFilesFrom(fileAddress)
        }

        val highAddress = highReadAddress
        if (files.hasNext()) {
            val leftBound = files.nextLong()
            val fileSize = getFileSize(leftBound, highAddress)

            if (leftBound == fileAddress && fileAddress + fileSize > pageAddress) {
                val block = if (writerThread) {
                    writer.getWriterBlock(fileAddress)
                } else {
                    writer.getBlock(fileAddress)
                }

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

            val maxFile = if (writerThread) {
                writer.maximumFile
            } else {
                writer.maximumWritingFile
            }

            if (fileAddress >= (maxFile ?: -1L)) {
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

        val lastPage = (highAddress and
                ((cachePageSize - 1).inv()).toLong())
        var checkConsistency = config.isCheckPagesAtRuntime &&
                formatWithHashCodeIsUsed &&
                (!rwIsReadonly || pageAddress < lastPage)
        checkConsistency = formatWithHashCodeIsUsed &&
                (checkConsistency || readBytes == cachePageSize || readBytes == 0)

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
                ).append(lockTimeout).append(" ms.\n\n Lock owner info: \n")
                    .append(dataWriter.lockInfo())
                if (dataWriter is AsyncFileDataWriter) {
                    exceptionMessage.append("\n Lock file path: ").append(dataWriter.lockFilePath())
                }
                throw ExodusException(exceptionMessage.toString())
            }
        }
    }

    private fun getHighPageAddress(highAddress: Long): Long {
        var alignment = highAddress.toInt() and cachePageSize - 1
        if (alignment == 0 && highAddress > 0) {
            alignment = cachePageSize
        }
        return highAddress - alignment // aligned address
    }

    fun writeContinuously(
        type: Byte, structureId: Int, data: ByteIterable,
        expiredLoggables: ExpiredLoggableCollection
    ): Long {
        if (rwIsReadonly) {
            throw ExodusException("Environment is working in read-only mode. No writes are allowed")
        }

        var result = beforeWrite()

        val isNull = NullLoggable.isNullLoggable(type)
        var recordLength = 1

        if (isNull) {
            writer.write(type xor 0x80.toByte())
        } else {
            val structureIdIterable =
                CompressedUnsignedLongByteIterable.getIterable(structureId.toLong())
            val dataLength = data.length
            val dataLengthIterable =
                CompressedUnsignedLongByteIterable.getIterable(dataLength.toLong())
            recordLength += structureIdIterable.length
            recordLength += dataLengthIterable.length
            recordLength += dataLength

            val leftInPage =
                cachePageSize - (result.toInt() and (cachePageSize - 1)) - BufferedDataWriter.HASH_CODE_SIZE
            val delta =
                if (leftInPage in 1 until recordLength && recordLength < (cachePageSize shr 4)) {
                    leftInPage + BufferedDataWriter.HASH_CODE_SIZE
                } else {
                    0
                }

            if (!writer.fitsIntoSingleFile(fileLengthBound, recordLength + delta)) {
                return -1L
            }

            if (delta > 0) {
                val gapAddress = writer.currentHighAddress
                val written = writer.padPageWithNulls()

                assert(written == delta)
                result += written
                expiredLoggables.add(gapAddress, written)

                assert(result % cachePageSize.toLong() == 0L)
            }

            writer.write(type xor 0x80.toByte())

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


        val identityGenerator = IdGenerator()

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
                    writer.write(bytes[0])
                } else {
                    writer.write(bytes, offset, length)
                }
            } else if (length >= 3) {
                val bytes = iterable.baseBytes
                val offset = iterable.baseOffset()

                writer.write(bytes, offset, length)
            } else {
                val iterator = iterable.iterator()
                writer.write(iterator.next())
                if (length == 2) {
                    writer.write(iterator.next())
                }
            }
        }
    }
}

private class LogCorruptionException(
    val dbRootAddress: Long,
    val dbRootEndAddress: Long,
    val loggablesProcessed: Int, override val cause: Throwable?
) :
    Exception("Data corruption was detected. ", cause) {
}
