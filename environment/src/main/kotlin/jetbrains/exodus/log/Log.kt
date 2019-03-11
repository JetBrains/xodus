/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.log


import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.core.dataStructures.hash.LongIterator
import jetbrains.exodus.crypto.cryptBlocksMutable
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.IdGenerator
import mu.KLogging
import java.io.Closeable
import java.io.File
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.experimental.xor

class Log(val config: LogConfig) : Closeable {

    val created = System.currentTimeMillis()

    @JvmField
    internal val cache: LogCache

    @Volatile
    var isClosing: Boolean = false
        private set

    var identity: Int = 0
        private set

    private val reader: DataReader = config.reader
    private val writer: DataWriter = config.writer

    private val internalTip: AtomicReference<LogTip>

    val tip: LogTip get() = internalTip.get()

    private var bufferedWriter: BufferedDataWriter? = null

    /** Last ticks when the sync operation was performed. */
    private var lastSyncTicks: Long = 0

    private val blockListeners = ArrayList<BlockListener>(2)
    private val readBytesListeners = ArrayList<ReadBytesListener>(2)

    /** Size of single page in log cache. */
    val cachePageSize = config.cachePageSize

    /** Size of a single file of the log in bytes.
     * @return size of a single log file in bytes.
     */
    val fileLengthBound: Long

    @Deprecated("for tests only")
    private var testConfig: LogTestConfig? = null

    val location: String
        get() = reader.location

    val numberOfFiles: Long
        get() = tip.blockSet.size().toLong()

    /**
     * Returns addresses of log files from the newest to the oldest ones.
     *
     * @return array of file addresses.
     */
    val allFileAddresses: LongArray
        get() = tip.blockSet.array

    val highAddress: Long
        get() = tip.highAddress

    val writtenHighAddress: Long
        get() = ensureWriter().highAddress

    val lowAddress: Long
        get() {
            val result = tip.blockSet.minimum
            return result ?: Loggable.NULL_ADDRESS
        }

    val highFileAddress: Long
        get() = getFileAddress(highAddress)

    val diskUsage: Long
        get() {
            val allFiles = tip.allFiles
            val filesCount = allFiles.size
            return if (filesCount == 0) 0L else (filesCount - 1) * fileLengthBound + getLastFileSize(allFiles[filesCount - 1], tip)
        }

    val cacheHitRate: Float
        get() = cache.hitRate()


    init {
        tryLock()
        val fileLength = config.fileSize * 1024L
        if (fileLength % cachePageSize != 0L) {
            throw InvalidSettingException("File size should be a multiple of cache page size.")
        }
        fileLengthBound = fileLength
        val blockSetMutable = BlockSet.Immutable(fileLength).beginWrite()
        if (reader is FileDataReader) {
            reader.setLog(this)
        }

        checkLogConsistency(blockSetMutable)

        val blockSetImmutable = blockSetMutable.endWrite()

        val memoryUsage = config.memoryUsage
        val nonBlockingCache = config.isNonBlockingCache
        val generationCount = config.cacheGenerationCount

        cache = if (memoryUsage != 0L) {
            if (config.isSharedCache)
                getSharedCache(memoryUsage, cachePageSize, nonBlockingCache, generationCount)
            else
                SeparateLogCache(memoryUsage, cachePageSize, nonBlockingCache, generationCount)
        } else {
            val memoryUsagePercentage = config.memoryUsagePercentage
            if (config.isSharedCache)
                getSharedCache(memoryUsagePercentage, cachePageSize, nonBlockingCache, generationCount)
            else
                SeparateLogCache(memoryUsagePercentage, cachePageSize, nonBlockingCache, generationCount)
        }
        DeferredIO.getJobProcessor()
        isClosing = false

        val lastFileAddress = blockSetMutable.maximum
        updateLogIdentity()
        if (lastFileAddress == null) {
            internalTip = AtomicReference(LogTip(fileLengthBound))
        } else {
            val currentHighAddress = lastFileAddress + blockSetMutable.getBlock(lastFileAddress).length()
            val highPageAddress = getHighPageAddress(currentHighAddress)
            val highPageContent = ByteArray(cachePageSize)
            val tmpTip = LogTip(highPageContent, highPageAddress, cachePageSize, currentHighAddress, currentHighAddress, blockSetImmutable)
            this.internalTip = AtomicReference(tmpTip) // TODO: this is a hack to provide readBytes below with high address (for determining last file length)
            val highPageSize = if (currentHighAddress == 0L) 0 else readBytes(highPageContent, highPageAddress)
            val proposedTip = LogTip(highPageContent, highPageAddress, highPageSize, currentHighAddress, currentHighAddress, blockSetImmutable)
            this.internalTip.set(proposedTip)
            // here we should check whether last loggable is written correctly
            val lastFileLoggables = LoggableIterator(this, lastFileAddress)
            var approvedHighAddress: Long = lastFileAddress
            try {
                while (lastFileLoggables.hasNext()) {
                    val loggable = lastFileLoggables.next()
                    val dataLength = if (NullLoggable.isNullLoggable(loggable)) 0 else loggable.dataLength
                    if (dataLength > 0) {
                        // if not null loggable read all data to the end
                        val data = loggable.data.iterator()
                        for (i in 0 until dataLength) {
                            if (!data.hasNext()) {
                                throw ExodusException("Can't read loggable fully" + LogUtil.getWrongAddressErrorMessage(data.address, fileLengthBound))
                            }
                            data.next()
                        }
                    } else if (dataLength < 0) {
                        // XD-728:
                        // this is either data corruption or encrypted database
                        // anyway recovery should stop at this point
                        break
                    }
                    val approvedHighAddressCandidate = loggable.address + loggable.length()
                    if (approvedHighAddressCandidate > currentHighAddress) {
                        // XD-728:
                        // this is either data corruption or encrypted database
                        // anyway recovery should stop at this point
                        break
                    }
                    approvedHighAddress = approvedHighAddressCandidate
                }
            } catch (e: ExodusException) { // if an exception is thrown then last loggable wasn't read correctly
                logger.error("Exception on Log recovery. Approved high address = $approvedHighAddress", e)
            }

            this.internalTip.set(proposedTip.withApprovedAddress(approvedHighAddress))
        }
        sync()
    }

    private fun checkLogConsistency(blockSetMutable: BlockSet.Mutable) {
        val blockIterator = reader.blocks.iterator()
        if (!blockIterator.hasNext()) {
            return
        }
        if (config.isCleanDirectoryExpected) {
            throw ExodusException("Clean log is expected")
        }
        var hasNext: Boolean
        do {
            val block = blockIterator.next()
            val address = block.address
            val blockLength = block.length()
            var clearLogReason: String? = null
            // if it is not the last file and its size is not as expected
            hasNext = blockIterator.hasNext()
            if (blockLength > fileLengthBound || hasNext && blockLength != fileLengthBound) {
                clearLogReason = "Unexpected file length" + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound)
            }
            // if the file address is not a multiple of fileLengthBound
            if (clearLogReason == null && address != getFileAddress(address)) {
                if (!config.isClearInvalidLog) {
                    throw ExodusException("Unexpected file address " +
                            LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound))
                }
                clearLogReason = "Unexpected file address " +
                        LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound)
            }
            if (clearLogReason != null) {
                if (!config.isClearInvalidLog) {
                    throw ExodusException(clearLogReason)
                }
                logger.error("Clearing log due to: $clearLogReason")
                blockSetMutable.clear()
                writer.clear()
                break
            }
            blockSetMutable.add(address, block)
        } while (hasNext)
    }


    fun setHighAddress(logTip: LogTip, highAddress: Long): LogTip {
        return setHighAddress(logTip, highAddress, logTip.blockSet.beginWrite())
    }

    fun setHighAddress(logTip: LogTip, highAddress: Long, blockSet: BlockSet.Mutable): LogTip {
        if (highAddress > logTip.highAddress) {
            throw ExodusException("Only can decrease high address")
        }
        if (highAddress == logTip.highAddress) {
            if (bufferedWriter != null) {
                throw IllegalStateException("Unexpected write in progress")
            }
            return logTip
        }

        // begin of test-only code
        val testConfig = this.testConfig
        if (testConfig != null && testConfig.isSettingHighAddressDenied) {
            throw ExodusException("Setting high address is denied")
        }
        // end of test-only code

        // at first, remove all files which are higher than highAddress
        closeWriter()
        val blocksToDelete = LongArrayList()
        var blockToTruncate = -1L
        for (blockAddress in blockSet.array) {
            if (blockAddress <= highAddress) {
                blockToTruncate = blockAddress
                break
            }
            blocksToDelete.add(blockAddress)
        }

        // truncate log
        for (i in 0 until blocksToDelete.size()) {
            removeFile(blocksToDelete.get(i), RemoveBlockType.Delete, blockSet)
        }
        if (blockToTruncate >= 0) {
            truncateFile(blockToTruncate, highAddress - blockToTruncate)
        }

        val updatedTip: LogTip
        if (blockSet.isEmpty) {
            updateLogIdentity()
            updatedTip = LogTip(fileLengthBound)
        } else {
            val oldHighPageAddress = logTip.pageAddress
            var approvedHighAddress = logTip.approvedHighAddress
            if (highAddress < approvedHighAddress) {
                approvedHighAddress = highAddress
            }
            val highPageAddress = getHighPageAddress(highAddress)
            val blockSetImmutable = blockSet.endWrite()
            val highPageSize = (highAddress - highPageAddress).toInt()
            if (oldHighPageAddress == highPageAddress) {
                updatedTip = logTip.withResize(highPageSize, highAddress, approvedHighAddress, blockSetImmutable)
            } else {
                updateLogIdentity()
                val highPageContent = ByteArray(cachePageSize)
                if (highPageSize > 0 && readBytes(highPageContent, highPageAddress) < highPageSize) {
                    throw ExodusException("Can't read expected high page bytes")
                }
                updatedTip = LogTip(highPageContent, highPageAddress, highPageSize, highAddress, approvedHighAddress, blockSetImmutable)
            }
        }
        compareAndSetTip(logTip, updatedTip)
        this.bufferedWriter = null
        return updatedTip
    }

    private fun closeWriter() {
        if (bufferedWriter != null) {
            throw IllegalStateException("Unexpected write in progress")
        }
        writer.close()
    }

    fun beginWrite(): LogTip {
        val writer = BufferedDataWriter(this, this.writer, tip)
        this.bufferedWriter = writer
        return writer.startingTip
    }

    fun abortWrite() { // any log rollbacks must be done via setHighAddress only
        this.bufferedWriter = null
    }

    fun revertWrite(logTip: LogTip) {
        val writer = ensureWriter()
        val blockSet = writer.blockSetMutable
        abortWrite()
        setHighAddress(compareAndSetTip(logTip, writer.updatedTip), logTip.highAddress, blockSet)
    }

    fun endWrite(): LogTip {
        val writer = ensureWriter()
        val logTip = writer.startingTip
        val updatedTip = writer.updatedTip
        compareAndSetTip(logTip, updatedTip)
        bufferedWriter = null
        return updatedTip
    }

    fun compareAndSetTip(logTip: LogTip, updatedTip: LogTip): LogTip {
        if (!internalTip.compareAndSet(logTip, updatedTip)) {
            throw ExodusException("write start/finish mismatch")
        }
        return updatedTip
    }

    fun getFileAddress(address: Long): Long {
        return address - address % fileLengthBound
    }

    fun getNextFileAddress(fileAddress: Long): Long {
        val files = tip.blockSet.getFilesFrom(fileAddress)
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

    fun isLastFileAddress(address: Long, logTip: LogTip): Boolean {
        return getFileAddress(address) == getFileAddress(logTip.highAddress)
    }

    fun isLastWrittenFileAddress(address: Long): Boolean {
        return getFileAddress(address) == getFileAddress(writtenHighAddress)
    }

    fun hasAddress(address: Long): Boolean {
        val fileAddress = getFileAddress(address)
        val logTip = tip
        val files = logTip.blockSet.getFilesFrom(fileAddress)
        if (!files.hasNext()) {
            return false
        }
        val leftBound = files.nextLong()
        return leftBound == fileAddress && leftBound + getFileSize(leftBound, logTip) > address
    }

    fun hasAddressRange(from: Long, to: Long): Boolean {
        var fileAddress = getFileAddress(from)
        val logTip = tip
        val files = logTip.blockSet.getFilesFrom(fileAddress)
        do {
            if (!files.hasNext() || files.nextLong() != fileAddress) {
                return false
            }
            fileAddress += getFileSize(fileAddress, logTip)
        } while (fileAddress in (from + 1)..to)
        return true
    }

    @JvmOverloads
    fun getFileSize(fileAddress: Long, logTip: LogTip = tip): Long {
        // readonly files (not last ones) all have the same size
        return if (!isLastFileAddress(fileAddress, logTip)) {
            fileLengthBound
        } else getLastFileSize(fileAddress, logTip)
    }

    private fun getLastFileSize(fileAddress: Long, logTip: LogTip): Long {
        val highAddress = logTip.highAddress
        val result = highAddress % fileLengthBound
        return if (result == 0L && highAddress != fileAddress) {
            fileLengthBound
        } else result
    }

    fun getHighPage(alignedAddress: Long): ByteArray? {
        val tip = tip
        return if (tip.pageAddress == alignedAddress && tip.count >= 0) {
            tip.bytes
        } else null
    }

    fun getCachedPage(pageAddress: Long): ByteArray {
        return cache.getPage(this, pageAddress)
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
        return ensureWriter().getByte(address, max)
    }

    @JvmOverloads
    fun read(it: ByteIteratorWithAddress, address: Long = it.address): RandomAccessLoggable {
        val type = it.next() xor 0x80.toByte()
        return if (NullLoggable.isNullLoggable(type)) {
            NullLoggable(address)
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
            return RandomAccessLoggableAndArrayByteIterable(
                    address, type, structureId, dataAddress, it.currentPage, it.offset, dataLength)
        }
        val data = RandomAccessByteIterable(dataAddress, this)
        return RandomAccessLoggableImpl(address, type, data, dataLength, structureId)
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
        val logTip = tip
        val files = logTip.blockSet.getFilesFrom(0)
        val approvedHighAddress = logTip.approvedHighAddress
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
                if (loggable.address + loggable.length() == approvedHighAddress) {
                    break
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
        val logTip = tip
        val approvedHighAddress = logTip.approvedHighAddress
        for (fileAddress in logTip.blockSet.array) {
            if (result != null) {
                break
            }
            val it = getLoggableIterator(fileAddress)
            while (it.hasNext()) {
                val loggable = it.next()
                if (loggable == null || loggable.address >= fileAddress + fileLengthBound) {
                    break
                }
                if (loggable.type.toInt() == type) {
                    result = loggable
                }
                if (loggable.address + loggable.length() == approvedHighAddress) {
                    break
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
    fun getLastLoggableOfTypeBefore(type: Int, beforeAddress: Long, logTip: LogTip): Loggable? {
        var result: Loggable? = null
        for (fileAddress in logTip.blockSet.array) {
            if (result != null) {
                break
            }
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
        return fileAddress + fileLengthBound <= tip.approvedHighAddress
    }

    @JvmOverloads
    fun flush(forceSync: Boolean = false) {
        ensureWriter().flush()
        if (forceSync || config.isDurableWrite) {
            sync()
        }
    }

    fun sync() {
        writer.sync()
        lastSyncTicks = System.currentTimeMillis()
    }

    override fun close() {
        val logTip = tip
        isClosing = true
        sync()
        reader.close()
        closeWriter()
        compareAndSetTip(logTip, LogTip(fileLengthBound, logTip.pageAddress, logTip.highAddress))
        release()
    }

    fun release() {
        writer.release()
    }

    fun clear(): LogTip {
        val logTip = tip
        cache.clear()
        reader.close()
        closeWriter()
        writer.clear()
        val updatedTip = LogTip(fileLengthBound)
        compareAndSetTip(logTip, updatedTip)
        this.bufferedWriter = null
        updateLogIdentity()
        return updatedTip
    }

    // for tests only
    @Deprecated("for tests only")
    fun forgetFile(address: Long) {
        beginWrite()
        forgetFiles(longArrayOf(address))
        endWrite()
    }

    fun forgetFiles(files: LongArray) {
        val blockSetMutable = ensureWriter().blockSetMutable
        for (file in files) {
            blockSetMutable.remove(file)
        }
    }

    @JvmOverloads
    fun removeFile(address: Long, rbt: RemoveBlockType = RemoveBlockType.Delete, blockSetMutable: BlockSet.Mutable? = null) {
        val block = tip.blockSet.getBlock(address)
        val listeners = blockListeners.notifyListeners { it.beforeBlockDeleted(block, reader, writer) }
        try {
            writer.removeBlock(address, rbt)
            // remove address of file of the list
            blockSetMutable?.remove(address)
            // clear cache
            clearFileFromLogCache(address)
        } finally {
            listeners.forEach { it.afterBlockDeleted(address, reader, writer) }
        }
    }

    fun ensureWriter(): BufferedDataWriter {
        return bufferedWriter ?: throw ExodusException("write not in progress")
    }

    private fun truncateFile(address: Long, length: Long) {
        writer.truncateBlock(address, length)
        writer.openOrCreateBlock(address, length)
        // clear cache
        clearFileFromLogCache(address, length - length % cachePageSize)
    }

    fun clearFileFromLogCache(address: Long, offset: Long = 0L) {
        var off = offset
        while (off < fileLengthBound) {
            cache.removePage(this, address + off)
            off += cachePageSize
        }
    }

    /**
     * Pad current file with null loggables. Null loggable takes only one byte in the log,
     * so each file of the log with arbitrary alignment can be padded with nulls.
     * Padding with nulls is automatically performed when a loggable to be written can't be
     * placed within the appendable file without overcome of the value of fileLengthBound.
     * This feature allows to guarantee that each file starts with a new loggable, no
     * loggable can begin in one file and end in another. Also, this simplifies reading algorithm:
     * if we started reading by address it definitely should finish within current file.
     */
    fun padWithNulls() {
        beforeWrite(ensureWriter())
        doPadWithNulls()
    }

    fun doPadWithNulls() {
        val writer = ensureWriter()
        var bytesToWrite = fileLengthBound - writer.getLastWrittenFileLength(fileLengthBound)
        if (bytesToWrite == 0L) {
            throw ExodusException("Nothing to pad")
        }
        if (bytesToWrite >= cachePageSize) {
            val cachedTailPage = LogCache.getCachedTailPage(cachePageSize)
            if (cachedTailPage != null) {
                do {
                    writer.write(cachedTailPage, cachePageSize)
                    bytesToWrite -= cachePageSize.toLong()
                    writer.incHighAddress(cachePageSize.toLong())
                } while (bytesToWrite >= cachePageSize)
            }
        }
        if (bytesToWrite == 0L) {
            writer.commit()
            closeFullFileIfNecessary(writer)
        } else {
            while (bytesToWrite-- > 0) {
                writeContinuously(NullLoggable.create())
            }
        }
    }

    fun readBytes(output: ByteArray, address: Long): Int {
        val fileAddress = getFileAddress(address)
        val logTip = tip
        val files = logTip.blockSet.getFilesFrom(fileAddress)
        if (files.hasNext()) {
            val leftBound = files.nextLong()
            val fileSize = getFileSize(leftBound, logTip)
            if (leftBound == fileAddress && fileAddress + fileSize > address) {
                val block = logTip.blockSet.getBlock(fileAddress)
                val readBytes = block.read(output, address - fileAddress, 0, output.size)
                val cipherProvider = config.cipherProvider
                if (cipherProvider != null) {
                    cryptBlocksMutable(cipherProvider, config.cipherKey, config.cipherBasicIV,
                            address, output, 0, readBytes, LogUtil.LOG_BLOCK_ALIGNMENT)
                }
                notifyReadBytes(output, readBytes)
                return readBytes
            }
            if (fileAddress < (logTip.blockSet.minimum ?: -1L)) {
                BlockNotFoundException.raise("Address is out of log space, underflow", this, address)
            }
            if (fileAddress >= (logTip.blockSet.maximum ?: -1L)) {
                BlockNotFoundException.raise("Address is out of log space, overflow", this, address)
            }
        }
        BlockNotFoundException.raise(this, address)
        return 0
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
            if (!writer.lock(lockTimeout)) {
                throw ExodusException("Can't acquire environment lock after " +
                        lockTimeout + " ms.\n\n Lock owner info: \n" + writer.lockInfo())
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

    fun getFilesFrom(logTip: LogTip, fileAddress: Long?): LongIterator {
        return logTip.blockSet.getFilesFrom(fileAddress!!)
    }

    /**
     * Writes specified loggable continuously in a single file.
     *
     * @param loggable the loggable to write.
     * @return address where the loggable was placed or less than zero value if the loggable can't be
     * written continuously in current appendable file.
     */
    fun writeContinuously(loggable: Loggable): Long {
        return writeContinuously(loggable.type, loggable.structureId, loggable.data)
    }

    fun writeContinuously(type: Byte, structureId: Int, data: ByteIterable): Long {
        val writer = ensureWriter()

        val result = beforeWrite(writer)

        val isNull = NullLoggable.isNullLoggable(type)
        var recordLength = 1
        if (isNull) {
            writer.write(type xor 0x80.toByte())
        } else {
            val structureIdIterable = CompressedUnsignedLongByteIterable.getIterable(structureId.toLong())
            val dataLength = data.length
            val dataLengthIterable = CompressedUnsignedLongByteIterable.getIterable(dataLength.toLong())
            recordLength += structureIdIterable.length
            recordLength += dataLengthIterable.length
            recordLength += dataLength
            if (recordLength > fileLengthBound - writer.getLastWrittenFileLength(fileLengthBound)) {
                return -1L
            }
            writer.write(type xor 0x80.toByte())
            writeByteIterable(writer, structureIdIterable)
            writeByteIterable(writer, dataLengthIterable)
            if (dataLength > 0) {
                writeByteIterable(writer, data)
            }

        }
        writer.commit()
        writer.incHighAddress(recordLength.toLong())
        closeFullFileIfNecessary(writer)
        return result
    }

    fun writeContinuously(data: ByteArray, count: Int): Long {
        val writer = ensureWriter()

        val result = beforeWrite(writer)

        if (count > fileLengthBound - writer.getLastWrittenFileLength(fileLengthBound)) {
            return -1L // protect file overflow
        }
        with(writer) {
            write(data, count)
            commit()
            incHighAddress(count.toLong())
            closeFullFileIfNecessary(this)
        }
        return result
    }

    private fun beforeWrite(writer: BufferedDataWriter): Long {
        val result = writer.highAddress

        // begin of test-only code
        val testConfig = this.testConfig
        if (testConfig != null) {
            val maxHighAddress = testConfig.maxHighAddress
            if (maxHighAddress in 0..result) {
                throw ExodusException("Can't write more than $maxHighAddress")
            }
        }
        // end of test-only code

        if (!this.writer.isOpen) {
            val fileAddress = getFileAddress(result)
            val block = writer.openOrCreateBlock(fileAddress, writer.getLastWrittenFileLength(fileLengthBound))
            val fileCreated = !writer.blockSetMutable.contains(fileAddress)
            if (fileCreated) {
                writer.blockSetMutable.add(fileAddress, block)
                // fsync the directory to ensure we will find the log file in the directory after system crash
                this.writer.syncDirectory()
                notifyBlockCreated(fileAddress)
            } else {
                notifyBlockModified(fileAddress)
            }
        }
        return result
    }

    private fun closeFullFileIfNecessary(writer: BufferedDataWriter) {
        val shouldCreateNewFile = writer.getLastWrittenFileLength(fileLengthBound) == 0L
        if (shouldCreateNewFile) {
            // Don't forget to fsync the old file before closing it, otherwise will get a corrupted DB in the case of a
            // system failure:
            flush(true)
            this.writer.close()
            // verify if last block is consistent
            val blockSet = writer.blockSetMutable
            val lastFile = blockSet.maximum
            if (lastFile != null) {
                val block = blockSet.getBlock(lastFile)
                val refreshed = block.refresh()
                if (block !== refreshed) {
                    blockSet.add(lastFile, refreshed)
                }
                val length = refreshed.length()
                if (length < fileLengthBound) {
                    throw IllegalStateException("File's too short (" + LogUtil.getLogFilename(lastFile) + "), block.length() = " + length + ", fileLengthBound = " + fileLengthBound)
                }
                if (config.isFullFileReadonly && block is File) {
                    block.setReadOnly()
                }
            }

        } else if (System.currentTimeMillis() > lastSyncTicks + config.syncPeriod) {
            flush(true)
        }
    }

    /**
     * Sets LogTestConfig.
     * Is destined for tests only, please don't set a not-null value in application code.
     */
    @Deprecated("for tests only")
    fun setLogTestConfig(testConfig: LogTestConfig?) {
        this.testConfig = testConfig
    }

    private fun notifyBlockCreated(address: Long) {
        val block = tip.blockSet.getBlock(address)
        blockListeners.notifyListeners { it.blockCreated(block, reader, writer) }
    }

    private fun notifyBlockModified(address: Long) {
        val block = tip.blockSet.getBlock(address)
        blockListeners.notifyListeners { it.blockModified(block, reader, writer) }
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
        private var sharedCache: LogCache? = null

        /**
         * For tests only!!!
         */
        @JvmStatic
        fun invalidateSharedCache() {
            synchronized(Log::class.java) {
                sharedCache = null
            }
        }

        private fun getSharedCache(memoryUsage: Long,
                                   pageSize: Int,
                                   nonBlocking: Boolean,
                                   cacheGenerationCount: Int): LogCache {
            var result = sharedCache
            if (result == null) {
                synchronized(Log::class.java) {
                    if (sharedCache == null) {
                        sharedCache = SharedLogCache(memoryUsage, pageSize, nonBlocking, cacheGenerationCount)
                    }
                    result = sharedCache
                }
            }
            checkCachePageSize(pageSize, result!!)
            return result!!
        }

        private fun getSharedCache(memoryUsagePercentage: Int,
                                   pageSize: Int,
                                   nonBlocking: Boolean,
                                   cacheGenerationCount: Int): LogCache {
            var result = sharedCache
            if (result == null) {
                synchronized(Log::class.java) {
                    if (sharedCache == null) {
                        sharedCache = SharedLogCache(memoryUsagePercentage, pageSize, nonBlocking, cacheGenerationCount)
                    }
                    result = sharedCache
                }
            }
            checkCachePageSize(pageSize, result!!)
            return result!!
        }

        private fun checkCachePageSize(pageSize: Int, result: LogCache) {
            if (result.pageSize != pageSize) {
                throw ExodusException("SharedLogCache was created with page size " + result.pageSize +
                        " and then requested with page size " + pageSize + ". EnvironmentConfig.LOG_CACHE_PAGE_SIZE was set manually.")
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
                val bytes = iterable.getBytesUnsafe()
                if (length == 1) {
                    writer.write(bytes[0])
                } else {
                    writer.write(bytes, length)
                }
            } else if (length >= 3) {
                writer.write(iterable.bytesUnsafe, length)
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
