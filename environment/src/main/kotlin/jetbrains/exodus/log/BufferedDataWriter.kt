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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.bindings.BindingUtils
import jetbrains.exodus.core.dataStructures.LongIntPair
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.LongIterator
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.crypto.cryptBlocksImmutable
import jetbrains.exodus.io.*
import mu.KLogging
import net.jpountz.xxhash.StreamingXXHash64
import net.jpountz.xxhash.XXHash64
import net.jpountz.xxhash.XXHashFactory
import org.jctools.maps.NonBlockingHashMapLong
import java.io.File
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiConsumer

class BufferedDataWriter internal constructor(
    private val log: Log,
    private val writer: DataWriter,
    private val calculateHashCode: Boolean,
    writeBoundarySemaphore: Semaphore,
    maxWriteBoundary: Int,
    files: BlockSet.Immutable,
    highAddress: Long,
    page: ByteArray,
    private val syncPeriod: Long
) {
    private val logCache: LogCache = log.cache
    private val cipherProvider: StreamCipherProvider? = log.config.getStreamCipherProvider()
    private val cipherKey: ByteArray? = log.config.getCipherKey()
    private val cipherBasicIV: Long = log.config.getCipherBasicIv()
    private val pageSize: Int = log.cachePageSize
    private val adjustedPageSize: Int = pageSize - HASH_CODE_SIZE
    private var blockSetMutable: BlockSet.Mutable? = null
    private var blockSetWasChanged = false
    private val writeBoundarySemaphore: Semaphore
    private val localWritesSemaphore: Semaphore
    private val writeCompletionHandler: BiConsumer<LongIntPair, in Throwable>
    private var currentPage: MutablePage? = null

    @JvmField
    var currentHighAddress: Long = 0

    @Volatile
    @JvmField
    var highAddress: Long = 0

    @Volatile
    private var blockSet: BlockSet.Immutable? = null

    @Volatile
    private var writeError: Throwable? = null

    @Volatile
    private var lastSyncedAddress: Long = 0

    @Volatile
    private var lastSyncedTs: Long
    private var sha256: MessageDigest? = null
    private val writeCache: NonBlockingHashMapLong<PageHolder?>

    init {
        writeCache = NonBlockingHashMapLong(maxWriteBoundary, false)
        this.writeBoundarySemaphore = writeBoundarySemaphore
        localWritesSemaphore = Semaphore(Int.MAX_VALUE)
        lastSyncedTs = System.currentTimeMillis()
        sha256 = if (cipherProvider != null) {
            try {
                MessageDigest.getInstance("SHA-256")
            } catch (e: NoSuchAlgorithmException) {
                throw ExodusException("SHA-256 hash function was not found", e)
            }
        } else {
            null
        }
        initCurrentPage(files, highAddress, page)
        writeCompletionHandler = BiConsumer { positionWrittenPair: LongIntPair, err: Throwable? ->
            val position = positionWrittenPair.first
            val written = positionWrittenPair.second
            val pageOffset = position and (pageSize - 1).toLong()
            val pageAddress = position - pageOffset
            val pageHolder = writeCache[pageAddress]!!
            val writtenInPage = pageHolder.written
            val result = writtenInPage.addAndGet(written)
            if (result == pageSize) {
                writeCache.remove(pageAddress)
            }
            if (err != null) {
                writeError = err
                logger.error(
                    "Error during writing of data to the file for the log " +
                            log.getLocation(),
                    err
                )
            }
            writeBoundarySemaphore.release()
            localWritesSemaphore.release()
        }
    }

    fun beforeWrite(): Long {
        checkWriteError()
        if (currentPage == null) {
            throw ExodusException("Current page is not initialized in the buffered writer.")
        }
        blockSetMutable = blockSet!!.beginWrite()
        blockSetWasChanged = false
        assert(currentHighAddress == highAddress)
        assert(currentHighAddress % pageSize == (currentPage!!.writtenCount % pageSize).toLong())
        return currentHighAddress
    }

    private fun initCurrentPage(
        files: BlockSet.Immutable,
        highAddress: Long,
        page: ByteArray
    ) {
        currentHighAddress = highAddress
        this.highAddress = highAddress
        lastSyncedAddress = 0
        if (pageSize != page.size) {
            throw InvalidSettingException(
                "Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + page.size
            )
        }
        val pageOffset = highAddress.toInt() and pageSize - 1
        val pageAddress = highAddress - pageOffset
        currentPage = MutablePage(page, pageAddress, pageOffset, pageOffset)
        val xxHash64 = currentPage!!.xxHash64
        if (calculateHashCode && pageOffset < pageSize) {
            if (cipherProvider != null) {
                val encryptedBytes = cryptBlocksImmutable(
                    cipherProvider, cipherKey!!,
                    cipherBasicIV, pageAddress, page, 0, pageOffset, LogUtil.LOG_BLOCK_ALIGNMENT
                )
                xxHash64.update(encryptedBytes, 0, encryptedBytes.size)
            } else {
                xxHash64.update(page, 0, pageOffset)
            }
        }
        blockSet = files
        val writtenCount = currentPage!!.writtenCount
        assert(writtenCount == currentPage!!.committedCount)
        if (writtenCount > 0) {
            val bytes = currentPage!!.bytes
            addPageToWriteCache(pageAddress, writtenCount, bytes)
        }
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
    }

    private fun addPageToWriteCache(pageAddress: Long, writtenCount: Int, bytes: ByteArray) {
        val holder = writeCache[pageAddress]
        if (holder == null) {
            writeCache.put(pageAddress, PageHolder(bytes, writtenCount))
        } else {
            holder.page = bytes
        }
    }

    fun endWrite(): Long {
        checkWriteError()
        assert(blockSetMutable != null)
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        flush()
        if (doNeedsToBeSynchronized(currentHighAddress)) {
            doSync(currentHighAddress)
        }
        if (blockSetWasChanged) {
            blockSet = blockSetMutable!!.endWrite()
            blockSetWasChanged = false
            blockSetMutable = null
        }
        assert(currentPage!!.committedCount <= currentPage!!.writtenCount)
        assert(
            currentPage!!.committedCount == pageSize ||
                    currentPage!!.pageAddress == currentHighAddress and (pageSize - 1).toLong().inv()
        )
        assert(currentPage!!.committedCount == 0 || currentPage!!.committedCount == pageSize || writeCache[currentPage!!.pageAddress] != null)
        assert(highAddress <= currentHighAddress)
        if (highAddress < currentHighAddress) {
            highAddress = currentHighAddress
        }
        return highAddress
    }

    fun needsToBeSynchronized(): Boolean {
        return doNeedsToBeSynchronized(highAddress)
    }

    private fun doNeedsToBeSynchronized(committedHighAddress: Long): Boolean {
        if (lastSyncedAddress < committedHighAddress) {
            val now = System.currentTimeMillis()
            return now - lastSyncedTs >= syncPeriod
        }
        return false
    }

    fun write(b: Byte) {
        checkWriteError()
        val currentPage = allocateNewPageIfNeeded()
        var delta = 1
        var writtenCount = currentPage!!.writtenCount
        assert(currentHighAddress == currentPage.pageAddress + currentPage.writtenCount)
        assert(writtenCount < adjustedPageSize)
        currentPage.bytes[writtenCount] = b
        writtenCount++
        currentPage.writtenCount = writtenCount
        if (writtenCount == adjustedPageSize) {
            currentPage.writtenCount = pageSize
            delta += HASH_CODE_SIZE
        }
        currentHighAddress += delta.toLong()
        assert(currentHighAddress == currentPage.pageAddress + currentPage.writtenCount)
        if (currentPage.writtenCount == pageSize) {
            writePage(currentPage)
        }
    }

    fun write(b: ByteArray, offset: Int, len: Int) {
        var resultLen = len
        checkWriteError()
        var off = 0
        var delta = resultLen
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        while (resultLen > 0) {
            val currentPage = allocateNewPageIfNeeded()
            val bytesToWrite = (adjustedPageSize - currentPage!!.writtenCount).coerceAtMost(resultLen)
            System.arraycopy(
                b, offset + off, currentPage.bytes,
                currentPage.writtenCount, bytesToWrite
            )
            currentPage.writtenCount += bytesToWrite
            if (currentPage.writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize
                delta += HASH_CODE_SIZE
                writePage(currentPage)
            }
            resultLen -= bytesToWrite
            off += bytesToWrite
        }
        currentHighAddress += delta.toLong()
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
    }

    fun fitsIntoSingleFile(fileLengthBound: Long, loggableSize: Int): Boolean {
        val currentHighAddress = currentHighAddress
        val fileReminder = fileLengthBound - (currentHighAddress and fileLengthBound - 1)
        val adjustLoggableSize =
            log.adjustLoggableAddress(currentHighAddress, loggableSize.toLong()) - currentHighAddress
        return adjustLoggableSize <= fileReminder
    }

    fun readPage(pageAddress: Long): ByteArray {
        if (currentPage != null && currentPage!!.pageAddress == pageAddress) {
            return currentPage!!.bytes
        }
        val holder = writeCache[pageAddress]
        if (holder != null) {
            return holder.page
        }
        val page = ByteArray(pageSize)
        log.readBytes(page, pageAddress)
        return page
    }

    fun getCurrentlyWritten(pageAddress: Long): ByteArray? {
        return if (currentPage!!.pageAddress == pageAddress) {
            currentPage!!.bytes
        } else null
    }

    fun removeBlock(blockAddress: Long, rbt: RemoveBlockType?) {
        val block = blockSet!!.getBlock(blockAddress)
        log.notifyBeforeBlockDeleted(block)
        try {
            writer.removeBlock(blockAddress, rbt!!)
            log.clearFileFromLogCache(blockAddress, 0)
        } finally {
            log.notifyAfterBlockDeleted(blockAddress)
        }
    }

    fun getBlock(blockAddress: Long): Block {
        return blockSet!!.getBlock(blockAddress)
    }

    fun forgetFiles(files: LongArray, fileBoundary: Long) {
        assert(blockSetMutable != null)
        var waitForCompletion = false
        for (file in files) {
            blockSetMutable!!.remove(file)
            blockSetWasChanged = true
            if (!waitForCompletion) {
                val fileEnd = file + fileBoundary
                var pageAddress = file
                while (pageAddress < fileEnd) {
                    if (writeCache.containsKey(pageAddress)) {
                        waitForCompletion = true
                        break
                    }
                    pageAddress += pageSize.toLong()
                }
            }
        }
        if (waitForCompletion) {
            ensureWritesAreCompleted()
        }
    }

    fun allFiles(): LongArray {
        return blockSet!!.getFiles()
    }

    fun getMinimumFile(): Long? = blockSet!!.getMinimum()
    fun getMaximumFile(): Long? = blockSet!!.getMaximum()

    fun flush() {
        checkWriteError()
        if (currentPage!!.committedCount < currentPage!!.writtenCount) {
            val pageAddress = currentPage!!.pageAddress
            val bytes = currentPage!!.bytes
            addPageToWriteCache(pageAddress, 0, bytes)
            logCache.cachePage(log, pageAddress, bytes)
        }
    }

    private fun checkWriteError() {
        if (writeError != null) {
            throw ExodusException.toExodusException(writeError)
        }
    }

    fun padPageWithNulls(): Int {
        checkWriteError()
        val written = doPadPageWithNulls()
        currentHighAddress += written.toLong()
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        if (written > 0) {
            assert(currentPage!!.writtenCount == pageSize)
            writePage(currentPage)
        }
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        return written
    }

    fun padWholePageWithNulls() {
        checkWriteError()
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        val written = doPadWholePageWithNulls()
        currentHighAddress += written.toLong()
        if (written > 0) {
            assert(currentPage!!.writtenCount == pageSize)
            writePage(currentPage)
        }
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
    }

    fun padWithNulls(fileLengthBound: Long, nullPage: ByteArray): Int {
        checkWriteError()
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        assert(nullPage.size == pageSize)
        var written = doPadPageWithNulls()
        if (written > 0) {
            assert(currentPage!!.writtenCount == pageSize)
            writePage(currentPage)
        }
        val spaceWritten = (currentHighAddress + written) % fileLengthBound
        if (spaceWritten == 0L) {
            currentHighAddress += written.toLong()
            assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
            return written
        }
        val reminder = fileLengthBound - spaceWritten
        val pages = reminder / pageSize
        assert(reminder % pageSize == 0L)
        for (i in 0 until pages) {
            val currentPage = allocNewPage(nullPage)
            writePage(currentPage)
            written += pageSize
        }
        currentHighAddress += written.toLong()
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
        return written
    }

    private fun allocateNewPageIfNeeded(): MutablePage? {
        val currentPage = currentPage
        return if (currentPage!!.writtenCount == pageSize) {
            allocNewPage()
        } else currentPage
    }

    fun sync() {
        doSync(highAddress)
    }

    private fun doSync(committedHighAddress: Long) {
        val currentPage = currentPage
        addHashCodeToPage(currentPage)
        if (currentPage!!.writtenCount > currentPage.committedCount) {
            writePage(currentPage)
        }
        ensureWritesAreCompleted()
        checkWriteError()
        writer.sync()
        assert(lastSyncedAddress <= committedHighAddress)
        if (lastSyncedAddress < committedHighAddress) {
            lastSyncedAddress = committedHighAddress
        }
        lastSyncedTs = System.currentTimeMillis()
    }

    private fun addHashCodeToPage(currentPage: MutablePage?) {
        if (currentPage!!.writtenCount == 0) {
            assert(currentPage.committedCount == 0)
            return
        }
        val lenSpaceLeft = pageSize - currentPage.writtenCount
        assert(lenSpaceLeft == 0 || lenSpaceLeft > HASH_CODE_SIZE)
        if (lenSpaceLeft <= HASH_CODE_SIZE + java.lang.Long.BYTES + java.lang.Byte.BYTES) {
            val written = doPadPageWithNulls()
            currentHighAddress += written.toLong()
            assert(currentHighAddress == currentPage.pageAddress + currentPage.writtenCount)
            assert(written == lenSpaceLeft)
            return
        }
        currentPage.bytes[currentPage.writtenCount++] = (0x80 xor HashCodeLoggable.TYPE.toInt()).toByte()
        val hashCode = calculateHashRecordHashCode(
            sha256,
            currentPage.bytes, currentPage.writtenCount - 1
        )
        BindingUtils.writeLong(hashCode, currentPage.bytes, currentPage.writtenCount)
        currentPage.writtenCount += HASH_CODE_SIZE
        currentHighAddress += (HASH_CODE_SIZE + java.lang.Byte.BYTES).toLong()
        assert(currentHighAddress == currentPage.pageAddress + currentPage.writtenCount)
    }

    fun close(sync: Boolean) {
        if (sync) {
            sync()
        } else {
            ensureWritesAreCompleted()
        }
        writer.close()
        writeCache.clear()
        if (currentPage != null) {
            currentPage!!.xxHash64.close()
            currentPage = null
        }
        if (blockSetMutable != null) {
            blockSetMutable!!.clear()
            blockSet = blockSetMutable!!.endWrite()
            blockSetMutable = null
        }
        lastSyncedTs = Long.MAX_VALUE
        lastSyncedAddress = Long.MAX_VALUE
    }

    fun getFilesSize(): Int {
        assert(blockSetMutable != null)
        return blockSetMutable!!.size()
    }

    fun clear() {
        ensureWritesAreCompleted()
        writeCache.clear()
        writer.clear()
        currentHighAddress = 0
        highAddress = 0
        lastSyncedAddress = 0
        if (currentPage != null) {
            currentPage!!.xxHash64.close()
        }
        blockSetMutable = null
        currentPage = MutablePage(ByteArray(pageSize), 0, 0, 0)
        blockSet = BlockSet.Immutable(log.fileLengthBound)
        assert(currentHighAddress == currentPage!!.pageAddress + currentPage!!.writtenCount)
    }

    private fun ensureWritesAreCompleted() {
        localWritesSemaphore.acquireUninterruptibly(Int.MAX_VALUE)
        localWritesSemaphore.release(Int.MAX_VALUE)
        assert(assertWriteCompletedWriteCache())
    }

    private fun assertWriteCompletedWriteCache(): Boolean {
        assert(writeCache.size <= 1)
        if (writeCache.size == 1) {
            val (key, value) = writeCache.entries.iterator().next()
            assert(key == currentPage!!.pageAddress)
            assert(value!!.written.get() < pageSize)
        }
        return true
    }

    private fun writePage(page: MutablePage?) {
        val bytes = page!!.bytes
        val xxHash64 = page.xxHash64
        val pageAddress = page.pageAddress
        val writtenCount = page.writtenCount
        val committedCount = page.committedCount
        val len = writtenCount - committedCount
        if (len > 0) {
            val contentLen: Int = if (writtenCount < pageSize) {
                len
            } else {
                len - HASH_CODE_SIZE
            }
            var encryptedBytes: ByteArray? = null
            if (cipherProvider == null) {
                if (calculateHashCode) {
                    xxHash64.update(bytes, committedCount, contentLen)
                    if (writtenCount == pageSize) {
                        BindingUtils.writeLong(
                            xxHash64.value, bytes,
                            adjustedPageSize
                        )
                    }
                }
            } else {
                encryptedBytes = cryptBlocksImmutable(
                    cipherProvider, cipherKey!!,
                    cipherBasicIV, pageAddress, bytes, committedCount, len, LogUtil.LOG_BLOCK_ALIGNMENT
                )
                if (calculateHashCode) {
                    xxHash64.update(encryptedBytes, 0, contentLen)
                    if (writtenCount == pageSize) {
                        BindingUtils.writeLong(xxHash64.value, encryptedBytes, contentLen)
                    }
                }
            }
            writeBoundarySemaphore.acquireUninterruptibly()
            localWritesSemaphore.acquireUninterruptibly()
            assert(writer.position() <= log.fileLengthBound)
            assert(
                writer.position() % log.fileLengthBound ==
                        (currentPage!!.pageAddress + currentPage!!.committedCount) % log.fileLengthBound
            )
            addPageToWriteCache(pageAddress, 0, bytes)
            logCache.cachePage(log, pageAddress, bytes)
            val result: Pair<Block, CompletableFuture<LongIntPair>>
            (if (cipherProvider != null) {
                writer.asyncWrite(encryptedBytes, 0, len)
            } else {
                writer.asyncWrite(bytes, committedCount, len)
            }).also { result = it }
            assert(writer.position() <= log.fileLengthBound)
            assert(
                writer.position() % log.fileLengthBound ==
                        (currentPage!!.pageAddress + currentPage!!.writtenCount) % log.fileLengthBound
            )
            val block = result.first
            val blockAddress = block.address
            assert(blockSetMutable != null)
            if (!blockSetMutable!!.contains(blockAddress)) {
                blockSetMutable!!.add(block.address, block)
                blockSetWasChanged = false
            }
            page.committedCount = page.writtenCount
            result.second.whenComplete(writeCompletionHandler)
        }
    }

    private fun allocNewPage(): MutablePage? {
        var currentPage = currentPage
        currentPage!!.xxHash64.close()
        this.currentPage = MutablePage(
            ByteArray(pageSize),
            currentPage.pageAddress + pageSize, 0, 0
        )
        currentPage = this.currentPage
        return currentPage
    }

    private fun allocNewPage(pageData: ByteArray): MutablePage {
        assert(pageData.size == pageSize)
        val currentPage = currentPage
        currentPage!!.xxHash64.close()
        return MutablePage(
            pageData,
            currentPage.pageAddress + pageSize, pageData.size, 0
        ).also { this.currentPage = it }
    }

    fun numberOfFiles(): Int {
        return blockSet!!.size()
    }

    fun getFilesFrom(address: Long): LongIterator {
        return blockSet!!.getFilesFrom(address)
    }

    fun closeFileIfNecessary(fileLengthBound: Long, makeFileReadOnly: Boolean) {
        val currentPage = currentPage
        val endPosition = writer.position() + currentPage!!.writtenCount - currentPage.committedCount
        assert(endPosition <= fileLengthBound)
        if (endPosition == fileLengthBound) {
            sync()
            assert(lastSyncedAddress <= highAddress)
            lastSyncedAddress = highAddress
            lastSyncedTs = System.currentTimeMillis()
            writer.close()
            assert(blockSetMutable != null)
            val blockSet = blockSetMutable
            val lastFile = blockSet!!.getMaximum()
            if (lastFile != null) {
                val block = blockSet.getBlock(lastFile)
                val refreshed = block.refresh()
                if (block !== refreshed) {
                    blockSet.add(lastFile, refreshed)
                }
                val length = refreshed.length()
                check(length >= fileLengthBound) {
                    ("File's too short (" + LogUtil.getLogFilename(lastFile)
                            + "), block.length() = " + length + ", fileLengthBound = " + fileLengthBound)
                }
                if (makeFileReadOnly && block is File) {
                    (block as File).setReadOnly()
                }
            }
        }
    }

    fun openNewFileIfNeeded(fileLengthBound: Long, log: Log) {
        assert(blockSetMutable != null)
        if (!writer.isOpen) {
            val fileAddress = currentHighAddress - currentHighAddress % fileLengthBound
            val block = writer.openOrCreateBlock(fileAddress, currentHighAddress % fileLengthBound)
            val fileCreated = !blockSetMutable!!.contains(fileAddress)
            if (fileCreated) {
                blockSetMutable!!.add(fileAddress, block)
                blockSetWasChanged = true

                // fsync the directory to ensure we will find the log file in the directory after system crash
                writer.syncDirectory()
                log.notifyBlockCreated(block)
            } else {
                log.notifyBlockModified(block)
            }
        }
    }

    private fun doPadWholePageWithNulls(): Int {
        val writtenInPage = currentPage!!.writtenCount
        if (writtenInPage > 0) {
            val written = pageSize - writtenInPage
            Arrays.fill(currentPage!!.bytes, writtenInPage, pageSize, 0x80.toByte())
            currentPage!!.writtenCount = pageSize
            return written
        }
        return 0
    }

    private fun doPadPageWithNulls(): Int {
        val writtenInPage = currentPage!!.writtenCount
        return if (writtenInPage > 0) {
            val pageDelta = adjustedPageSize - writtenInPage
            var written = 0
            if (pageDelta > 0) {
                Arrays.fill(currentPage!!.bytes, writtenInPage, adjustedPageSize, 0x80.toByte())
                currentPage!!.writtenCount = pageSize
                written = pageDelta + HASH_CODE_SIZE
            }
            written
        } else {
            0
        }
    }

    private class MutablePage(
        page: ByteArray,
        pageAddress: Long, writtenCount: Int, committedCount: Int
    ) {
        @JvmField
        val bytes: ByteArray
        @JvmField
        val pageAddress: Long
        @JvmField
        var committedCount: Int
        @JvmField
        var writtenCount: Int
        @JvmField
        val xxHash64: StreamingXXHash64

        init {
            assert(committedCount <= writtenCount)
            bytes = page
            this.pageAddress = pageAddress
            this.writtenCount = writtenCount
            this.committedCount = committedCount
            xxHash64 = XX_HASH_FACTORY.newStreamingHash64(
                XX_HASH_SEED
            )
        }
    }

    private class PageHolder(@JvmField @Volatile var page: ByteArray, written: Int) {
        @JvmField
        val written: AtomicInteger

        init {
            this.written = AtomicInteger(written)
        }
    }

    companion object : KLogging() {
        const val XX_HASH_SEED = 0xADEF1279AL
        @JvmField
        val XX_HASH_FACTORY: XXHashFactory = XXHashFactory.fastestJavaInstance()
        @JvmField
        val xxHash: XXHash64 = XX_HASH_FACTORY.hash64()
        const val HASH_CODE_SIZE = java.lang.Long.BYTES
        fun checkPageConsistency(pageAddress: Long, bytes: ByteArray, pageSize: Int, log: Log) {
            if (pageSize != bytes.size) {
                DataCorruptionException.raise(
                    "Unexpected page size (bytes). {expected " + pageSize
                            + ": , actual : " + bytes.size + "}", log, pageAddress
                )
            }
            val calculatedHash = calculatePageHashCode(bytes, 0, pageSize - HASH_CODE_SIZE)
            val storedHash = BindingUtils.readLong(bytes, pageSize - HASH_CODE_SIZE)
            if (storedHash != calculatedHash) {
                DataCorruptionException.raise(
                    "Page is broken. Expected and calculated hash codes are different.",
                    log, pageAddress
                )
            }
        }

        fun checkLastPageConsistency(
            sha256: MessageDigest?,
            pageAddress: Long,
            bytes: ByteArray,
            pageSize: Int,
            log: Log
        ): Int {
            if (pageSize != bytes.size) {
                var lastHashBlock = -1
                for (i in 0 until
                        (bytes.size - (java.lang.Byte.BYTES + java.lang.Long.BYTES))
                            .coerceAtMost(pageSize - HASH_CODE_SIZE - (java.lang.Byte.BYTES + java.lang.Long.BYTES))) {
                    val type = (0x80.toByte().toInt() xor bytes[i].toInt()).toByte()
                    if (HashCodeLoggable.isHashCodeLoggable(type)) {
                        val loggable = HashCodeLoggable(pageAddress + i, i, bytes)
                        val hash = calculateHashRecordHashCode(sha256, bytes, i)
                        if (hash == loggable.hashCode) {
                            lastHashBlock = i
                        }
                    }
                }
                if (lastHashBlock > 0) {
                    return lastHashBlock
                }
                DataCorruptionException.raise(
                    "Unexpected page size (bytes). {expected " + pageSize
                            + ": , actual : " + bytes.size + "}", log, pageAddress
                )
            }
            val calculatedHash = calculatePageHashCode(bytes, 0, pageSize - HASH_CODE_SIZE)
            val storedHash = BindingUtils.readLong(bytes, pageSize - HASH_CODE_SIZE)
            if (storedHash != calculatedHash) {
                DataCorruptionException.raise(
                    "Page is broken. Expected and calculated hash codes are different.",
                    log, pageAddress
                )
            }
            return pageSize
        }

        private fun calculateHashRecordHashCode(sha256: MessageDigest?, bytes: ByteArray, len: Int): Long {
            val hashCode: Long = if (sha256 != null) {
                sha256.update(bytes, 0, len)
                BindingUtils.readLong(sha256.digest(), 0)
            } else {
                calculatePageHashCode(bytes, 0, len)
            }

            return hashCode
        }

        @JvmStatic
        fun calculatePageHashCode(bytes: ByteArray, offset: Int, len: Int): Long {
            val xxHash = xxHash
            return xxHash.hash(bytes, offset, len, XX_HASH_SEED)
        }

        fun updatePageHashCode(bytes: ByteArray) {
            val hashCodeOffset = bytes.size - HASH_CODE_SIZE
            updatePageHashCode(bytes, 0, hashCodeOffset)
        }

        @JvmStatic
        fun updatePageHashCode(bytes: ByteArray, offset: Int, len: Int) {
            val hash = calculatePageHashCode(bytes, offset, len)
            BindingUtils.writeLong(hash, bytes, len)
        }

        fun generateNullPage(pageSize: Int): ByteArray {
            val data = ByteArray(pageSize)
            Arrays.fill(data, 0, pageSize - HASH_CODE_SIZE, 0x80.toByte())
            val hash = xxHash.hash(data, 0, pageSize - HASH_CODE_SIZE, XX_HASH_SEED)
            BindingUtils.writeLong(hash, data, pageSize - HASH_CODE_SIZE)
            return data
        }
    }
}
