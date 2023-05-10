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

import jetbrains.exodus.bindings.LongBinding

class DataIterator(
    private val log: Log,
    startAddress: Long = -1L,
    private var length: Long = Long.MAX_VALUE
) : ByteIteratorWithAddress {
    private val cachePageSize: Int = log.cachePageSize
    private val pageAddressMask: Long = (cachePageSize - 1).toLong().inv()
    private var pageAddress: Long
    private var currentPage: ByteArray? = null
    private var offset = 0
    private var chunkLength = 0
    private val formatWithHashCodeIsUsed: Boolean

    init {
        pageAddress = -1L
        formatWithHashCodeIsUsed = log.formatWithHashCodeIsUsed
        if (startAddress >= 0) {
            checkPageSafe(startAddress)
        }
    }

    override fun getCurrentPage(): ByteArray? = currentPage

    override fun getOffset(): Int = offset

    override fun hasNext(): Boolean {
        assert(length >= 0)
        if (currentPage == null || length == 0L) {
            return false
        }
        if (offset >= chunkLength) {
            checkPageSafe(getAddress())
            return hasNext()
        }
        return true
    }

    override fun next(): Byte {
        if (!hasNext()) {
            DataCorruptionException.raise(
                "DataIterator: no more bytes available", log, getAddress()
            )
        }
        assert(length > 0)
        val current = offset
        offset++
        length--
        assert(offset <= chunkLength)
        return currentPage!![current]
    }

    override fun skip(bytes: Long): Long {
        if (bytes <= 0) {
            return 0
        }
        val bytesToSkip = bytes.coerceAtMost(length)
        val pageBytesToSkip = bytesToSkip.coerceAtMost((chunkLength - offset).toLong())
        offset += pageBytesToSkip.toInt()
        if (bytesToSkip > pageBytesToSkip) {
            var chunkSize: Long = (cachePageSize - BufferedDataWriter.HASH_CODE_SIZE).toLong()
            if (!formatWithHashCodeIsUsed) {
                chunkSize = cachePageSize.toLong()
            }
            val rest = bytesToSkip - pageBytesToSkip
            val pagesToSkip = rest / chunkSize
            val pageSkip = pagesToSkip * cachePageSize
            val offsetSkip = pagesToSkip * chunkSize
            val pageOffset = (rest - offsetSkip).toInt()
            val addressDiff = pageSkip + pageOffset
            checkPageSafe(getAddress() + addressDiff)
        }
        length -= bytesToSkip
        return bytesToSkip
    }

    override fun nextLong(length: Int): Long {
        if (this.length < length) {
            DataCorruptionException.raise(
                "DataIterator: no more bytes available", log, getAddress()
            )
        }
        if (currentPage == null || chunkLength - offset < length) {
            return LongBinding.entryToUnsignedLong(this, length)
        }
        val result = LongBinding.entryToUnsignedLong(currentPage, offset, length)
        offset += length
        this.length -= length.toLong()
        return result
    }

    fun checkPage(address: Long) {
        val pageAddress = address and pageAddressMask
        if (this.pageAddress != pageAddress) {
            if (address >= log.getHighReadAddress()) {
                BlockNotFoundException.raise(log, address)
                return
            }
            currentPage = log.getCachedPage(pageAddress)
            this.pageAddress = pageAddress
        }
        chunkLength = cachePageSize - BufferedDataWriter.HASH_CODE_SIZE
        if (!formatWithHashCodeIsUsed) {
            chunkLength = cachePageSize
        }
        offset = (address - pageAddress).toInt()
    }

    override fun available(): Int {
        if (length > Int.MAX_VALUE) {
            throw UnsupportedOperationException()
        }
        return length.toInt()
    }

    override fun getAddress(): Long {
        assert(offset <= chunkLength)

        return if (offset < chunkLength) {
            pageAddress + offset
        } else pageAddress + cachePageSize
    }

    private fun checkPageSafe(address: Long) {
        try {
            checkPage(address)
            val pageAddress = address and pageAddressMask
            chunkLength = (log.getHighReadAddress() - pageAddress).coerceAtMost(
                (
                        cachePageSize - BufferedDataWriter.HASH_CODE_SIZE).toLong()
            ).toInt()
            if (!formatWithHashCodeIsUsed) {
                chunkLength = (log.getHighAddress() - pageAddress).coerceAtMost(cachePageSize.toLong()).toInt()
            }
            if (chunkLength > offset) {
                return
            }
        } catch (ignore: BlockNotFoundException) {
        }
        pageAddress = -1L
        currentPage = null
        chunkLength = 0
        offset = 0
    }

    override fun availableInCurrentPage(bytes: Int): Boolean {
        return chunkLength - offset >= bytes
    }
}
