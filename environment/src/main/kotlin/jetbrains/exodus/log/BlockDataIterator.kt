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
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.crypto.cryptBlocksMutable
import jetbrains.exodus.io.*
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException


class BlockDataIterator(
    private val log: Log,
    private val block: Block,
    override var address: Long,
    private val checkPage: Boolean,
    private val lastBlock: Boolean
) : ByteIteratorWithAddress {
    private val cipherProvider: StreamCipherProvider?
    private var end: Long
    override var currentPage: ByteArray? = null
    private val pageSize: Int
    private var chunkSize = 0
    private val config: LogConfig
    private var sha256: MessageDigest? = null
    private var throwCorruptionException = false

    init {
        end = block.address + block.length()
        pageSize = log.cachePageSize
        config = log.config
        cipherProvider = config.streamCipherProvider
        sha256 = if (cipherProvider != null) {
            try {
                MessageDigest.getInstance("SHA-256")
            } catch (e: NoSuchAlgorithmException) {
                throw ExodusException("SHA-256 hash function was not found", e)
            }
        } else {
            null
        }
        chunkSize = if (log.formatWithHashCodeIsUsed) {
            pageSize - BufferedDataWriter.HASH_CODE_SIZE
        } else {
            pageSize
        }
    }

    override fun hasNext(): Boolean {
        if (address == end && throwCorruptionException) {
            DataCorruptionException.raise("Last page was corrupted", log, address)
        }
        return address < end
    }

    override fun next(): Byte {
        if (address >= end) {
            DataCorruptionException.raise(
                "DataIterator: no more bytes available", log, address
            )
        }
        if (currentPage != null) {
            val pageOffset = address.toInt() and pageSize - 1
            assert(pageOffset <= pageSize)
            val result = currentPage!![pageOffset]
            address++
            if (pageOffset + 1 == chunkSize) {
                address = address - pageOffset - 1 + pageSize
                currentPage = null
            }
            return result
        }
        loadPage()
        return next()
    }

    private fun loadPage() {
        if (throwCorruptionException) {
            DataCorruptionException.raise("Last page was corrupted", log, address)
        }
        val currentPageSize = (end - address).coerceAtMost(pageSize.toLong()).toInt()
        val result = ByteArray(currentPageSize)
        val read = block.read(result, address - block.address, 0, currentPageSize)
        if (read != currentPageSize) {
            DataCorruptionException.raise(
                "Incorrect amount of bytes was read, expected " +
                        currentPageSize + " but was " + read,
                log, address
            )
        }
        if (checkPage) {
            if (currentPageSize != pageSize) {
                if (!lastBlock) {
                    DataCorruptionException.raise("Incorrect page size -  $currentPageSize", log, address)
                }
                if (cipherProvider != null) {
                    cryptBlocksMutable(
                        cipherProvider, config.cipherKey!!, config.cipherBasicIV,
                        address, result, 0, chunkSize.coerceAtMost(currentPageSize), LogUtil.LOG_BLOCK_ALIGNMENT
                    )
                }
                val validPageSize: Int = BufferedDataWriter.checkLastPageConsistency(
                    sha256,
                    address, result, pageSize, log
                )
                currentPage = result.copyOfRange(0, validPageSize)
                end = address + validPageSize
                throwCorruptionException = true
                return
            } else {
                BufferedDataWriter.checkPageConsistency(address, result, pageSize, log)
            }
        }
        val encryptedBytes: Int
        if (cipherProvider != null) {
            encryptedBytes = if (currentPageSize < pageSize) {
                currentPageSize
            } else {
                chunkSize
            }
            cryptBlocksMutable(
                cipherProvider, config.cipherKey!!, config.cipherBasicIV,
                address, result, 0, encryptedBytes, LogUtil.LOG_BLOCK_ALIGNMENT
            )
        }
        currentPage = result
    }

    override fun skip(bytes: Long): Long {
        val bytesToSkip = bytes.coerceAtMost(address - end)
        val currentPageOffset = address.toInt() and pageSize - 1
        val pageBytesToSkip = bytesToSkip.coerceAtMost((chunkSize - currentPageOffset).toLong())
        address += pageBytesToSkip
        if (bytesToSkip > pageBytesToSkip) {
            val rest = bytesToSkip - pageBytesToSkip
            val pagesToSkip = rest / chunkSize
            val pageSkip = pagesToSkip * pageSize
            val offsetSkip = pagesToSkip * chunkSize
            val pageOffset = (rest - offsetSkip).toInt()
            val addressDiff = pageSkip + pageOffset
            address += addressDiff
            currentPage = null
        }
        return bytesToSkip
    }

    override val offset: Int
        get() = (address - block.address).toInt()

    override fun available(): Int {
        throw UnsupportedOperationException()
    }
}
