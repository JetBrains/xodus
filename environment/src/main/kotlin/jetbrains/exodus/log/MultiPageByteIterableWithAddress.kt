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

import jetbrains.exodus.*

class MultiPageByteIterableWithAddress(override val dataAddress: Long, private val length: Int, private val log: Log) :
    ByteIterableWithAddress {
    private var bytes: ByteArray? = null
    override fun getLength(): Int {
        return length
    }

    override fun getBytesUnsafe(): ByteArray {
        return doBytesUnsafe()!!
    }

    private fun doBytesUnsafe(): ByteArray? {
        if (bytes != null) {
            return bytes
        }
        when (length) {
            0 -> {
                bytes = ByteIterable.EMPTY_BYTES
            }
            1 -> {
                val iterator = iterator()
                bytes = ByteIterableBase.SINGLE_BYTES[0xFF and iterator.next().toInt()]
            }
            else -> {
                val iterator = iterator()
                bytes = ByteArray(length)
                for (i in 0 until length) {
                    bytes!![i] = iterator.next()
                }
            }
        }
        return bytes
    }

    override fun baseOffset(): Int {
        return 0
    }

    override fun getBaseBytes(): ByteArray {
        return doBytesUnsafe()!!
    }

    override fun iterator(): ByteIteratorWithAddress {
        return DataIterator(log, dataAddress, length.toLong())
    }

    override fun iterator(offset: Int): ByteIteratorWithAddress {
        return if (offset == 0) {
            DataIterator(log, dataAddress, length.toLong())
        } else DataIterator(
            log,
            log.adjustLoggableAddress(dataAddress, offset.toLong()),
            length.toLong()
        )
    }

    override fun byteAt(offset: Int): Byte {
        return iterator(offset).next()
    }

    override fun nextLong(offset: Int, length: Int): Long {
        return iterator(offset).nextLong(length)
    }

    override val compressedUnsignedInt: Int
        get() = CompressedUnsignedLongByteIterable.getInt(this)

    override fun compareTo(other: ByteIterable): Int {
        return compare(0, dataAddress, length, other, 0, other.length, log)
    }

    override fun compareTo(length: Int, right: ByteIterable, rightLength: Int): Int {
        return compare(0, dataAddress, length, right, 0, rightLength, log)
    }

    override fun compareTo(from: Int, length: Int, right: ByteIterable, rightFrom: Int, rightLength: Int): Int {
        return compare(from, dataAddress, length, right, rightFrom, rightLength, log)
    }

    override fun cloneWithOffset(offset: Int): ByteIterableWithAddress {
        require(offset <= length) {
            "Provided offset is " + offset +
                    " but maximum allowed offset (length) is " + length
        }
        val newAddress = log.adjustLoggableAddress(dataAddress, offset.toLong())
        return MultiPageByteIterableWithAddress(newAddress, length - offset, log)
    }

    override fun cloneWithAddressAndLength(address: Long, length: Int): ByteIterableWithAddress {
        return MultiPageByteIterableWithAddress(address, length, log)
    }

    override fun subIterable(offset: Int, length: Int): ByteIterable {
        return LogAwareFixedLengthByteIterable(this, offset, length)
    }

    companion object {
        private fun compare(
            leftOffset: Int, leftAddress: Long, len: Int,
            right: ByteIterable, rightOffset: Int, rightLen: Int,
            log: Log
        ): Int {
            val pageSize = log.cachePageSize
            val mask = pageSize - 1
            var alignedAddress = log.adjustLoggableAddress(leftAddress, leftOffset.toLong())
            var endAddress = log.adjustLoggableAddress(alignedAddress, len.toLong())
            endAddress -= (endAddress.toInt() and mask).toLong()
            var leftStep = alignedAddress.toInt() and mask
            alignedAddress -= leftStep.toLong()
            var leftPage = log.getPageIterable(alignedAddress)
            val leftPageLen = leftPage.length
            if (leftPageLen <= leftStep) { // alignment is >= 0 for sure
                BlockNotFoundException.raise(log, alignedAddress)
            }
            var leftBaseArray = leftPage.baseBytes
            var leftBaseOffset = leftPage.baseOffset()
            val rightBaseArray = right.baseBytes
            val rightBaseLen = right.length.coerceAtMost(rightLen)
            val rightBaseOffset = rightOffset + right.baseOffset()
            var rightStep = 0
            var limit = len.coerceAtMost((leftPageLen - leftStep).coerceAtMost(rightBaseLen))
            while (true) {
                while (rightStep < limit) {
                    val b1 = leftBaseArray[leftBaseOffset + leftStep++]
                    val b2 = rightBaseArray[rightBaseOffset + rightStep++]
                    if (b1 != b2) {
                        return (b1.toInt() and 0xff) - (b2.toInt() and 0xff)
                    }
                }
                if (rightStep == rightBaseLen || alignedAddress >= endAddress) {
                    return len - rightBaseLen
                }
                // move left array to next cache page
                alignedAddress += pageSize.toLong()
                leftPage = log.getPageIterable(alignedAddress)
                leftBaseArray = leftPage.baseBytes
                leftBaseOffset = leftPage.baseOffset()
                leftStep = 0
                limit = len.coerceAtMost((leftPage.length + rightStep).coerceAtMost(rightBaseLen))
            }
        }
    }
}
