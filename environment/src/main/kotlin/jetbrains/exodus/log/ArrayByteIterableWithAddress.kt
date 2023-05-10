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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding

class ArrayByteIterableWithAddress(
    private val dataAddress: Long, bytes: ByteArray,
    start: Int, length: Int
) : ArrayByteIterable(bytes, start, length), ByteIterableWithAddress {

    override fun getDataAddress(): Long = dataAddress

    override fun nextLong(offset: Int, length: Int): Long {
        val start = this.offset + offset
        val end = start + length
        var result: Long = 0
        for (i in start until end) {
            result = (result shl 8) + (bytes[i].toInt() and 0xff)
        }
        return result
    }

    override fun getCompressedUnsignedInt(): Int {
        var result = 0
        var shift = 0
        var i = offset
        while (true) {
            val b = bytes[i]
            result += b.toInt() and 0x7f shl shift
            if (b.toInt() and 0x80 != 0) {
                return result
            }
            shift += 7
            ++i
        }
    }

    override fun iterator(): ArrayByteIteratorWithAddress {
        return ArrayByteIteratorWithAddress(bytes, offset, length)
    }

    override fun iterator(offset: Int): ArrayByteIteratorWithAddress {
        return ArrayByteIteratorWithAddress(
            bytes,
            this.offset + offset, length - offset
        )
    }

    override fun cloneWithOffset(offset: Int): ArrayByteIterableWithAddress {
        return ArrayByteIterableWithAddress(
            dataAddress + offset, bytes,
            this.offset + offset, length - offset
        )
    }

    override fun cloneWithAddressAndLength(address: Long, length: Int): ArrayByteIterableWithAddress {
        val offset = (address - dataAddress).toInt()
        return ArrayByteIterableWithAddress(
            address, bytes,
            this.offset + offset, length
        )
    }

    inner class ArrayByteIteratorWithAddress internal constructor(bytes: ByteArray?, offset: Int, length: Int) :
        Iterator(bytes, offset, length), ByteIteratorWithAddress {
        override fun getAddress(): Long = dataAddress + super.offset - this@ArrayByteIterableWithAddress.offset
        override fun getOffset(): Int = super.offset

        override fun available(): Int {
            return end - super.offset
        }

        override fun nextLong(length: Int): Long {
            val result = LongBinding.entryToUnsignedLong(bytes, super.offset, length)
            super.offset += length
            return result
        }

        override fun getCompressedUnsignedInt(): Int {
            if (super.offset == end) {
                throw NoSuchElementException()
            }
            var result = 0
            var shift = 0
            do {
                val b = bytes[super.offset++]
                result += b.toInt() and 0x7f shl shift
                if (b.toInt() and 0x80 != 0) {
                    return result
                }
                shift += 7
            } while (super.offset < end)
            throw NoSuchElementException()
        }

        override fun getCompressedUnsignedLong(): Long {
            if (super.offset == end) {
                throw NoSuchElementException()
            }
            var result: Long = 0
            var shift = 0
            do {
                val b = bytes[super.offset++]
                result += (b.toInt() and 0x7f).toLong() shl shift
                if (b.toInt() and 0x80 != 0) {
                    return result
                }
                shift += 7
            } while (super.offset < end)
            throw NoSuchElementException()
        }
    }
}
