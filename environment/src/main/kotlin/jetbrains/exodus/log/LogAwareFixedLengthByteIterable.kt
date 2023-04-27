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
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.FixedLengthByteIterable

internal class LogAwareFixedLengthByteIterable(
    source: ByteIterableWithAddress,
    offset: Int, length: Int
) : FixedLengthByteIterable(source, offset, length) {
    override fun compareTo(other: ByteIterable): Int {
        return getSource().compareTo(offset, length, other, 0, other.length)
    }

    override fun compareTo(length: Int, right: ByteIterable, rightLength: Int): Int {
        require(length <= this.length)
        return getSource().compareTo(offset, length, right, 0, rightLength)
    }

    override fun compareTo(from: Int, length: Int, right: ByteIterable, rightFrom: Int, rightLength: Int): Int {
        require(length <= this.length - from)
        return getSource().compareTo(offset + from, length, right, rightFrom, rightLength)
    }

    override fun getSource(): ByteIterableWithAddress {
        return super.getSource() as ByteIterableWithAddress
    }

    override fun getIterator(): ByteIterator {
        if (length == 0) {
            return EMPTY_ITERATOR
        }
        if (bytes != null) {
            return ArrayByteIterable.Iterator(bytes, baseOffset, length)
        }
        val bi = source.iterator()
        bi.skip(offset.toLong())
        return LogAwareFixedLengthByteIterator(bi, length)
    }

    private class LogAwareFixedLengthByteIterator(
        private val si: ByteIterator,
        private var bytesToRead: Int
    ) : ByteIterator, BlockByteIterator {
        override fun hasNext(): Boolean {
            return bytesToRead > 0 && si.hasNext()
        }

        override fun next(): Byte {
            bytesToRead--
            return si.next()
        }

        override fun skip(bytes: Long): Long {
            val result = si.skip(Math.min(bytes, bytesToRead.toLong()))
            bytesToRead -= result.toInt()
            return result
        }

        override fun nextBytes(array: ByteArray, off: Int, len: Int): Int {
            val bytesToRead = Math.min(len, bytesToRead)
            if (si is BlockByteIterator) {
                val result = (si as BlockByteIterator).nextBytes(array, off, bytesToRead)
                this.bytesToRead -= result
                return result
            }
            for (i in 0 until bytesToRead) {
                array[off + i] = next()
            }
            return bytesToRead
        }
    }
}
