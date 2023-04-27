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
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.util.LightOutputStream

/**
 * This ByteIterable cannot be used for representing comparable or signed longs.
 */
class CompressedUnsignedLongByteIterable private constructor(l: Long) : ByteIterableBase() {
    private val l: Long

    init {
        require(l >= 0) { l.toString() }
        this.l = l
    }

    override fun getIterator(): ByteIterator {
        return object : ByteIterator {
            private var goon = true
            private var l = this@CompressedUnsignedLongByteIterable.l
            override fun hasNext(): Boolean {
                return goon
            }

            override fun next(): Byte {
                var b = (l and 0x7fL).toByte()
                l = l shr 7
                if (!(l > 0).also { goon = it }) {
                    b = (b.toInt() or 0x80).toByte()
                }
                return b
            }

            override fun skip(bytes: Long): Long {
                for (i in 0 until bytes) {
                    if (goon) {
                        l = l shr 7
                        goon = l > 0
                    } else {
                        return i
                    }
                }
                return bytes
            }
        }
    }

    companion object {
        private const val ITERABLES_CACHE_SIZE = 65536
        private val ITERABLES_CACHE: Array<ByteIterable> = Array(ITERABLES_CACHE_SIZE) {
            CompressedUnsignedLongByteIterable(it.toLong())
        }

        @JvmStatic
        fun getIterable(l: Long): ByteIterable {
            return if (l < ITERABLES_CACHE_SIZE) {
                ITERABLES_CACHE[l.toInt()]
            } else CompressedUnsignedLongByteIterable(l)
        }

        @JvmStatic
        fun fillBytes(l: Long, output: LightOutputStream) {
            var input = l
            require(input >= 0) { input.toString() }
            while (true) {
                val b = (input and 0x7fL).toByte()
                if (7.let { input = input shr it; input } == 0L) {
                    output.write(b.toInt() or 0x80)
                    break
                }
                output.write(b.toInt())
            }
        }

        @JvmStatic
        fun getLong(iterable: ByteIterable): Long {
            return getLong(iterable.iterator())
        }

        @JvmStatic
        fun getLong(iterator: ByteIterator): Long {
            var result: Long = 0
            var shift = 0
            do {
                val b = iterator.next()
                result += (b.toInt() and 0x7f).toLong() shl shift
                if (b.toInt() and 0x80 != 0) {
                    return result
                }
                shift += 7
            } while (iterator.hasNext())
            return throwBadCompressedNumber().toLong()
        }

        @JvmStatic
        fun getInt(iterable: ByteIterable): Int {
            return getInt(iterable.iterator())
        }

        @JvmStatic
        fun getInt(iterator: ByteIterator): Int {
            var result = 0
            var shift = 0
            do {
                val b = iterator.next()
                result += b.toInt() and 0x7f shl shift
                if (b.toInt() and 0x80 != 0) {
                    return result
                }
                shift += 7
            } while (iterator.hasNext())
            return throwBadCompressedNumber()
        }

        @JvmStatic
        fun getInt(iterator: DataIterator): Int {
            var b = iterator.next()
            if (b.toInt() and 0x80 != 0) {
                return b.toInt() and 0x7f
            }
            var result = b.toInt() and 0x7f
            var shift = 7
            while (true) {
                b = iterator.next()
                result += b.toInt() and 0x7f shl shift
                if (b.toInt() and 0x80 != 0) {
                    return result
                }
                shift += 7
            }
        }

        @JvmStatic
        fun getCompressedSize(l: Long): Int {
            var input = l
            if (input < 128) {
                return 1
            }
            if (input < 16384) {
                return 2
            }
            input = input shr 21
            var result = 3
            while (input > 0) {
                ++result
                input = input shr 7
            }
            return result
        }

        private fun throwBadCompressedNumber(): Int {
            throw ExodusException("Bad compressed number")
        }
    }
}
