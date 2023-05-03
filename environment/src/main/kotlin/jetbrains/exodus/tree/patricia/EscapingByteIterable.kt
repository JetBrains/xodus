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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterableBase
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.ExodusException

/**
 * Escapes zeros in origin byte iterable.
 */
internal class EscapingByteIterable(private val origin: ByteIterable) : ByteIterableBase() {
    override fun getIterator(): ByteIterator {
        return object : ByteIterator {
            private val originIt = origin.iterator()
            private var hasEscaped = false
            private var escaped: Byte = 0
            override fun hasNext(): Boolean {
                return hasEscaped || originIt.hasNext()
            }

            override fun next(): Byte {
                if (hasEscaped) {
                    hasEscaped = false
                    return escaped
                }
                val nextByte = originIt.next()
                if (nextByte.toInt() == 0 || nextByte.toInt() == 1) {
                    hasEscaped = true
                    escaped = (nextByte + 1).toByte()
                    return ESCAPING_BYTE
                }
                return nextByte
            }

            override fun skip(bytes: Long): Long {
                throw UnsupportedOperationException()
            }
        }
    }

    companion object {
        const val ESCAPING_BYTE: Byte = 1
    }
}

internal class UnEscapingByteIterable(private val origin: ByteIterable) : ByteIterableBase() {
    override fun getIterator(): ByteIterator {
        return object : ByteIterator {
            private val originIt = origin.iterator()
            override fun hasNext(): Boolean {
                return originIt.hasNext()
            }

            override fun next(): Byte {
                var nextByte = originIt.next()
                if (nextByte == EscapingByteIterable.ESCAPING_BYTE) {
                    if (!originIt.hasNext()) {
                        throw ExodusException("No byte follows escaping byte")
                    }
                    nextByte = (originIt.next() - 1).toByte()
                }
                return nextByte
            }

            override fun skip(bytes: Long): Long {
                throw UnsupportedOperationException()
            }
        }
    }
}
