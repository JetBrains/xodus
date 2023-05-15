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
package jetbrains.exodus.util

import java.io.*

object UTFUtil {
    const val NULL_STRING_UTF_VALUE = 0xff
    private const val SINGLE_UTF_CHUNK_SIZE = 0x10000 / 3

    /**
     * Writes long strings to output stream as several chunks.
     *
     * @param stream stream to write to.
     * @param str    string to be written.
     * @throws IOException if something went wrong
     */
    @JvmStatic
    @Throws(IOException::class)
    fun writeUTF(stream: OutputStream, str: String) {
        DataOutputStream(stream).use { dataStream ->
            val len = str.length
            if (len < SINGLE_UTF_CHUNK_SIZE) {
                dataStream.writeUTF(str)
            } else {
                var startIndex = 0
                var endIndex: Int
                do {
                    endIndex = startIndex + SINGLE_UTF_CHUNK_SIZE
                    if (endIndex > len) {
                        endIndex = len
                    }
                    dataStream.writeUTF(str.substring(startIndex, endIndex))
                    startIndex += SINGLE_UTF_CHUNK_SIZE
                } while (endIndex < len)
            }
        }
    }

    /**
     * Reads a string from input stream saved as a sequence of UTF chunks and closes steam after its usage.
     *
     * @param stream stream to read from.
     * @return output string
     * @throws IOException if something went wrong
     */
    @JvmStatic
    @Throws(IOException::class)
    fun readUTF(stream: InputStream): String? {
        val dataInput = DataInputStream(stream)
        if (stream is ByteArraySizedInputStream) {
            val sizedStream = stream
            val streamSize = sizedStream.size()
            if (streamSize >= 2) {
                sizedStream.mark(Int.MAX_VALUE)
                try {
                    val utfLen = dataInput.readUnsignedShort()
                    if (utfLen == streamSize - 2) {
                        var isAscii = true
                        val bytes = sizedStream.toByteArray()
                        for (i in 0 until utfLen) {
                            if (bytes!![i + 2].toInt() and 0xff > 127) {
                                isAscii = false
                                break
                            }
                        }
                        if (isAscii) {
                            return fromAsciiByteArray(bytes!!, 2, utfLen)
                        }
                    }
                } finally {
                    sizedStream.reset()
                }
            }
        }
        dataInput.use {

            //streams managed by transaction should be reset.
            if (stream.markSupported()) {
                stream.mark(Int.MAX_VALUE)
            }
            var result: String? = null
            var builder: StringBuilder? = null
            while (true) {
                val temp: String
                try {
                    temp = dataInput.readUTF()
                    if (result != null && result.length == 0 && builder != null && builder.length == 0 && temp.length == 0) {
                        break
                    }
                } catch (e: EOFException) {
                    break
                }
                if (result == null) {
                    result = temp
                } else {
                    if (builder == null) {
                        builder = StringBuilder()
                        builder.append(result)
                    }
                    builder.append(temp)
                }
            }
            if (stream.markSupported()) {
                try {
                    stream.reset()
                } catch (e: IOException) {
                    //should never happen with tx managed steams
                }
            }
            return builder?.toString() ?: result
        }
    }

    fun fromAsciiByteArray(bytes: ByteArray, off: Int, len: Int): String {
        return String(bytes, 0, off, len)
    }

    @JvmStatic
    fun getUtfByteLength(value: String): Int {
        val strLen = value.length
        var len = strLen
        for (i in 0 until strLen) {
            val c = value[i]
            if (c.code > 0x7f || c.code < 1) {
                len += if (c.code > 0x7ff) 2 else 1
            }
        }
        return len
    }

    @JvmStatic
    fun utfCharsToBytes(value: String, bytes: ByteArray, offset: Int) {
        var offset = offset
        for (i in 0 until value.length) {
            val c = value[i].code
            if (c >= 0x0001 && c <= 0x007F) {
                bytes[offset++] = c.toByte()
            } else if (c > 0x07FF) {
                bytes[offset++] = (0xE0 or (c shr 12 and 0x0F)).toByte()
                bytes[offset++] = (0x80 or (c shr 6 and 0x3F)).toByte()
                bytes[offset++] = (0x80 or (c and 0x3F)).toByte()
            } else {
                bytes[offset++] = (0xC0 or (c shr 6 and 0x1F)).toByte()
                bytes[offset++] = (0x80 or (c and 0x3F)).toByte()
            }
        }
    }
}
