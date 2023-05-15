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

object HexUtil {
    @JvmOverloads
    fun byteArrayToString(array: ByteArray, off: Int = 0, len: Int = array.size): String {
        val builder = StringBuilder()
        for (i in 0 until len) {
            val b = array[off + i]
            var digit = b.toInt() shr 4 and 0xf
            builder.append((if (digit < 10) digit + '0'.code else digit - 10 + 'a'.code).toChar())
            digit = b.toInt() and 0xf
            builder.append((if (digit < 10) digit + '0'.code else digit - 10 + 'a'.code).toChar())
        }
        return builder.toString()
    }

    fun stringToByteArray(str: String): ByteArray {
        val strLen = str.length
        require(strLen and 1 != 1) { "Odd hex string length" }
        val result = ByteArray(strLen / 2)
        var i = 0
        var j = 0
        while (i < strLen) {
            result[j++] = (hexChar(str[i]) shl 4 or hexChar(str[i + 1])).toByte()
            i += 2
        }
        return result
    }

    private fun hexChar(c: Char): Int {
        val result = c.digitToIntOrNull(16) ?: -1
        require(!(result < 0 || result > 15)) { "Bad hex digit: $c" }
        return result
    }
}