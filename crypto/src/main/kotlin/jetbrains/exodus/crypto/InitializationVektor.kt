/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.crypto

const val LONG_BYTES = java.lang.Long.SIZE / java.lang.Byte.SIZE

/**
 * Converts `Long` initialization vector to a `ByteArray` of size 8.
 */
fun Long.toBytes(): ByteArray {
    val result = ByteArray(LONG_BYTES)
    var l = this
    repeat(LONG_BYTES, {
        result[it] = (l and 0xff).toByte()
        l = l shr java.lang.Byte.SIZE
    })
    return result
}