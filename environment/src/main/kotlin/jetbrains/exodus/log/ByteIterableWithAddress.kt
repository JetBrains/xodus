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

interface ByteIterableWithAddress : ByteIterable {
    val dataAddress: Long
    fun nextLong(offset: Int, length: Int): Long
    val compressedUnsignedInt: Int
    override fun iterator(): ByteIteratorWithAddress
    fun iterator(offset: Int): ByteIteratorWithAddress
    fun cloneWithOffset(offset: Int): ByteIterableWithAddress
    fun cloneWithAddressAndLength(address: Long, length: Int): ByteIterableWithAddress

    companion object {
        private fun getEmpty(): ByteIterableWithAddress {
            return ArrayByteIterableWithAddress(Loggable.NULL_ADDRESS, ByteIterable.EMPTY_BYTES, 0, 0)
        }

        @JvmField
        val EMPTY = getEmpty()
    }
}
