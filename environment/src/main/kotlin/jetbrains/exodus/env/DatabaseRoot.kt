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
package jetbrains.exodus.env

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.util.LightOutputStream

class DatabaseRoot internal constructor(private val loggable: Loggable, it: ByteIterator) {
    val rootAddress: Long
    val lastStructureId: Int
    val isValid: Boolean

    constructor(loggable: Loggable) : this(loggable, loggable.data.iterator())

    init {
        rootAddress = CompressedUnsignedLongByteIterable.getLong(it)
        lastStructureId = CompressedUnsignedLongByteIterable.getInt(it)
        isValid = rootAddress ==
                CompressedUnsignedLongByteIterable.getLong(it) - lastStructureId - MAGIC_DIFF
    }

    val address: Long
        get() = loggable.address

    companion object {
        const val DATABASE_ROOT_TYPE: Byte = 1
        private const val MAGIC_DIFF = 199L
        fun asByteIterable(rootAddress: Long, lastStructureId: Int): ByteIterable {
            val output = LightOutputStream(20)
            CompressedUnsignedLongByteIterable.fillBytes(rootAddress, output)
            CompressedUnsignedLongByteIterable.fillBytes(lastStructureId.toLong(), output)
            CompressedUnsignedLongByteIterable.fillBytes(rootAddress + lastStructureId + MAGIC_DIFF, output)
            return output.asArrayByteIterable()
        }
    }
}
