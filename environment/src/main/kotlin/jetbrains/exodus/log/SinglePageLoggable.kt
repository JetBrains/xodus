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

class SinglePageLoggable(
    address: Long,
    end: Long,
    type: Byte,
    structureId: Int,
    dataAddress: Long,
    bytes: ByteArray,
    start: Int,
    dataLength: Int
) : RandomAccessLoggable {
    private val address: Long
    private val end: Long
    private val structureId: Int
    private val type: Byte
    private var length = -1
    private val data: ArrayByteIterableWithAddress

    init {
        data = ArrayByteIterableWithAddress(dataAddress, bytes, start, dataLength)
        this.structureId = structureId
        this.type = type
        this.address = address
        this.end = end
    }

    override fun getAddress(): Long = address

    override fun getStructureId(): Int = structureId

    override fun getType(): Byte = type

    override fun getData(): ByteIterableWithAddress = data

    override fun length(): Int {
        if (length >= 0) {
            return length
        }
        val dataLength = data.length
        length = dataLength + CompressedUnsignedLongByteIterable.getCompressedSize(structureId.toLong()) +
                CompressedUnsignedLongByteIterable.getCompressedSize(dataLength.toLong()) + 1
        return length
    }

    override fun end(): Long {
        return end
    }

    override fun getDataLength(): Int = data.length
    override fun isDataInsideSinglePage(): Boolean = true

    companion object {
        val NULL_PROTOTYPE = SinglePageLoggable(
            Loggable.NULL_ADDRESS,
            Loggable.NULL_ADDRESS,
            0.toByte(),
            Loggable.NO_STRUCTURE_ID,
            Loggable.NULL_ADDRESS,
            ByteIterable.EMPTY_BYTES,
            0,
            0
        )
    }
}
