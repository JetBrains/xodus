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

import jetbrains.exodus.bindings.BindingUtils

class HashCodeLoggable internal constructor(private val address: Long, pageOffset: Int, page: ByteArray?) :
    RandomAccessLoggable {
    @JvmField
    val hashCode: Long

    init {
        hashCode = BindingUtils.readLong(page, pageOffset + java.lang.Byte.BYTES)
    }

    override fun getAddress(): Long = address

    override fun length(): Int {
        return java.lang.Long.BYTES + 1
    }

    override fun end(): Long {
        return address + length()
    }

    override fun getData(): ByteIterableWithAddress {
        val bytes = ByteArray(java.lang.Long.BYTES)
        BindingUtils.writeLong(hashCode, bytes, 0)
        return ArrayByteIterableWithAddress(address + java.lang.Byte.BYTES, bytes, 0, bytes.size)
    }

    override fun getDataLength(): Int = Long.SIZE_BYTES
    override fun getStructureId(): Int = Loggable.NO_STRUCTURE_ID
    override fun isDataInsideSinglePage(): Boolean = true

    override fun getType(): Byte = TYPE

    companion object {
        const val TYPE: Byte = 0x7F
        fun isHashCodeLoggable(type: Byte): Boolean {
            return type == TYPE
        }

        fun isHashCodeLoggable(loggable: Loggable): Boolean {
            return loggable.getType() == TYPE
        }
    }
}