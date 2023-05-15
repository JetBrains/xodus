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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * Lightweight substitute of ByteArrayOutputStream: toByteArray() doesn't create a copy.
 */
class LightByteArrayOutputStream : ByteArrayOutputStream {
    constructor() : super()
    constructor(size: Int) : super(size)

    fun write(buffer: ByteBuffer): Int {
        val bufferLen = buffer.remaining()
        if (bufferLen > 0) {
            val newCount = count + bufferLen
            if (newCount > buf.size) {
                buf = Arrays.copyOf(buf, Math.max(buf.size shl 1, newCount))
            }
            buffer[buf, count, bufferLen]
            count = newCount
        }
        return bufferLen
    }

    override fun toByteArray(): ByteArray {
        return buf
    }

    override fun size(): Int {
        return count
    }

    fun setSize(size: Int) {
        count = size
    }
}
