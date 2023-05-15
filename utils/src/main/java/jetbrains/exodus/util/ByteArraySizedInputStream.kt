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

import java.io.ByteArrayInputStream

class ByteArraySizedInputStream : ByteArrayInputStream {
    constructor(buf: ByteArray?) : super(buf)
    constructor(buf: ByteArray?, offset: Int, length: Int) : super(buf, offset, length)

    fun size(): Int {
        return count - mark
    }

    fun count(): Int {
        return count
    }

    fun pos(): Int {
        return pos
    }

    var pos: Int
        get() = super.pos
        set(pos) {
            this.pos = pos
        }

    fun toByteArray(): ByteArray {
        return buf
    }

    fun copy(): ByteArraySizedInputStream {
        return ByteArraySizedInputStream(buf, pos, count)
    }

    override fun hashCode(): Int {
        var result = size() + 1
        val mark = mark
        val count = count
        val buf = buf
        for (i in mark until count) {
            result = result * 31 + buf[i]
        }
        return result
    }

    override fun equals(obj: Any?): Boolean {
        if (obj !is ByteArraySizedInputStream) return false
        val right = obj
        val size = size()
        if (size != right.size()) return false
        val mark = mark
        val buf = buf
        val rMark = right.mark
        val rBuf = this.buf
        for (i in 0 until size) {
            if (buf[i + mark] != rBuf[i + rMark]) return false
        }
        return true
    }
}
