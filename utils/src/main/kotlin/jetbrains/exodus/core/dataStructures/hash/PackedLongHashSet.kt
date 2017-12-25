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
package jetbrains.exodus.core.dataStructures.hash

import java.lang.Long.SIZE
import java.util.*

private const val LONG_BITS: Long = SIZE.toLong()

class PackedLongHashSet : AbstractSet<Long>(), LongSet {

    private val map = LongHashMap<Long>(40, 2f)
    private var count: Int = 0

    override val size: Int
        get() {
            return count
        }

    override fun contains(element: Long): Boolean {
        val v = map[element.key]
        return v != null && v and (1L shl element.bit) != 0L
    }

    override fun add(element: Long): Boolean {
        val key = element.key
        val bit = element.bit
        val mask = 1L shl bit
        val v = map[key]
        if (v == null) {
            map[key] = mask
            ++count
            return true
        }
        if (v and mask != 0L) {
            return false
        }
        map[key] = v xor mask
        ++count
        return true
    }

    override fun remove(element: Long): Boolean {
        val key = element.key
        val bit = element.bit
        val mask = 1L shl bit
        var v = map[key]
        if (v == null || v and mask == 0L) {
            return false
        }
        v = v xor mask
        if (v == 0L) {
            map.remove(key)
        } else {
            map[key] = v
        }
        --count
        return true
    }

    override fun iterator(): LongIterator {
        return object : LongIterator {

            private val longs = toLongArray()
            private var i = 0

            override fun next(): Long {
                return nextLong()
            }

            override fun remove() {
                remove(longs[i - 1])
            }

            override fun hasNext(): Boolean {
                return i < longs.size
            }

            override fun nextLong(): Long {
                return longs[i++]
            }
        }
    }

    override fun toLongArray(): LongArray {
        return LongArray(count).apply {
            var i = 0
            map.forEach {
                val base = it.key * LONG_BITS
                var mask = 1L
                for (j in 0 until LONG_BITS.toInt()) {
                    if (it.value and mask != 0L) {
                        this[i++] = base + j
                    }
                    mask = mask shl 1
                }
            }
        }
    }

    companion object {

        private val Long.key: Long get() = this / LONG_BITS

        private val Long.bit: Int get() = (this % LONG_BITS).toInt()
    }
}