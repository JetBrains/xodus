/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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

private const val LONG_BITS: Int = SIZE
private const val LONG_BITS_LOG: Int = 6

class PackedLongHashSet : AbstractSet<Long>(), LongSet {

    private val map = LongHashMap<BitsHolder>(20)
    private var count: Int = 0

    override val size: Int
        get() {
            return count
        }

    override fun contains(element: Long): Boolean {
        val v = map[element.key]
        return v != null && v.bits and masks[element.bit] != 0L
    }

    override fun add(element: Long): Boolean {
        val key = element.key
        val bit = element.bit
        val mask = masks[bit]
        val v = map[key]
        if (v == null) {
            map[key] = BitsHolder(mask)
            ++count
            return true
        }
        if (v.bits and mask != 0L) {
            return false
        }
        v.bits = v.bits xor mask
        ++count
        return true
    }

    override fun remove(element: Long): Boolean {
        val key = element.key
        val bit = element.bit
        val mask = masks[bit]
        val v = map[key] ?: return false
        val bits = v.bits
        if (bits and mask == 0L) {
            return false
        }
        v.bits = bits xor mask
        if (v.bits == 0L) {
            map.remove(key)
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
                val base = it.key shl LONG_BITS_LOG
                val value = it.value.bits
                @Suppress("LoopToCallChain")
                for (j in 0 until LONG_BITS) {
                    if (value and masks[j] != 0L) {
                        this[i++] = base + j
                    }
                }
            }
        }
    }

    private class BitsHolder(var bits: Long = 0L)

    companion object {

        private val masks = LongArray(LONG_BITS).apply {
            var mask = 1L
            for (i in 0 until LONG_BITS) {
                this[i] = mask
                mask = mask shl 1
            }
        }

        private val Long.key: Long get() = this shr LONG_BITS_LOG

        private val Long.bit: Int get() = (this and (LONG_BITS - 1).toLong()).toInt()
    }
}