/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import kotlin.experimental.and
import kotlin.experimental.xor

private typealias Bit = Long

open class BitmapImpl(open val store: StoreImpl) : Bitmap {

    override fun getEnvironment() = store.environment

    override fun get(txn: Transaction, bit: Bit): Boolean {
        val key = bit.ensureNonNegative().key
        val bitmapEntry = store.get(txn, LongBinding.longToCompressedEntry(key)) ?: return false
        val mask = 1L shl bit.index
        return bitmapEntry.asLong and mask != 0L
    }

    override fun set(txn: Transaction, bit: Bit, value: Boolean): Boolean {
        val key = bit.ensureNonNegative().key
        val keyEntry = LongBinding.longToCompressedEntry(key)
        val mask = 1L shl bit.index
        val bitmap = store.get(txn, keyEntry)?.asLong ?: 0L
        val prevValue = bitmap and mask != 0L
        if (prevValue == value) return false
        (bitmap xor mask).let {
            if (it == 0L) {
                store.delete(txn, keyEntry)
            } else {
                store.put(txn, keyEntry, it.asEntry)
            }
        }
        return true
    }

    override fun clear(txn: Transaction, bit: Bit): Boolean = this.set(txn, bit, false)

    override fun iterator(txn: Transaction): BitmapIterator = BitmapIterator(txn, store)

    override fun reverseIterator(txn: Transaction): BitmapIterator = BitmapIterator(txn, store, -1)

    override fun getFirst(txn: Transaction): Long {
        return iterator(txn).use {
            if (it.hasNext()) it.next() else -1L
        }
    }

    override fun getLast(txn: Transaction): Long {
        return reverseIterator(txn).use {
            if (it.hasNext()) it.next() else -1L
        }
    }

    override fun count(txn: Transaction): Long {
        store.openCursor(txn).let { cursor ->
            var size = 0L
            while (cursor.next) {
                cursor.value.let {
                    if (it.length == 1) {
                        it.iterator().next().toInt().let { tag ->
                            size += when {
                                tag == 0 -> 64L
                                tag < Long.SIZE_BITS + 1 -> 1L
                                else -> 63L
                            }
                        }
                    } else {
                        size += it.countBits()
                    }
                }
            }
            return size
        }
    }

    companion object {

        private const val ALL_ONES = -1L
        private val SINGLE_ZEROS = LongHashMap<Int>()
        private val SINGLE_ONES = LongHashMap<Int>()

        init {
            var bit = 1L
            for (i in 0..63) {
                SINGLE_ONES[bit] = i
                SINGLE_ZEROS[ALL_ONES xor bit] = i
                bit = bit shl 1
            }
        }

        internal val Long.asEntry: ByteIterable
            get() {
                if (this == ALL_ONES) {
                    return ArrayByteIterable(byteArrayOf(0))
                }
                SINGLE_ONES[this]?.let { bit ->
                    return ArrayByteIterable(byteArrayOf((bit + 1).toByte()))
                }
                SINGLE_ZEROS[this]?.let { bit ->
                    return ArrayByteIterable(byteArrayOf((bit + Long.SIZE_BITS + 1).toByte()))
                }
                return LongBinding.longToEntry(this)
            }

        internal val ByteIterable.asLong: Long
            get() {
                if (this.length != 1) {
                    return LongBinding.entryToLong(this)
                }
                this.iterator().next().toInt().let { tag ->
                    return when {
                        tag == 0 -> ALL_ONES
                        tag < Long.SIZE_BITS + 1 -> 1L shl (tag - 1)
                        else -> ALL_ONES xor (1L shl (tag - Long.SIZE_BITS - 1))
                    }
                }
            }
    }
}

private fun ByteIterable.countBits(): Int {
    var size = 0
    this.iterator().let {
        size += (it.next() xor (0x80).toByte()).countBits()
        while (it.hasNext()) {
            size += it.next().countBits()
        }
    }
    return size
}

private fun Byte.countBits(): Int {
    var size = 0
    var byte = this
    while (byte != 0.toByte()) {
        size += 1
        byte = byte and (byte - 1).toByte()
    }
    return size
}

private fun Bit.ensureNonNegative() =
    this.also { if (it < 0L) throw IllegalArgumentException("Bit number should be non-negative") }

internal val Bit.key: Bit get() = this shr 6

internal val Bit.index: Int get() = (this and 63).toInt()