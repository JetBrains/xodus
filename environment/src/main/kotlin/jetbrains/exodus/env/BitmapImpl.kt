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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.LongBinding.compressedEntryToLong
import jetbrains.exodus.core.dataStructures.hash.LongHashMap

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
        store.openCursor(txn).use { cursor ->
            var count = 0L
            cursor.forEach {
                count += value.countBits
            }
            return count
        }
    }

    override fun count(txn: Transaction, firstBit: Bit, lastBit: Bit): Long {
        if (firstBit > lastBit) throw IllegalArgumentException("firstBit > lastBit")
        if (firstBit == lastBit) {
            return if (get(txn, firstBit)) 1L else 0L
        }
        store.openCursor(txn).use { cursor ->
            val firstKey = firstBit.ensureNonNegative().key
            val lastKey = lastBit.ensureNonNegative().key
            val keyEntry = LongBinding.longToCompressedEntry(firstKey)
            val valueEntry = cursor.getSearchKeyRange(keyEntry) ?: return 0L
            var count = 0L
            val key = compressedEntryToLong(cursor.key)
            if (key in (firstKey + 1) until lastKey) {
                count += valueEntry.countBits
            } else {
                val bits = valueEntry.asLong
                val lowBit = if (key == firstKey) firstBit.index else 0
                val highBit = if (key == lastKey) lastBit.index else Long.SIZE_BITS - 1
                for (i in lowBit..highBit) {
                    if (bits and (1L shl i) != 0L) {
                        ++count
                    }
                }
            }
            cursor.forEach {
                val currentKey = compressedEntryToLong(this.key)
                if (currentKey < lastKey) {
                    count += value.countBits
                } else {
                    if (currentKey == lastKey) {
                        val bits = value.asLong
                        for (i in 0..lastBit.index) {
                            if (bits and (1L shl i) != 0L) {
                                ++count
                            }
                        }
                    }
                    return count
                }
            }
            return count
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
                this.iterator().next().unsigned.let { tag ->
                    return when {
                        tag == 0 -> ALL_ONES
                        tag < Long.SIZE_BITS + 1 -> 1L shl (tag - 1)
                        else -> ALL_ONES xor (1L shl (tag - Long.SIZE_BITS - 1))
                    }
                }
            }

        private val ByteIterable.countBits: Int
            get() {
                this.iterator().let {
                    if (length == 1) {
                        return it.next().unsigned.let { tag ->
                            when {
                                tag == 0 -> 64
                                tag < Long.SIZE_BITS + 1 -> 1
                                else -> 63
                            }
                        }
                    }
                    var size = 0
                    size += (it.next().unsigned xor 0x80).countOneBits()
                    while (it.hasNext()) {
                        size += it.next().unsigned.countOneBits()
                    }
                    return size
                }
            }
    }
}

internal val Bit.key: Bit get() = this shr 6

internal val Bit.index: Int get() = (this and 63).toInt()

private val Byte.unsigned: Int get() = this.toInt() and 0xff

private fun Bit.ensureNonNegative() =
    this.also { if (it < 0L) throw IllegalArgumentException("Bit number should be non-negative") }
