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

import jetbrains.exodus.bindings.LongBinding.*

open class BitmapImpl(private val store: StoreImpl) : Bitmap {

    private var bitmapIterator: BitmapIterator? = null

    override fun getEnvironment() = store.environment

    override fun get(txn: Transaction, bit: Long): Boolean {
        if (bit < 0) {
            throw IllegalArgumentException("Bit number should be non-negative")
        }
        val key = getKey(bit)
        val storedBitmap = store.get(txn, longToCompressedEntry(key)) ?: return false
        val bitIndex = getBitIndex(bit)
        return entryToLong(storedBitmap).shr(bitIndex).and(1L) == 1L
    }

    override fun set(txn: Transaction, bit: Long, value: Boolean): Boolean {
        if (bit < 0) {
            throw IllegalArgumentException("Bit number should be non-negative")
        }
        val key = getKey(bit)
        val bitIndex = getBitIndex(bit)
        val storedBitmap = store.get(txn, longToCompressedEntry(key))
        val storedBitmapLong = if (storedBitmap == null) 0L else entryToLong(storedBitmap)

        val prevValue = storedBitmapLong.shr(bitIndex).and(1L) == 1L
        if (prevValue != value) {
            val modifiedBitmap = if (value) {
                storedBitmapLong.or(1L.shl(bitIndex))
            } else {
                storedBitmapLong.xor(1L.shl(bitIndex))
            }

            if (modifiedBitmap == 0L) {
                store.delete(txn, longToCompressedEntry(key))
            } else {
                store.put(txn, longToCompressedEntry(key), longToEntry(modifiedBitmap))
            }

            return true
        }

        return false
    }

    override fun clear(txn: Transaction, bit: Long): Boolean = this.set(txn, bit, false)

    private fun getKey(bit: Long): Long = bit.shr(6)

    private fun getBitIndex(bit: Long): Int = (bit % 64).toInt()

    override fun iterator(txn: Transaction): BitmapIterator {
        bitmapIterator = BitmapIterator(txn, store)
        return bitmapIterator as BitmapIterator
    }
}