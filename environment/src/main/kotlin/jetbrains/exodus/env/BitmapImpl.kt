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

    override fun getEnvironment() = store.environment

    override fun get(txn: Transaction, bit: Long): Boolean {
        val keyBit = getKey(bit)
        val storedBitmap = store.get(txn, signedLongToCompressedEntry(keyBit)) ?: return false
        val bitIndex = getBitIndex(bit)
        return compressedEntryToSignedLong(storedBitmap).shr(bitIndex).and(1L) == 1L
    }

    override fun set(txn: Transaction, bit: Long, value: Boolean): Boolean {
        val prevValue = this.get(txn, bit)
        if (prevValue != value) {
            val key = getKey(bit)
            val bitIndex = getBitIndex(bit)
            val storedBitmap = store.get(txn, signedLongToCompressedEntry(key)) ?: longToCompressedEntry(0L)
            val modifiedBitmap = if (value) {
                compressedEntryToSignedLong(storedBitmap).or(1L.shl(bitIndex))
            } else {
                compressedEntryToSignedLong(storedBitmap).and(Long.MAX_VALUE - 1L.shl(bitIndex))
            }
            store.put(txn, signedLongToCompressedEntry(key), signedLongToCompressedEntry(modifiedBitmap))

            return true
        }

        return false
    }

    override fun clear(txn: Transaction, bit: Long): Boolean = this.set(txn, bit, false)

    private fun getKey(bit: Long): Long = bit.shr(6)

    private fun getBitIndex(bit: Long): Int = (bit % 64).toInt()

}