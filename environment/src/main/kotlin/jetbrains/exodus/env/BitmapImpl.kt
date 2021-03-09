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

private typealias Bit = Long

open class BitmapImpl(private val store: StoreImpl) : Bitmap {

    override fun getEnvironment() = store.environment

    override fun get(txn: Transaction, bit: Bit): Boolean {
        val key = bit.ensureNonNegative().key
        val bitmapEntry = store.get(txn, longToCompressedEntry(key)) ?: return false
        val mask = 1L shl bit.index
        return entryToLong(bitmapEntry) and mask != 0L
    }

    override fun set(txn: Transaction, bit: Bit, value: Boolean): Boolean {
        val key = bit.ensureNonNegative().key
        val keyEntry = longToCompressedEntry(key)
        val mask = 1L shl bit.index
        val bitmap = store.get(txn, keyEntry)?.let { entryToLong(it) } ?: 0L
        val prevValue = bitmap and mask != 0L
        if (prevValue == value) return false
        (bitmap xor mask).let {
            if (it == 0L) {
                store.delete(txn, keyEntry)
            } else {
                store.put(txn, keyEntry, longToEntry(it))
            }
        }
        return true
    }

    override fun clear(txn: Transaction, bit: Bit): Boolean = this.set(txn, bit, false)

    override fun iterator(txn: Transaction): BitmapIterator = BitmapIterator(txn, store)

    override fun reverseIterator(txn: Transaction): BitmapIterator = BitmapIterator(txn, store, -1)
}

internal fun Bit.ensureNonNegative() =
        if (this < 0L) {
            throw IllegalArgumentException("Bit number should be non-negative")
        } else {
            this
        }

internal val Bit.key: Bit get() = this shr 6

internal val Bit.index: Int get() = (this and 63).toInt()