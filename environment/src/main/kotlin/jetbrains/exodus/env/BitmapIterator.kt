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

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.LongBinding.compressedEntryToLong
import jetbrains.exodus.bindings.LongBinding.entryToLong
import jetbrains.exodus.core.dataStructures.hash.LongIterator
import java.util.*

class BitmapIterator(val txn: Transaction, var store: StoreImpl) : LongIterator {

    private val cursor = store.openCursor(txn)
    private var current: Long? = null
    private var next: Long? = null
    private var key = 0L
    private var value = 0L

    override fun remove() {
        if (current == null) {
            throw IllegalStateException()
        }

        val compressedKey = LongBinding.longToCompressedEntry(current!!.shr(6))
        val bitIndex = (current!! % 64).toInt()
        val storedBitmap = store.get(txn, compressedKey)
        val storedBitmapLong = if (storedBitmap == null) 0L else entryToLong(storedBitmap)
        val modifiedBitmap = storedBitmapLong.xor(1L.shl(bitIndex))
        if (modifiedBitmap == 0L) {
            store.delete(txn, compressedKey)
        } else {
            store.put(txn, compressedKey, LongBinding.longToEntry(modifiedBitmap))
        }
        current = null
    }

    override fun hasNext(): Boolean {
        if (next != null) {
            return true
        }

        setNext()
        if (next == null) {
            cursor.close()
        }

        return next != null
    }

    override fun next(): Long = nextLong()

    override fun nextLong(): Long {
        if (hasNext()) {
            current = next
            setNext()
            return current!!
        }

        throw NoSuchElementException()
    }

    private fun setNext() {
        while (value == 0L && cursor.next) {
            key = compressedEntryToLong(cursor.key)
            value = entryToLong(cursor.value)
        }

        if (value != 0L) {
            val ind = smallestBitIndex(value)
            next = key * 64 + ind
            value -= 1L.shl(ind)
        } else {
            next = null
        }
    }

    private fun smallestBitIndex(n: Long): Int {
        for(i in 0..62) {
            if (n % 1L.shl(i + 1) != 0L) {
                return i
            }
        }
        return 63
    }
}