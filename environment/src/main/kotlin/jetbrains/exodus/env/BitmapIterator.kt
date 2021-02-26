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
        current?.let { current ->
            val keyEntry = LongBinding.longToCompressedEntry(current.key)
            val bitmap = store.get(txn, keyEntry)?.let { entryToLong(it) } ?: 0L
            (bitmap xor (1L shl current.index)).let {
                if (it == 0L) {
                    store.delete(txn, keyEntry)
                } else {
                    store.put(txn, keyEntry, LongBinding.longToEntry(it))
                }
            }
            this.current = null
            return
        }
        throw IllegalStateException()
    }

    override fun hasNext(): Boolean {
        if (next == null) {
            setNext()
        }
        return next != null
    }

    override fun next(): Long = nextLong()

    override fun nextLong(): Long {
        if (hasNext()) {
            current = next
            next?.also {
                setNext()
                return it
            }
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
            next = (key shl 6) + ind
            value -= 1L shl ind
        } else {
            next = null
            cursor.close()
        }
    }

    private fun smallestBitIndex(n: Long): Int {
        var bits = n
        for (i in 0..62) {
            if (bits and 1L == 1L) return i
            bits = bits shr 1
        }
        return 63
    }
}