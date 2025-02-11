/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.LongBinding.compressedEntryToLong
import jetbrains.exodus.core.dataStructures.hash.LongIterator
import jetbrains.exodus.env.BitmapImpl.Companion.asEntry
import jetbrains.exodus.env.BitmapImpl.Companion.asLong
import java.io.Closeable

class BitmapIterator(
    val txn: Transaction,
    var store: StoreImpl,
    private val direction: Int = 1
) : LongIterator, Closeable {

    val cursor: Cursor = store.openCursor(txn)

    // We must use currentSet bit, as values may reach the -1L
    private var current: Long = -1L
    private var currentSet: Boolean = false

    // We must use nextSet bit, as values may reach the -1L
    private var next: Long = -1L
    private var nextSet: Boolean = false

    private var key = 0L
    private var value = 0L
    private var bitIndex = 1

    init {
        if (direction != 1 && direction != -1) {
            throw IllegalArgumentException("direction can only be 1 or -1")
        }
    }

    override fun remove() {
        if (currentSet) {
            val current = current
            val keyEntry = LongBinding.longToCompressedEntry(current.key)
            val bitmap = store.get(txn, keyEntry)?.asLong ?: 0L
            (bitmap xor (1L shl current.index)).let {
                if (it == 0L) {
                    store.delete(txn, keyEntry)
                } else {
                    store.put(txn, keyEntry, it.asEntry)
                }
            }

            this.current = -1L
            this.currentSet = false

            return
        }
        throw IllegalStateException()
    }

    override fun hasNext(): Boolean {
        if (!nextSet) {
            setNext()
        }
        return nextSet
    }

    override fun next(): Long = nextLong()

    override fun nextLong(): Long {
        if (hasNext()) {
            // hasNext implies nextSet
            current = next
            currentSet = true

            val prev = next
            setNext()
            return prev
        }
        throw NoSuchElementException()
    }

    override fun close() = cursor.close()

    private fun setNext() {
        while (value == 0L && if (direction == 1) cursor.next else cursor.prev) {
            key = compressedEntryToLong(cursor.key)
            value = cursor.value.asLong
            bitIndex = if (direction == 1) 0 else 63
        }

        if (value != 0L) {
            setNextBitIndex()

            next = (key shl 6) + bitIndex
            nextSet = true

            value -= 1L shl bitIndex
        } else {
            next = -1L
            nextSet = false

            cursor.close()
        }
    }

    private fun setNextBitIndex() {
        while (value and 1L.shl(bitIndex) == 0L) {
            bitIndex += direction
        }
    }

    fun getSearchBit(bit: Long): Boolean {
        val searchKey = bit.key
        val searchIndex = bit.index
        if (this.getSearchKey(searchKey)) {
            val navigatedKey = compressedEntryToLong(cursor.key)
            key = navigatedKey
            value = cursor.value.asLong
            bitIndex = if (navigatedKey != searchKey) if (direction > 0) 0 else 63
            else {
                if (direction > 0) {
                    // clear lower bits
                    value = (value shr searchIndex) shl searchIndex
                } else {
                    // clear higher bits
                    val shiftBits = Long.SIZE_BITS - searchIndex - 1
                    value = (value shl shiftBits) ushr shiftBits
                }
                searchIndex
            }
            setNext()
            return true
        }
        return false
    }

    private fun getSearchKey(searchKey: Long): Boolean {
        var navigated = cursor.getSearchKeyRange(LongBinding.longToCompressedEntry(searchKey)) != null
        if (direction < 0 && compressedEntryToLong(cursor.key) != searchKey) {
            navigated = cursor.prev
        }
        return navigated
    }
}