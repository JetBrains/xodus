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
package jetbrains.exodus.core.dataStructures.hash

import jetbrains.exodus.util.*
import java.util.*

class LongLinkedHashSet @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractSet<Long?>(), LongSet {
    private var table: Array<Entry?>
    private var top: Entry? = null
    private var back: Entry? = null
    private var capacity = 0
    private override var size = 0
    private var mask = 0

    init {
        init(capacity)
    }

    override fun size(): Int {
        return size
    }

    override fun toLongArray(): LongArray {
        if (size == 0) return LongSet.Companion.EMPTY_ARRAY
        val result = LongArray(size)
        var i = 0
        val itr = iterator()
        while (itr.hasNext()) {
            result[i++] = itr.nextLong()
        }
        return result
    }

    override fun contains(key: Long): Boolean {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        var e = table[index]
        while (e != null) {
            if (e.key == key) {
                return true
            }
            e = e.hashNext
        }
        return false
    }

    override operator fun contains(key: Any): Boolean {
        return contains(key as Long)
    }

    override fun add(key: Long): Boolean {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                if (e.key == key) {
                    return false
                }
                e = e.hashNext
            }
        }
        val e = Entry(key)
        e.hashNext = table[index]
        table[index] = e
        val top = top
        e.next = top
        if (top != null) {
            top.previous = e
        } else {
            back = e
        }
        this.top = e
        size += 1
        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return true
    }

    override fun add(key: Long): Boolean {
        return add(key)
    }

    override fun remove(key: Long): Boolean {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        var e = table[index] ?: return false
        if (e.key == key) {
            table[index] = e.hashNext
        } else {
            while (true) {
                val last = e
                e = e.hashNext
                if (e == null) return false
                if (e.key == key) {
                    last.hashNext = e.hashNext
                    break
                }
            }
        }
        unlink(e)
        size -= 1
        return true
    }

    override fun remove(key: Any): Boolean {
        return remove(key as Long)
    }

    override fun iterator(): LongIterator {
        return LinkedHashIterator()
    }

    private fun allocateTable(length: Int) {
        table = arrayOfNulls(length)
        mask = (1 shl MathUtil.integerLogarithm(table.size)) - 1
    }

    private fun init(capacity: Int) {
        var capacity = capacity
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY
        }
        allocateTable(HashUtil.getCeilingPrime((capacity / loadFactor).toInt()))
        back = null
        top = back
        this.capacity = capacity
        size = 0
    }

    private fun unlink(e: Entry?) {
        val prev = e!!.previous
        val next = e.next
        if (prev != null) {
            prev.next = next
        } else {
            top = next
        }
        if (next != null) {
            next.previous = prev
        } else {
            back = prev
        }
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            allocateTable(length)
            val table = table
            val mask = mask
            var e = back
            while (e != null) {
                val index = HashUtil.indexFor(e.key, length, mask)
                e.hashNext = table[index]
                table[index] = e
                e = e.previous
            }
        }
    }

    private class Entry(val key: Long) {
        val next: Entry? = null
        val previous: Entry? = null
        val hashNext: Entry? = null
    }

    private inner class LinkedHashIterator private constructor() : LongIterator {
        private var e: Entry?
        private var last: Entry? = null

        init {
            e = back
        }

        override fun hasNext(): Boolean {
            return e != null
        }

        override fun remove() {
            checkNotNull(last)
            this@LongLinkedHashSet.remove(last!!.key)
            last = null
        }

        override fun next(): Long {
            return nextLong()
        }

        override fun nextLong(): Long {
            last = e
            val result = last
            e = result!!.previous
            return result.key
        }
    }
}
