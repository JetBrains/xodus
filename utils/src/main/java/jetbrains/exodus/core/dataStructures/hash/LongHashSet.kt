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

class LongHashSet @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractSet<Long?>(), LongSet {
    private var table: Array<Entry?>
    private var capacity = 0
    private override var size = 0
    private var mask = 0

    init {
        init(capacity)
    }

    constructor(source: LongSet) : this(source.size) {
        for (element in source) {
            add(element)
        }
    }

    constructor(source: Collection<Long>) : this(source.size) {
        for (element in source) {
            add(element)
        }
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
        size -= 1
        return true
    }

    override fun remove(key: Any): Boolean {
        return remove(key as Long)
    }

    override fun iterator(): LongIterator {
        return HashSetIterator()
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
        this.capacity = capacity
        size = 0
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            val entries: Iterator<Entry> = RehashIterator()
            allocateTable(length)
            val table = table
            val mask = mask
            while (entries.hasNext()) {
                val e = entries.next()
                val index = HashUtil.indexFor(e.key, length, mask)
                e.hashNext = table[index]
                table[index] = e
            }
        }
    }

    private inner class RehashIterator : AbstractHashSetIterator<Entry?>() {
        override fun next(): Entry {
            return nextEntry()!!
        }
    }

    private class Entry(val key: Long) {
        val hashNext: Entry? = null
    }

    private abstract inner class AbstractHashSetIterator<T> internal constructor() : MutableIterator<T> {
        private val table = this@LongHashSet.table
        private var index = 0
        private var e: Entry? = null
        private var last: Entry? = null

        init {
            initNextEntry()
        }

        override fun hasNext(): Boolean {
            return e != null
        }

        override fun remove() {
            checkNotNull(last)
            this@LongHashSet.remove(last!!.key)
            last = null
        }

        protected fun nextEntry(): Entry? {
            last = e
            val result = last
            initNextEntry()
            return result
        }

        private fun initNextEntry() {
            var result = e
            if (result != null) {
                result = result.hashNext
            }
            val table = this.table
            while (result == null && index < table.size) {
                result = table[index++]
            }
            e = result
        }
    }

    private inner class HashSetIterator : AbstractHashSetIterator<Long?>(), LongIterator {
        override fun next(): Long {
            return nextEntry()!!.key
        }

        override fun nextLong(): Long {
            return nextEntry()!!.key
        }
    }
}
