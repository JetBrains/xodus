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

import jetbrains.exodus.util.MathUtil
import java.util.*

class HashSet<E> @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractSet<E>() {
    private var table: Array<Entry<E>?>
    private var capacity = 0
    private override var size = 0
    private var mask = 0
    private var holdsNull = false

    init {
        init(capacity)
    }

    constructor(collection: Collection<E>) : this(collection.size) {
        this.addAll(collection)
    }

    override operator fun contains(key: Any): Boolean {
        if (key == null) {
            return holdsNull
        }
        val table = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        var e = table[index]
        while (e != null) {
            val entryKey: E
            if (e.key.also { entryKey = it } === key || entryKey == key) {
                return true
            }
            e = e.hashNext
        }
        return false
    }

    override fun add(key: E): Boolean {
        if (key == null) {
            val wasHoldingNull = holdsNull
            holdsNull = true
            if (!wasHoldingNull) {
                size += 1
            }
            return !wasHoldingNull
        }
        val table = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                val entryKey: E
                if (e.key.also { entryKey = it } === key || entryKey == key) {
                    return false
                }
                e = e.hashNext
            }
        }
        val e = Entry<E>(key)
        e.hashNext = table[index]
        table[index] = e
        size += 1
        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return true
    }

    override fun remove(key: Any): Boolean {
        if (key == null) {
            val wasHoldingNull = holdsNull
            holdsNull = false
            if (wasHoldingNull) {
                size -= 1
            }
            return wasHoldingNull
        }
        val table = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        var e = table[index] ?: return false
        var entryKey: E
        if (e.key.also { entryKey = it } === key || entryKey == key) {
            table[index] = e.hashNext
        } else {
            while (true) {
                val last = e
                e = e.hashNext
                if (e == null) return false
                if (e.key.also { entryKey = it } === key || entryKey == key) {
                    last.hashNext = e.hashNext
                    break
                }
            }
        }
        size -= 1
        return true
    }

    override fun iterator(): MutableIterator<E> {
        return object : HashSetIterator<E>() {
            override operator fun next(): E {
                return nextEntry().key
            }
        }
    }

    override fun size(): Int {
        return size
    }

    private fun allocateTable(length: Int) {
        table = arrayOfNulls<Entry<*>?>(length)
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
        holdsNull = false
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            val entries: Iterator<Entry<E>> = RehashIterator()
            allocateTable(length)
            val table = table
            val mask = mask
            while (entries.hasNext()) {
                val e = entries.next()
                val index = HashUtil.indexFor(e.key.hashCode(), length, mask)
                e.hashNext = table[index]
                table[index] = e
            }
        }
    }

    private inner class RehashIterator : HashSetIterator<Entry<E>?>() {
        override operator fun next(): Entry<E> {
            return nextEntry()
        }
    }

    private class Entry<E> {
        val key: E?
        var hashNext: Entry<E>?

        constructor() {
            key = null
            hashNext = null
        }

        constructor(key: E) {
            this.key = key
            hashNext = null
        }
    }

    private abstract inner class HashSetIterator<T> internal constructor() : MutableIterator<T> {
        private val table = this@HashSet.table
        private var index = 0
        private var e: Entry<E>? = null
        private var last: Entry<E>? = null
        private var holdsNull = this@HashSet.holdsNull

        init {
            initNextEntry()
        }

        override fun hasNext(): Boolean {
            return e != null
        }

        override fun remove() {
            checkNotNull(last)
            this@HashSet.remove(last!!.key)
            last = null
        }

        protected fun nextEntry(): Entry<E>? {
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
            val length = table.size
            while (result == null && index < length) {
                result = table[index++]
            }
            if (result == null && holdsNull) {
                holdsNull = false
                result = Entry()
            }
            e = result
        }
    }
}
