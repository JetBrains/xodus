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

open class IntHashMap<V> @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractHashMap<Int, V>() {
    private var table: Array<Entry<V>?>
    private var capacity = 0
    private var mask = 0

    init {
        init(capacity)
    }

    fun get(key: Int): V? {
        val e = getEntry(key)
        return e?.value
    }

    fun put(key: Int, value: V): V? {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                if (e.key == key) {
                    return e.setValue(value)
                }
                e = e.hashNext
            }
        }
        val e = Entry(key, value)
        e.hashNext = table[index]
        table[index] = e
        internalSize += 1
        if (internalSize > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return null
    }

    override fun put(key: Int, value: V): V? {
        return put(key, value)
    }

    fun containsKey(key: Int): Boolean {
        return getEntry(key) != null
    }

    fun remove(key: Int): V? {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        var e = table[index] ?: return null
        if (e.key == key) {
            table[index] = e.hashNext
        } else {
            while (true) {
                val last = e
                e = e.hashNext
                if (e == null) return null
                if (e.key == key) {
                    last.hashNext = e.hashNext
                    break
                }
            }
        }
        internalSize -= 1
        return e.value
    }

    override fun remove(key: Any): V? {
        return remove(key as Int)
    }

    override fun getEntry(key: Any?): Map.Entry<Int, V>? {
        return getEntry(key as Int?)
    }

    override fun init(capacity: Int) {
        var capacity = capacity
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY
        }
        allocateTable(HashUtil.getCeilingPrime((capacity / loadFactor).toInt()))
        this.capacity = capacity
        internalSize = 0
    }

    override fun hashIterator(): HashMapIterator {
        return HashIterator()
    }

    private fun getEntry(key: Int): Entry<V>? {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        var e = table[index]
        while (e != null) {
            if (e.key == key) {
                return e
            }
            e = e.hashNext
        }
        return null
    }

    private fun allocateTable(length: Int) {
        table = arrayOfNulls<Entry<*>?>(length)
        mask = (1 shl MathUtil.integerLogarithm(table.size)) - 1
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            val entries: Iterator<Map.Entry<Int, V>> = entries.iterator()
            allocateTable(length)
            val table = table
            val mask = mask
            while (entries.hasNext()) {
                val e = entries.next() as Entry<V>
                val index = HashUtil.indexFor(e.key, length, mask)
                e.hashNext = table[index]
                table[index] = e
            }
        }
    }

    private class Entry<V>(override val key: Int, override var value: V) : MutableMap.MutableEntry<Int?, V> {
        val hashNext: Entry<V>? = null
        override fun setValue(value: V): V {
            val result = this.value
            this.value = value
            return result
        }
    }

    private inner class HashIterator internal constructor() : HashMapIterator() {
        private val table = this@IntHashMap.table
        private var index = 0
        private var e: Entry<V?>? = null
        private var last: Entry<V?>? = null

        init {
            initNextEntry()
        }

        public override fun hasNext(): Boolean {
            return e != null
        }

        public override fun remove() {
            checkNotNull(last)
            this@IntHashMap.remove(last!!.key)
            last = null
        }

        override fun nextEntry(): MutableMap.MutableEntry<Int, V>? {
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
            val table: Array<Entry<V?>?> = this.table
            while (result == null && index < table.size) {
                result = table[index++]
            }
            e = result
        }
    }
}
