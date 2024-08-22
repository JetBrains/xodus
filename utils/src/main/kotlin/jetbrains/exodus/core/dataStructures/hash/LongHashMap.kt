/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
import java.lang.Integer.max

class LongHashMap<V> @JvmOverloads constructor(capacity: Int = 0, private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR) : AbstractHashMap<Long, V>() {

    private var table: Array<Entry<V>?> = emptyArray()
    private var capacity = 0
    private var mask = 0

    init {
        init(capacity)
    }

    override fun get(key: Long): V? = getEntry(key)?.value

    override fun put(key: Long, value: V): V? {
        table.let { table ->
            val index = HashUtil.indexFor(key, table.size, mask)
            var e = table[index]
            while (e != null) {
                if (e.key == key) {
                    return e.setValue(value)
                }
                e = e.hashNext
            }
            Entry(key, value).let { newEntry ->
                newEntry.hashNext = table[index]
                table[index] = newEntry
            }
            _size += 1
            if (_size > capacity) {
                rehash(HashUtil.nextCapacity(capacity))
            }
            return null
        }
    }

    override fun containsKey(key: Long) = getEntry(key) != null

    override fun remove(key: Long): V? {
        table.let { table ->
            val index = HashUtil.indexFor(key, table.size, mask)
            var e: Entry<V> = table[index] ?: return null
            var last: Entry<V>? = null
            while (key != e.key) {
                last = e
                e = e.hashNext ?: return null
            }
            _size -= 1
            if (last == null) {
                table[index] = e.hashNext
            } else {
                last.hashNext = e.hashNext
            }
            return e.value
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun getEntry(key: Any): MutableMap.MutableEntry<Long, V>? {
        return getEntry((key as Long).toLong()) as MutableMap.MutableEntry<Long, V>?
    }

    override fun init(capacity: Int) {
        max(capacity, HashUtil.MIN_CAPACITY).let { c ->
            allocateTable(HashUtil.getCeilingPrime((c / loadFactor).toInt()))
            this.capacity = c
            this._size = 0
        }
    }

    override fun hashIterator(): HashMapIterator {
        return HashIterator()
    }

    private fun getEntry(key: Long): Entry<V>? {
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
        table = arrayOfNulls<Entry<V>?>(length)
        mask = (1 shl MathUtil.integerLogarithm(table.size)) - 1
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            val entries: Iterator<Map.Entry<Long, V>> = entries.iterator()
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

    private class Entry<V>(override val key: Long, override var value: V) : MutableMap.MutableEntry<Long?, V> {

        var hashNext: Entry<V>? = null

        override fun setValue(newValue: V) = value.also { value = newValue }
    }

    private inner class HashIterator internal constructor() : HashMapIterator() {

        private val table = this@LongHashMap.table
        private var index = 0
        private var e: Entry<V>? = null
        private var last: Entry<V>? = null

        init {
            initNextEntry()
        }

        public override fun hasNext() = e != null

        public override fun remove() {
            this@LongHashMap.remove(checkNotNull(last).key)
            last = null
        }

        override fun nextEntry() = checkNotNull(e).also {
            last = it
            initNextEntry()
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
}