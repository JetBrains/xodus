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
import java.io.Serializable

open class HashMap<K, V> : AbstractHashMap<K?, V>, Serializable {
    private var table: Array<Entry<K, V>?>? = null

    private var capacity = 0
    private val loadFactor: Float
    private var mask = 0
    private var nullEntry: Entry<K?, V>? = null

    constructor() : this(0, 0, HashUtil.DEFAULT_LOAD_FACTOR, DEFAULT_TABLE_SIZE, DEFAULT_MASK)

    @JvmOverloads
    constructor(capacity: Int, loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR) {
        this.loadFactor = loadFactor

        init(capacity)
    }

    constructor(copy: HashMap<K, V>) : this(copy.capacity, copy.size, copy.loadFactor, copy.table.size, copy.mask) {
        val source = copy.table
        for (i in source.indices) {
            table[i] = copyEntry(source[i])
        }
    }

    protected constructor(capacity: Int, size: Int, loadFactor: Float, tableSize: Int, mask: Int) {
        var currentCapacity = capacity

        this.loadFactor = loadFactor
        if (currentCapacity < HashUtil.MIN_CAPACITY) {
            currentCapacity = HashUtil.MIN_CAPACITY
        }

        table = arrayOfNulls<Entry<*, *>?>(tableSize)
        this.mask = mask
        this.capacity = currentCapacity
        internalSize = size
    }

    override fun put(key: K?, value: V): V? {
        if (key == null) {
            if (nullEntry == null) {
                internalSize += 1
                nullEntry = Entry(null, value)
                return null
            }
            return nullEntry!!.setValue(value)
        }
        val table = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                val entryKey: K
                if (e.key.also { entryKey = it } === key || entryKey == key) {
                    return e.setValue(value)
                }
                e = e.hashNext
            }
        }
        val e = Entry<K, V>(key, value)
        e.hashNext = table[index]
        table[index] = e
        internalSize += 1
        if (internalSize > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return null
    }

    override fun remove(key: Any): V? {
        if (key == null) {
            if (nullEntry != null) {
                internalSize -= 1
                val hadNullValue = nullEntry!!.value
                nullEntry = null
                return hadNullValue
            }
            return null
        }
        val table = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        var e = table[index] ?: return null
        var entryKey: K
        if (e.key.also { entryKey = it } === key || entryKey == key) {
            table[index] = e.hashNext
        } else {
            while (true) {
                val last = e
                e = e.hashNext
                if (e == null) return null
                if (e.key.also { entryKey = it } === key || entryKey == key) {
                    last.hashNext = e.hashNext
                    break
                }
            }
        }
        internalSize -= 1
        return e.value
    }

    override fun getEntry(key: Any?): Map.Entry<K?, V>? {
        if (key == null) {
            return nullEntry
        }
        val table: Array<Entry<K?, V>?> = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        var e = table[index]
        while (e != null) {
            val entryKey: K
            if (e.key.also { entryKey = it } === key || entryKey == key) {
                return e
            }
            e = e.hashNext
        }
        return null
    }

   final override fun init(capacity: Int) {
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

    private fun allocateTable(length: Int) {
        table = arrayOfNulls<Entry<*, *>?>(length)
        mask = (1 shl MathUtil.integerLogarithm(table.size)) - 1
    }

    private fun rehash(capacity: Int) {
        val length = HashUtil.getCeilingPrime((capacity / loadFactor).toInt())
        this.capacity = capacity
        if (length != table.size) {
            val entries: Iterator<Map.Entry<K?, V>> = entries.iterator()
            allocateTable(length)
            val table: Array<Entry<K?, V>?> = table
            val mask = mask
            while (entries.hasNext()) {
                val e = entries.next() as Entry<K?, V>
                if (e.key != null) {
                    val index = HashUtil.indexFor(e.key.hashCode(), length, mask)
                    e.hashNext = table[index]
                    table[index] = e
                }
            }
        }
    }

    private fun copyEntry(sourceEntry: Entry<K, V>?): Entry<K, V>? {
        return if (sourceEntry == null) null else Entry(
            sourceEntry.key,
            sourceEntry.value,
            copyEntry(sourceEntry.hashNext)
        )
    }

    private class Entry<K, V> : MutableMap.MutableEntry<K, V>, Serializable {
        override val key: K
        override var value: V
            private set
        var hashNext: Entry<K, V>? = null

        constructor(key: K, value: V) {
            this.key = key
            this.value = value
        }

        constructor(key: K, value: V, hashNext: Entry<K, V>?) {
            this.key = key
            this.value = value
            this.hashNext = hashNext
        }

        override fun setValue(value: V): V {
            val result = this.value
            this.value = value
            return result
        }
    }

    private inner class HashIterator internal constructor() : HashMapIterator() {
        private val table = this@HashMap.table
        private var index = -1
        private var e: Entry<K?, V>? = null
        private var last: Entry<K?, V>? = null

        init {
            initNextEntry()
        }

        override fun hasNext(): Boolean {
            return e != null
        }

        override fun remove() {
            checkNotNull(last)
            this@HashMap.remove(last!!.key)
            last = null
        }

        override fun nextEntry(): Entry<K?, V>? {
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
            if (index < 0) {
                result = nullEntry
                index = 0
            }
            val table: Array<Entry<K?, V>?> = this.table
            while (result == null && index < table.size) {
                result = table[index++]
            }
            e = result
        }
    }

    companion object {
        protected const val DEFAULT_TABLE_SIZE = 3
        protected const val DEFAULT_MASK = 3
    }
}
