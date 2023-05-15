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

open class LinkedHashMap<K, V> @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractHashMap<K?, V>() {
    private var table: Array<Entry<K, V>?>
    private var top: Entry<K?, V>? = null
    private var back: Entry<K?, V>? = null
    private var capacity = 0
    private var mask = 0

    init {
        init(capacity)
    }

    override fun put(key: K, value: V): V? {
        val table: Array<Entry<K?, V>?> = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                val entryKey: K
                if (e.key.also { entryKey = it } === key || entryKey == key) {
                    moveToTop(e)
                    return e.setValue(value)
                }
                e = e.hashNext
            }
        }
        val e = Entry<K?, V>(key, value)
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
        internalSize += 1
        if (removeEldestEntry(back)) {
            remove(eldestKey())
        } else if (internalSize > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return null
    }

    override fun remove(key: Any): V? {
        val table: Array<Entry<K?, V>?> = table
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
        unlink(e)
        internalSize -= 1
        return e.value
    }

    fun removeEldest(): V? {
        return remove(eldestKey())
    }

    private fun eldestKey(): K? {
        return back!!.key
    }

    protected open fun removeEldestEntry(eldest: Map.Entry<K?, V>?): Boolean {
        return false
    }

    override fun getEntry(key: Any?): Map.Entry<K?, V>? {
        val table: Array<Entry<K?, V>?> = table
        val hash = key.hashCode()
        val index = HashUtil.indexFor(hash, table.size, mask)
        var e = table[index]
        while (e != null) {
            val entryKey: K
            if (e.key.also { entryKey = it } === key || entryKey == key) {
                moveToTop(e)
                return e
            }
            e = e.hashNext
        }
        return null
    }

    override fun init(capacity: Int) {
        var capacity = capacity
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY
        }
        allocateTable(HashUtil.getCeilingPrime((capacity / loadFactor).toInt()))
        back = null
        top = back
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

    private fun moveToTop(e: Entry<K?, V>) {
        val top = top
        if (top !== e) {
            val prev = e.previous
            val next = e.next
            prev!!.next = next
            if (next != null) {
                next.previous = prev
            } else {
                back = prev
            }
            top!!.previous = e
            e.next = top
            e.previous = null
            this.top = e
        }
    }

    private fun unlink(e: Entry<K?, V>?) {
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
            val table: Array<Entry<K?, V>?> = table
            val mask = mask
            var e = back
            while (e != null) {
                val index = HashUtil.indexFor(e.key.hashCode(), length, mask)
                e.hashNext = table[index]
                table[index] = e
                e = e.previous
            }
        }
    }

    private class Entry<K, V>(override val key: K, override var value: V) : MutableMap.MutableEntry<K, V> {
        val next: Entry<K, V>? = null
        val previous: Entry<K, V>? = null
        val hashNext: Entry<K, V>? = null
        override fun setValue(value: V): V {
            val result = this.value
            this.value = value
            return result
        }
    }

    private inner class HashIterator : HashMapIterator() {
        private var e = top
        private var last: Entry<K?, V>? = null
        public override fun hasNext(): Boolean {
            return e != null
        }

        public override fun remove() {
            checkNotNull(last)
            this@LinkedHashMap.remove(last!!.key)
            last = null
        }

        override fun nextEntry(): Entry<K?, V>? {
            last = e
            val result = last
            e = result!!.next
            return result
        }
    }
}