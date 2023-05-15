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

open class LongLinkedHashMap<V> @JvmOverloads constructor(
    capacity: Int = 0,
    private val loadFactor: Float = HashUtil.DEFAULT_LOAD_FACTOR
) : AbstractHashMap<Long, V>() {
    private var table: Array<Entry<V>?>
    private var top: Entry<V>? = null
    private var back: Entry<V>? = null
    private var capacity = 0
    private var mask = 0

    init {
        init(capacity)
    }

    fun get(key: Long): V? {
        val e = getEntry(key)
        return e?.value
    }

    fun put(key: Long, value: V): V? {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        run {
            var e = table[index]
            while (e != null) {
                if (e.key == key) {
                    moveToTop(e)
                    return e.setValue(value)
                }
                e = e.hashNext
            }
        }
        val e = Entry(key, value)
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
            remove(back!!.key)
        } else if (internalSize > capacity) {
            rehash(HashUtil.nextCapacity(capacity))
        }
        return null
    }

    override fun put(key: Long, value: V): V? {
        return put(key, value)
    }

    fun containsKey(key: Long): Boolean {
        return getEntry(key) != null
    }

    fun remove(key: Long): V? {
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
        unlink(e)
        internalSize -= 1
        return e.value
    }

    override fun remove(key: Any): V? {
        return remove(key as Long)
    }

    protected open fun removeEldestEntry(eldest: Map.Entry<Long, V>?): Boolean {
        return false
    }

    override fun getEntry(key: Any?): Map.Entry<Long, V>? {
        return getEntry(key as Long?)
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

    private fun getEntry(key: Long): Entry<V>? {
        val table = table
        val index = HashUtil.indexFor(key, table.size, mask)
        var e = table[index]
        while (e != null) {
            if (e.key == key) {
                moveToTop(e)
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

    private fun moveToTop(e: Entry<V>) {
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

    private fun unlink(e: Entry<V>?) {
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

    private class Entry<V>(override val key: Long, override var value: V) : MutableMap.MutableEntry<Long?, V> {
        val next: Entry<V>? = null
        val previous: Entry<V>? = null
        val hashNext: Entry<V>? = null
        override fun setValue(value: V): V {
            val result = this.value
            this.value = value
            return result
        }
    }

    private inner class HashIterator : HashMapIterator() {
        private var e = top
        private var last: Entry<V>? = null
        public override fun hasNext(): Boolean {
            return e != null
        }

        public override fun remove() {
            checkNotNull(last)
            this@LongLinkedHashMap.remove(last!!.key)
            last = null
        }

        override fun nextEntry(): Entry<V>? {
            last = e
            val result = last
            e = result!!.next
            return result
        }
    }
}