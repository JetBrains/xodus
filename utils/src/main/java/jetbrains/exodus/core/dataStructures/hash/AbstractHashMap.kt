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

abstract class AbstractHashMap<K, V> : AbstractMutableMap<K, V?>() {
    protected var internalSize = 0
    override val size: Int
        get() {
            return internalSize
        }

    override fun clear() {
        init(0)
    }

    override operator fun get(key: K): V? {
        val e = getEntry(key)
        return e?.value
    }

    override fun containsKey(key: K): Boolean {
        return getEntry(key) != null
    }

    override val keys: MutableSet<K> get() = KeySet()


    override val values: MutableCollection<V?> = Values()


    override val entries: MutableSet<MutableMap.MutableEntry<K, V?>>
        get() {
            return EntrySet()
        }

    fun forEachKey(procedure: ObjectProcedure<K>): Boolean {
        for ((key) in entries) {
            if (!procedure.execute(key)) return false
        }
        return true
    }

    fun forEachValue(procedure: ObjectProcedure<V?>): Boolean {
        for ((_, value) in entries) {
            if (!procedure.execute(value)) return false
        }
        return true
    }

    fun forEachEntry(procedure: ObjectProcedure<Map.Entry<K, V?>?>): Boolean {
        for (entry in entries) {
            if (!procedure.execute(entry)) return false
        }
        return true
    }

    fun <E : Throwable?> forEachEntry(procedure: ObjectProcedureThrows<Map.Entry<K, V?>?, E>): Boolean {
        for (entry in entries) {
            if (!procedure.execute(entry)) return false
        }
        return true
    }

    protected abstract fun getEntry(key: Any?): Map.Entry<K, V>?
    protected abstract fun init(capacity: Int)
    protected abstract inner class HashMapIterator {
        abstract fun nextEntry(): MutableMap.MutableEntry<K, V?>
        abstract operator fun hasNext(): Boolean
        abstract fun remove()
    }

    protected abstract fun hashIterator(): HashMapIterator
    private abstract inner class HashIteratorDecorator<T> protected constructor() : MutableIterator<T> {
        protected val decorated: HashMapIterator = hashIterator()

        override fun hasNext(): Boolean {
            return decorated.hasNext()
        }

        override fun remove() {
            decorated.remove()
        }
    }

    private inner class EntrySet : AbstractMutableSet<MutableMap.MutableEntry<K, V?>>() {
        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V?>> {
            return object : HashIteratorDecorator<MutableMap.MutableEntry<K, V?>>() {
                override fun next(): MutableMap.MutableEntry<K, V?> {
                    return decorated.nextEntry()
                }
            }
        }

        override operator fun contains(element: MutableMap.MutableEntry<K, V?>): Boolean {
            val (key, value) = element
            val leftEntry = getEntry(key)
            return leftEntry != null && leftEntry.value == value
        }

        override fun remove(element: MutableMap.MutableEntry<K, V?>): Boolean {
            val (key) = element
            return this@AbstractHashMap.remove(key) != null
        }

        override val size: Int
            get() {
                return internalSize
            }

        override fun add(element: MutableMap.MutableEntry<K, V?>): Boolean {
            val oldValue = this@AbstractHashMap.put(element.key, element.value)
            return oldValue != element.value
        }

        override fun clear() {
            this@AbstractHashMap.clear()
        }
    }

    private inner class KeySet : AbstractMutableSet<K>() {
        override fun iterator(): MutableIterator<K> {
            return object : HashIteratorDecorator<K>() {
                override fun next(): K {
                    return decorated.nextEntry().key
                }
            }
        }

        override val size: Int
            get() {
                return internalSize
            }

        override fun add(element: K): Boolean {
            throw UnsupportedOperationException()
        }

        override operator fun contains(element: K): Boolean {
            return containsKey(element)
        }

        override fun remove(element: K): Boolean {
            return this@AbstractHashMap.remove(element) != null
        }

        override fun clear() {
            this@AbstractHashMap.clear()
        }
    }

    private inner class Values : AbstractMutableCollection<V?>() {
        override fun iterator(): MutableIterator<V?> {
            return object : HashIteratorDecorator<V?>() {
                override fun next(): V? {
                    return decorated.nextEntry().value
                }
            }
        }

        override val size: Int
            get() {
                return internalSize
            }

        override fun add(element: V?): Boolean {
            throw UnsupportedOperationException()
        }

        override operator fun contains(element: V?): Boolean {
            return containsValue(element)
        }

        override fun clear() {
            this@AbstractHashMap.clear()
        }
    }
}
