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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.ImmutableTree
import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.MutableTree

class Persistent23TreeMap<K : Comparable<K>?, V> internal constructor(root: AbstractPersistent23Tree.RootNode<Entry<K, V>?>?) {
    private val set: Persistent23Tree<Entry<K, V>?>

    constructor() : this(null)

    init {
        set = Persistent23Tree(root)
    }

    fun beginRead(): ImmutableMap<K, V> {
        return ImmutableMap(set)
    }

    val clone: Persistent23TreeMap<K, V>
        get() = Persistent23TreeMap(set.getRoot())

    fun beginWrite(): MutableMap<K, V> {
        return MutableMap(set)
    }

    fun endWrite(tree: MutableMap<K?, V>?): Boolean {
        return set.endWrite(tree)
    }

    fun createEntry(key: K): Entry<K, V> {
        return Entry(key)
    }

    class ImmutableMap<K : Comparable<K>?, V> internal constructor(set: Persistent23Tree<Entry<K, V>?>) :
        ImmutableTree<Entry<K, V>?>(set.getRoot()) {
        operator fun get(key: K): V? {
            val root = root
                ?: return null
            val entry = root.get(Entry<K?, V?>(key))
            return entry?.value
        }

        fun containsKey(key: K): Boolean {
            val root: Node<Entry<K?, V?>?>? = getRoot()
            return root != null && root[Entry<K?, V?>(key)] != null
        }
    }

    class MutableMap<K : Comparable<K>?, V> internal constructor(set: Persistent23Tree<Entry<K, V>?>) :
        MutableTree<Entry<K, V>?>(set) {
        operator fun get(key: K): V? {
            val root = root
                ?: return null
            val entry = root.get(Entry<K?, V?>(key))
            return entry?.value
        }

        fun containsKey(key: K): Boolean {
            return get(key) != null
        }

        fun put(key: K, value: V) {
            add(Entry(key, value))
        }

        fun remove(key: K): V? {
            var root: RootNode<Entry<K?, V?>?>? = getRoot()
                ?: return null
            val removeResult = root.remove(Entry<K?, V?>(key), true)
                ?: return null
            var res = removeResult.getFirst()
            if (res is RemovedNode<*>) {
                res = res.getFirstChild()
            }
            root = res?.asRoot(root.getSize() - 1)
            setRoot(root)
            return removeResult.getSecond().value
        }
    }

    class Entry<K : Comparable<K>?, V> @JvmOverloads internal constructor(val key: K, val value: V? = null) :
        Comparable<Entry<K, V>> {

        override fun compareTo(o: Entry<K, V>): Int {
            return key!!.compareTo(o.key)
        }
    }
}