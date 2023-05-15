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

import jetbrains.exodus.core.dataStructures.persistent.AbstractPersistentHashSet.RootTableNode
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet.ImmutablePersistentHashSet
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet.MutablePersistentHashSet

class PersistentHashMap<K, V> {
    private val set: PersistentHashSet<Entry<K, V>?>

    constructor() {
        set = PersistentHashSet()
    }

    private constructor(root: RootTableNode<Entry<K, V>?>) {
        set = PersistentHashSet(root)
    }

    val current: ImmutablePersistentHashMap
        get() = ImmutablePersistentHashMap()
    val clone: PersistentHashMap<K, V>
        get() = PersistentHashMap(set.root)

    fun beginWrite(): MutablePersistentHashMap {
        return MutablePersistentHashMap()
    }

    fun endWrite(tree: MutablePersistentHashMap): Boolean {
        return set.endWrite(tree)
    }

    inner class ImmutablePersistentHashMap internal constructor() : ImmutablePersistentHashSet<Entry<K, V>?>(set.root) {
        operator fun get(key: K): V? {
            val entry = root.getKey(Entry(key), key.hashCode(), 0)
            return entry?.value
        }

        fun containsKey(key: K): Boolean {
            return root.getKey(Entry(key), key.hashCode(), 0) != null
        }
    }

    inner class MutablePersistentHashMap internal constructor() : MutablePersistentHashSet<Entry<K, V>?>(set) {
        operator fun get(key: K): V? {
            val entry = root.getKey(Entry(key), key.hashCode(), 0)
            return entry?.value
        }

        fun containsKey(key: K): Boolean {
            return root.getKey(Entry(key), key.hashCode(), 0) != null
        }

        fun put(key: K, value: V) {
            add(Entry(key, value))
        }

        fun removeKey(key: K): V? {
            val entry = root.getKey(Entry(key), key.hashCode(), 0)
            val result = entry?.value
            entry?.let { remove(it) }
            return result
        }
    }

    class Entry<K, V>(val key: K, value: V? = null) {
        val value: V

        init {
            this.value = value
        }

        override fun equals(obj: Any?): Boolean {
            return key == (obj as Entry<K, V>?)!!.key
        }

        override fun hashCode(): Int {
            return key.hashCode()
        }
    }
}
