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

class PersistentLong23TreeMap<V> private constructor(root: AbstractPersistent23Tree.RootNode<PersistentLongMap.Entry<V>?>?) :
    PersistentLongMap<V> {
    private val set: Persistent23Tree<PersistentLongMap.Entry<V>?>

    constructor() : this(null)

    init {
        set = Persistent23Tree(root)
    }

    override fun beginRead(): PersistentLongMap.ImmutableMap<V> {
        return ImmutableMap(set.getRoot())
    }

    override val clone: PersistentLongMap<V>
        get() = PersistentLong23TreeMap(set.root)

    override fun beginWrite(): PersistentLongMap.MutableMap<V> {
        return MutableMap(set)
    }

    @Deprecated("")
    fun endWrite(tree: MutableMap<V?>?): Boolean {
        return set.endWrite(tree)
    }

    open class ImmutableMap<V> internal constructor(root: RootNode<PersistentLongMap.Entry<V?>?>?) :
        ImmutableTree<PersistentLongMap.Entry<V>?>(root), PersistentLongMap.ImmutableMap<V?> {
        override fun get(key: Long): V? {
            val root = root
                ?: return null
            val entry = root.getByWeight(key)
            return entry?.value
        }

        override fun containsKey(key: Long): Boolean {
            val root: Node<PersistentLongMap.Entry<V?>?>? = getRoot()
            return root != null && root.getByWeight(key) != null
        }

        override fun tailEntryIterator(staringKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return tailIterator(LongMapEntry(staringKey))
        }

        override fun tailReverseEntryIterator(staringKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return tailReverseIterator(LongMapEntry(staringKey))
        }
    }

    class MutableMap<V> internal constructor(set: Persistent23Tree<PersistentLongMap.Entry<V>?>) :
        MutableTree<PersistentLongMap.Entry<V>?>(set), PersistentLongMap.MutableMap<V>, RootHolder {
        override fun get(key: Long): V? {
            val root = root
                ?: return null
            val entry = root.getByWeight(key)
            return entry?.value
        }

        override fun containsKey(key: Long): Boolean {
            return get(key) != null
        }

        override fun tailEntryIterator(staringKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return tailIterator(LongMapEntry(staringKey))
        }

        override fun tailReverseEntryIterator(staringKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return tailReverseIterator(LongMapEntry(staringKey))
        }

        override fun put(key: Long, value: V) {
            add(LongMapEntry(key, value))
        }

        override fun remove(key: Long): V {
            var root: RootNode<PersistentLongMap.Entry<V>?>? = root ?: return null
            val removeResult = root.remove(LongMapEntry(key), true) ?: return null
            var res = removeResult.getFirst()
            if (res is RemovedNode<*>) {
                res = res.getFirstChild()
            }
            root = res?.asRoot(root.size - 1)
            setRoot(root)
            return removeResult.getSecond().getValue()
        }

        override fun clear() {
            root = null
        }
    }
}
