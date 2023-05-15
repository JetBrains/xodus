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

import jetbrains.exodus.core.dataStructures.hash.LongIterator

class PersistentLong23TreeSet private constructor(root: AbstractPersistent23Tree.RootNode<PersistentLongMap.Entry<Boolean?>?>?) :
    PersistentLongSet {
    private val set: Persistent23Tree<PersistentLongMap.Entry<Boolean?>?>

    constructor() : this(null)

    init {
        set = Persistent23Tree(root)
    }

    override fun beginRead(): PersistentLongSet.ImmutableSet {
        return ImmutableSet(set.root)
    }

    override val clone: PersistentLongSet
        get() = PersistentLong23TreeSet(set.root)

    override fun beginWrite(): PersistentLongSet.MutableSet {
        return MutableSet(set)
    }

    protected class ImmutableSet internal constructor(root: RootNode<PersistentLongMap.Entry<Boolean?>?>?) :
        PersistentLong23TreeMap.ImmutableMap<Boolean?>(root), PersistentLongSet.ImmutableSet {
        override fun contains(key: Long): Boolean {
            return containsKey(key)
        }

        override fun longIterator(): LongIterator {
            return IteratorImpl(iterator())
        }

        override fun reverseLongIterator(): LongIterator {
            return IteratorImpl(reverseIterator())
        }

        override fun tailLongIterator(key: Long): LongIterator {
            return IteratorImpl(tailEntryIterator(key))
        }

        override fun tailReverseLongIterator(key: Long): LongIterator {
            return IteratorImpl(tailReverseEntryIterator(key))
        }
    }

    protected class MutableSet internal constructor(set: Persistent23Tree<PersistentLongMap.Entry<Boolean?>?>) :
        PersistentLongSet.MutableSet {
        private val map: PersistentLong23TreeMap.MutableMap<Boolean?>

        init {
            map = PersistentLong23TreeMap.MutableMap(set)
        }

        override fun longIterator(): LongIterator {
            return IteratorImpl(map.iterator())
        }

        override fun reverseLongIterator(): LongIterator {
            return IteratorImpl(map.reverseIterator())
        }

        override fun tailLongIterator(key: Long): LongIterator {
            return IteratorImpl(map.tailEntryIterator(key))
        }

        override fun tailReverseLongIterator(key: Long): LongIterator {
            return IteratorImpl(map.tailReverseEntryIterator(key))
        }

        override val isEmpty: Boolean
            get() = map.isEmpty

        override fun size(): Int {
            return map.size()
        }

        override fun contains(key: Long): Boolean {
            return map[key] === java.lang.Boolean.TRUE
        }

        override fun add(key: Long) {
            map.put(key, java.lang.Boolean.TRUE)
        }

        override fun remove(key: Long): Boolean {
            return map.remove(key) ?: return false
        }

        override fun clear() {
            map.root = null
        }

        override fun endWrite(): Boolean {
            return map.endWrite()
        }
    }

    protected class IteratorImpl internal constructor(private val it: Iterator<PersistentLongMap.Entry<Boolean?>?>?) :
        LongIterator {
        override fun nextLong(): Long {
            val entry = it!!.next() ?: throw NoSuchElementException()
            return entry.key
        }

        override fun hasNext(): Boolean {
            return it!!.hasNext()
        }

        override fun next(): Long {
            val entry = it!!.next() ?: throw NoSuchElementException()
            return entry.key
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }
}
