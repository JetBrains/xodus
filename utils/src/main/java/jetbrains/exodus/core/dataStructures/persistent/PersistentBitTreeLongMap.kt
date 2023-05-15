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

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.MutableTree
import java.util.*

class PersistentBitTreeLongMap<V> : PersistentLongMap<V> {
    private val root: Root<V>

    constructor() {
        this.root = Root(Persistent23Tree(), 0)
    }

    private constructor(root: Root<V>) {
        this.root = root
    }

    override fun beginRead(): PersistentLongMap.ImmutableMap<V> {
        return ImmutableMap(root.map.beginRead(), root.size)
    }

    override val clone: PersistentLongMap<V>
        get() = PersistentBitTreeLongMap(root.getClone())

    override fun beginWrite(): PersistentLongMap.MutableMap<V> {
        return MutableMap(root.map.beginWrite(), root.size, this)
    }

    protected class ImmutableMap<V> internal constructor(
        protected val map: AbstractPersistent23Tree<Entry?>,
        protected val size: Int
    ) : PersistentLongMap.ImmutableMap<V?> {
        private fun getEntryByIndex(index: Long): Entry? {
            val root = map.root ?: return null
            return root.getByWeight(index)
        }

        override fun get(key: Long): V? {
            val entry = getEntryByIndex(getEntryIndex(key)) ?: return null
            return entry.data[(key and MASK.toLong()).toInt()] as V?
        }

        override fun containsKey(key: Long): Boolean {
            val entry = getEntryByIndex(getEntryIndex(key))
            return entry != null && entry.data[(key and MASK.toLong()).toInt()] != null
        }

        override val minimum: PersistentLongMap.Entry<V?>?
            get() {
                val entry = map.minimum ?: return null
                val index = entry.bits.nextSetBit(0)
                check(index != -1) { "unexpected empty entry" }
                return LongMapEntry(index + (entry.weight shl BITS_PER_ENTRY), entry.data[index] as V?)
            }

        override fun iterator(): MutableIterator<PersistentLongMap.Entry<V?>> {
            return ItemIterator(map)
        }

        override fun reverseIterator(): Iterator<PersistentLongMap.Entry<V?>> {
            return ReverseItemIterator(map)
        }

        override fun tailEntryIterator(startingKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return ItemTailIterator<V?>(map, startingKey)
        }

        override fun tailReverseEntryIterator(startingKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return ReverseItemTailIterator<V?>(map, startingKey)
        }

        override val isEmpty: Boolean
            get() = size == 0

        override fun size(): Int {
            return size
        }
    }

    protected class MutableMap<V> internal constructor(
        private val mutableMap: MutableTree<Entry?>,
        private var size: Int,
        private val baseMap: PersistentBitTreeLongMap<*>
    ) : PersistentLongMap.MutableMap<V>, RootHolder {
        override fun getRoot(): AbstractPersistent23Tree.RootNode<Entry?>? {
            return mutableMap.root
        }

        private fun getEntryByIndex(index: Long): Entry? {
            val root = root ?: return null
            return root.getByWeight(index)
        }

        override fun get(key: Long): V? {
            val entry = getEntryByIndex(getEntryIndex(key)) ?: return null
            return entry.data[(key and MASK.toLong()).toInt()] as V?
        }

        override fun containsKey(key: Long): Boolean {
            val entry = getEntryByIndex(getEntryIndex(key))
            return entry != null && entry.data[(key and MASK.toLong()).toInt()] != null
        }

        override val isEmpty: Boolean
            get() = size == 0

        override fun size(): Int {
            return size
        }

        override val minimum: PersistentLongMap.Entry<V?>?
            get() {
                val entry = mutableMap.minimum ?: return null
                val index = entry.bits.nextSetBit(0)
                check(index != -1) { "unexpected empty entry" }
                return LongMapEntry(index + (entry.weight shl BITS_PER_ENTRY), entry.data[index] as V?)
            }

        override fun iterator(): MutableIterator<PersistentLongMap.Entry<V>> {
            return ItemIterator(mutableMap)
        }

        override fun reverseIterator(): Iterator<PersistentLongMap.Entry<V>> {
            return ReverseItemIterator(mutableMap)
        }

        override fun tailEntryIterator(startingKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return ItemTailIterator<V?>(mutableMap, startingKey)
        }

        override fun tailReverseEntryIterator(startingKey: Long): Iterator<PersistentLongMap.Entry<V?>?>? {
            return ReverseItemTailIterator<V?>(mutableMap, startingKey)
        }

        override fun put(key: Long, value: V) {
            val index = getEntryIndex(key)
            var entry = getEntryByIndex(index)
            val bitIndex = (key and MASK.toLong()).toInt()
            if (entry == null) {
                entry = Entry(index)
                mutableMap.add(entry)
                size++
            } else {
                val copy = Entry(index, entry)
                mutableMap.add(copy)
                entry = copy
                if (!entry.bits[bitIndex]) {
                    size++
                }
            }
            entry.bits.set(bitIndex)
            entry.data[bitIndex] = value
        }

        override fun remove(key: Long): V {
            val index = getEntryIndex(key)
            val entry = getEntryByIndex(index) ?: return null
            val bitIndex = (key and MASK.toLong()).toInt()
            if (entry.bits[bitIndex]) {
                size--
            } else {
                return null
            }
            val result = entry.data[bitIndex]!!
            val copy = Entry(index, entry)
            copy.bits.clear(bitIndex)
            if (copy.bits.isEmpty) {
                mutableMap.exclude(entry)
            } else {
                copy.data[bitIndex] = null
                mutableMap.add(copy)
            }
            return result as V
        }

        override fun clear() {
            mutableMap.root = null
            size = 0
        }

        override fun endWrite(): Boolean {
            if (!mutableMap.endWrite()) {
                return false
            }
            // TODO: consistent size update
            baseMap.root.size = size
            return true
        }

        override fun testConsistency() {
            mutableMap.testConsistency()
        }
    }

    protected class Entry : LongComparable<Entry?> {
        override val weight: Long
        val bits: BitSet
        val data: Array<Any?>

        constructor(min: Long) {
            weight = min
            data = arrayOfNulls(ELEMENTS_PER_ENTRY)
            bits = BitSet(ELEMENTS_PER_ENTRY)
        }

        constructor(min: Long, other: Entry?) {
            weight = min
            if (other != null) {
                bits = BitSet(ELEMENTS_PER_ENTRY)
                bits.or(other.bits)
                data = arrayOfNulls(ELEMENTS_PER_ENTRY)
                System.arraycopy(other.data, 0, data, 0, ELEMENTS_PER_ENTRY)
            } else {
                bits = FAKE_BITS
                data = FAKE_DATA
            }
        }

        override operator fun compareTo(o: Entry): Int {
            val otherMin = o.weight
            return if (weight > otherMin) 1 else if (weight == otherMin) 0 else -1
        }
    }

    protected class Root<V> internal constructor(val map: Persistent23Tree<Entry?>, val size: Int) {
        fun getClone(): Root<V> {
            return Root(map.clone, size)
        }
    }

    private class ItemIterator<V> internal constructor(tree: AbstractPersistent23Tree<Entry?>) :
        MutableIterator<PersistentLongMap.Entry<V?>> {
        private val iterator: Iterator<Entry?>
        private var currentEntry: Entry? = null
        private var currentEntryBase: Long = 0
        private var next = -1

        init {
            iterator = tree.iterator()
        }

        override fun next(): PersistentLongMap.Entry<V?> {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = next
            val key = index + currentEntryBase
            val result = currentEntry!!.data[index]
            next = currentEntry!!.bits.nextSetBit(index + 1)
            return LongMapEntry(key, result as V?)
        }

        override fun hasNext(): Boolean {
            return next != -1 || fetchEntry()
        }

        private fun fetchEntry(): Boolean {
            while (iterator.hasNext()) {
                val entry = iterator.next()
                val nextIndex = entry!!.bits.nextSetBit(0)
                if (nextIndex != -1) {
                    currentEntry = entry
                    currentEntryBase = entry.weight shl BITS_PER_ENTRY
                    next = nextIndex
                    return true
                }
            }
            return false
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    private class ItemTailIterator<V> internal constructor(tree: AbstractPersistent23Tree<Entry?>, startingKey: Long) :
        MutableIterator<PersistentLongMap.Entry<V?>> {
        private val iterator: Iterator<Entry?>
        private val startingEntryIndex: Long
        private var startingIndex: Int
        private var currentEntry: Entry? = null
        private var currentEntryBase: Long = 0
        private var next = -1

        init {
            startingEntryIndex = getEntryIndex(startingKey)
            iterator = tree.tailIterator(makeIndexEntry(startingEntryIndex))
            startingIndex = (startingKey and MASK.toLong()).toInt()
        }

        override fun next(): PersistentLongMap.Entry<V?> {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = next
            val key = index + currentEntryBase
            val result = currentEntry!!.data[index]
            next = currentEntry!!.bits.nextSetBit(index + 1)
            return LongMapEntry(key, result as V?)
        }

        override fun hasNext(): Boolean {
            return next != -1 || fetchEntry()
        }

        private fun fetchEntry(): Boolean {
            while (iterator.hasNext()) {
                val entry = iterator.next()
                var fromIndex = startingIndex
                if (fromIndex != 0) {
                    if (startingEntryIndex != entry!!.weight) {
                        fromIndex = 0
                    }
                    startingIndex = 0
                }
                val nextIndex = entry!!.bits.nextSetBit(fromIndex)
                if (nextIndex != -1) {
                    currentEntry = entry
                    currentEntryBase = entry.weight shl BITS_PER_ENTRY
                    next = nextIndex
                    return true
                }
            }
            return false
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    private class ReverseItemIterator<V> internal constructor(tree: AbstractPersistent23Tree<Entry?>) :
        MutableIterator<PersistentLongMap.Entry<V?>> {
        private val iterator: Iterator<Entry?>
        private var currentEntry: Entry? = null
        private var currentEntryBase: Long = 0
        private var next = -1

        init {
            iterator = tree.reverseIterator()
        }

        override fun next(): PersistentLongMap.Entry<V?> {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = next
            val key = index + currentEntryBase
            val result = currentEntry!!.data[index]
            next = currentEntry!!.bits.previousSetBit(index - 1)
            return LongMapEntry(key, result as V?)
        }

        override fun hasNext(): Boolean {
            return next != -1 || fetchEntry()
        }

        private fun fetchEntry(): Boolean {
            while (iterator.hasNext()) {
                val entry = iterator.next()
                val prevIndex = entry!!.bits.length() - 1
                if (prevIndex != -1) {
                    currentEntry = entry
                    currentEntryBase = entry.weight shl BITS_PER_ENTRY
                    next = prevIndex
                    return true
                }
            }
            return false
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    private class ReverseItemTailIterator<V> internal constructor(
        tree: AbstractPersistent23Tree<Entry?>,
        minKey: Long
    ) : MutableIterator<PersistentLongMap.Entry<V?>> {
        private val iterator: Iterator<Entry?>
        private val finishingEntryIndex: Long
        private val finishingIndex: Int
        private var currentEntry: Entry? = null
        private var currentEntryBase: Long = 0
        private var next = -1

        init {
            finishingEntryIndex = getEntryIndex(minKey)
            iterator = tree.tailReverseIterator(makeIndexEntry(finishingEntryIndex))
            finishingIndex = (minKey and MASK.toLong()).toInt()
        }

        override fun next(): PersistentLongMap.Entry<V?> {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = next
            val key = index + currentEntryBase
            val entry = currentEntry
            val result = entry!!.data[index]
            val prevIndex = entry.bits.previousSetBit(index - 1)
            if (entry.weight == finishingEntryIndex && prevIndex < finishingIndex) {
                next = -1
            } else {
                next = prevIndex
            }
            return LongMapEntry(key, result as V?)
        }

        override fun hasNext(): Boolean {
            return next != -1 || fetchEntry()
        }

        private fun fetchEntry(): Boolean {
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry!!.weight < finishingEntryIndex) {
                    return false
                }
                val prevIndex = entry.bits.length() - 1
                if (entry.weight == finishingEntryIndex && prevIndex < finishingIndex) {
                    return false
                }
                if (prevIndex != -1) {
                    currentEntry = entry
                    currentEntryBase = entry.weight shl BITS_PER_ENTRY
                    next = prevIndex
                    return true
                }
            }
            return false
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    companion object {
        private const val BITS_PER_ENTRY = 10
        private const val ELEMENTS_PER_ENTRY = 1 shl BITS_PER_ENTRY
        private const val MASK = ELEMENTS_PER_ENTRY - 1
        private val FAKE_BITS = BitSet()
        private val FAKE_DATA = arrayOfNulls<Any>(0)
        private fun getEntryIndex(value: Long): Long {
            return value shr BITS_PER_ENTRY
        }

        private fun makeIndexEntry(index: Long): Entry {
            return Entry(index, null)
        }
    }
}
