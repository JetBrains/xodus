/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.hash.LongIterator
import java.util.*

private const val BITS_PER_ENTRY = 10

class PersistentBitTreeLongSet : PersistentLongSet {

    private val root: Root
    private val bitsPerEntry: Int
    private val elementsPerEntry: Int
    private val mask: Int

    @JvmOverloads constructor(bitsPerEntry: Int = BITS_PER_ENTRY) {
        this.bitsPerEntry = bitsPerEntry
        elementsPerEntry = 1 shl bitsPerEntry
        mask = elementsPerEntry - 1
        this.root = Root(Persistent23Tree(), 0)
    }

    private constructor(source: PersistentBitTreeLongSet) {
        bitsPerEntry = source.bitsPerEntry
        elementsPerEntry = source.elementsPerEntry
        mask = source.mask
        this.root = source.root.clone
    }

    override fun beginRead(): PersistentLongSet.ImmutableSet {
        return ImmutableSet(root.map.beginRead(), root.size, bitsPerEntry, mask)
    }

    override fun getClone(): PersistentLongSet {
        return PersistentBitTreeLongSet(this)
    }

    override fun beginWrite(): PersistentLongSet.MutableSet {
        return MutableSet(root.map.beginWrite(), root.size, bitsPerEntry, mask, this)
    }

    private class ImmutableSet internal constructor(private val map: AbstractPersistent23Tree<Entry>,
                                                    private val size: Int,
                                                    private val bitsPerEntry: Int,
                                                    private val mask: Int) : PersistentLongSet.ImmutableSet {

        private fun getEntryByIndex(index: Long): Entry? {
            val root = map.root ?: return null
            return root.getByWeight(index)
        }

        override fun contains(key: Long): Boolean {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return entry != null && entry.bits.get(key.toInt() and mask)
        }

        override fun longIterator(): LongIterator {
            return ItemIterator(map.iterator(), bitsPerEntry)
        }

        override fun reverseLongIterator(): LongIterator {
            return ItemIterator(map.reverseIterator(), bitsPerEntry, isReversed = true)
        }

        override fun tailLongIterator(key: Long): LongIterator {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return if (entry == null) LongIterator.EMPTY else
                skipTail(ItemIterator(map.tailIterator(entry), bitsPerEntry), key)
        }

        override fun tailReverseLongIterator(key: Long): LongIterator {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return if (entry == null) LongIterator.EMPTY else
                skipTail(ItemIterator(map.tailReverseIterator(entry), bitsPerEntry, isReversed = true), key)
        }

        override fun isEmpty(): Boolean {
            return size == 0
        }

        override fun size(): Int {
            return size
        }
    }

    internal class MutableSet internal constructor(private val mutableSet: Persistent23Tree.MutableTree<Entry>,
                                                   private var size: Int,
                                                   private val bitsPerEntry: Int,
                                                   private val mask: Int,
                                                   private val baseSet: PersistentBitTreeLongSet) : PersistentLongSet.MutableSet {

        private fun getEntryByIndex(index: Long): Entry? {
            val root = mutableSet.root ?: return null
            return root.getByWeight(index)
        }

        override fun contains(key: Long): Boolean {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return entry != null && entry.bits.get(key.toInt() and mask)
        }

        override fun longIterator(): LongIterator {
            return ItemIterator(mutableSet.iterator(), bitsPerEntry)
        }

        override fun reverseLongIterator(): LongIterator {
            return ItemIterator(mutableSet.reverseIterator(), bitsPerEntry, isReversed = true)
        }

        override fun tailLongIterator(key: Long): LongIterator {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return if (entry == null) LongIterator.EMPTY else
                skipTail(ItemIterator(mutableSet.tailIterator(entry), bitsPerEntry), key)
        }

        override fun tailReverseLongIterator(key: Long): LongIterator {
            val entry = getEntryByIndex(key shr bitsPerEntry)
            return if (entry == null) LongIterator.EMPTY else
                skipTail(ItemIterator(mutableSet.tailReverseIterator(entry), bitsPerEntry, isReversed = true), key)
        }

        override fun isEmpty(): Boolean {
            return size == 0
        }

        override fun size(): Int {
            return size
        }

        override fun add(key: Long) {
            val index = key shr bitsPerEntry
            var entry = getEntryByIndex(index)
            val bitIndex = key.toInt() and mask
            if (entry == null) {
                entry = Entry(index, mask + 1)
                mutableSet.add(entry)
                size++
            } else {
                if (entry.bits.get(bitIndex)) {
                    return
                } else {
                    val copy = Entry(index, entry, mask + 1)
                    mutableSet.add(copy)
                    entry = copy
                    size++
                }
            }
            entry.bits.set(bitIndex)
        }

        override fun remove(key: Long): Boolean {
            val index = key shr bitsPerEntry
            val entry = getEntryByIndex(index) ?: return false
            val bitIndex = key.toInt() and mask
            if (entry.bits.get(bitIndex)) {
                size--
            } else {
                return false
            }
            val copy = Entry(index, entry, mask + 1)
            copy.bits.clear(bitIndex)
            if (copy.bits.isEmpty) {
                mutableSet.exclude(entry)
            } else {
                mutableSet.add(copy)
            }
            return true
        }

        override fun clear() {
            mutableSet.root = null
            size = 0
        }

        override fun endWrite(): Boolean {
            if (!mutableSet.endWrite()) {
                return false
            }
            // TODO: consistent size update
            baseSet.root.size = size
            return true
        }
    }

    internal class Entry : LongComparable<Entry> {

        internal val index: Long
        internal val bits: BitSet

        constructor(index: Long, elementsPerEntry: Int) {
            this.index = index
            this.bits = BitSet(elementsPerEntry)
        }

        constructor(index: Long, other: Entry, elementsPerEntry: Int) {
            this.index = index
            this.bits = BitSet(elementsPerEntry)
            this.bits.or(other.bits)
        }

        override fun getWeight(): Long {
            return index
        }

        override fun compareTo(other: PersistentBitTreeLongSet.Entry): Int {
            return java.lang.Long.compare(index, other.index)
        }
    }

    internal class Root internal constructor(internal val map: Persistent23Tree<Entry>, internal var size: Int) {

        val clone: Root get() = Root(map.clone, size)
    }

    private open class ItemIterator internal constructor(private val iterator: Iterator<Entry>,
                                                         private val bitsPerEntry: Int,
                                                         private val isReversed: Boolean = false) : LongIterator {

        private lateinit var currentEntry: Entry
        private var currentEntryBase: Long = 0
        private var next = -1

        override fun next(): Long {
            return nextLong()
        }

        override fun nextLong(): Long {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = this.next
            val result = index + currentEntryBase
            this.next = nextBit(currentEntry, next + if (isReversed) -1 else 1)
            return result
        }

        override fun hasNext(): Boolean {
            return next != -1 || fetchEntry()
        }

        private fun fetchEntry(): Boolean {
            while (iterator.hasNext()) {
                val entry = iterator.next()
                val nextIndex = nextBit(entry)
                if (nextIndex != -1) {
                    currentEntry = entry
                    currentEntryBase = entry.index shl bitsPerEntry
                    next = nextIndex
                    return true
                }
            }
            return false
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }

        private fun nextBit(e: Entry, fromBit: Int = Int.MAX_VALUE): Int {
            val bits = e.bits
            return if (isReversed) {
                bits.previousSetBit(if (fromBit == Int.MAX_VALUE) bits.size() else fromBit)
            } else {
                bits.nextSetBit(if (fromBit == Int.MAX_VALUE) 0 else fromBit)
            }
        }
    }
}
