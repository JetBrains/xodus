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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityStoreException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.tables.PropertyTypes
import jetbrains.exodus.kotlin.notNull
import org.slf4j.LoggerFactory

class UpdatablePropertiesCachedInstanceIterable private constructor(txn: PersistentStoreTransaction?,
                                                                    source: EntityIterableBase,
                                                                    private var typeId: Int,
                                                                    private var valueClass: Class<Comparable<Any>>?,
                                                                    private val index: Persistent23Tree<IndexEntry>
) : UpdatableCachedInstanceIterable(txn, source) {

    companion object {
        private val logger = LoggerFactory.getLogger(UpdatablePropertiesCachedInstanceIterable::class.java)
        private val PropertyValueIterator.nextId get() = this.nextId().notNull
        private val PropertyValueIterator.currentValue: Comparable<Any> get() = this.currentValue().notNull

        @JvmStatic
        fun newInstance(
                txn: PersistentStoreTransaction?, it: PropertyValueIterator?, source: EntityIterableBase
        ): UpdatablePropertiesCachedInstanceIterable {
            try {
                if (it != null && it.hasNext()) {
                    val index = Persistent23Tree<IndexEntry>()
                    val id: EntityId = it.nextId
                    val typeId = id.typeId
                    var prevValue: Comparable<Any> = it.currentValue
                    val valueClass = prevValue.javaClass
                    val tempList = mutableListOf(IndexEntry(prevValue, id.localId))
                    while (it.hasNext()) {
                        val localId = it.nextId.localId
                        val currentValue: Comparable<Any> = it.currentValue
                        if (prevValue == currentValue) {
                            tempList.add(IndexEntry(prevValue, localId))
                        } else {
                            tempList.add(IndexEntry(currentValue, localId))
                            prevValue = currentValue
                            if (valueClass != currentValue.javaClass) {
                                throw EntityStoreException("Unexpected property value class")
                            }
                        }
                    }
                    index.beginWrite().run {
                        addAll(tempList, tempList.size)
                        endWrite()
                    }
                    return UpdatablePropertiesCachedInstanceIterable(txn, source, typeId, valueClass, index)
                } else {
                    return UpdatablePropertiesCachedInstanceIterable(txn, source, -1, null, Persistent23Tree<IndexEntry>())
                }
            } finally {
                if (it is EntityIteratorBase) {
                    it.disposeIfShouldBe()
                }
            }
        }
    }

    private var mutableIndex: Persistent23Tree.MutableTree<IndexEntry>? = null

    // constructor for mutating source index
    private constructor(source: UpdatablePropertiesCachedInstanceIterable) : this(
            source.transaction,
            source,
            source.typeId,
            source.valueClass,
            source.index.clone
    ) {
        mutableIndex = index.beginWrite()
    }

    override fun getEntityTypeId() = typeId

    fun getPropertyValueClass() = valueClass

    override fun isSortedById() = false

    override fun beginUpdate() = UpdatablePropertiesCachedInstanceIterable(this)

    override fun isMutated() = mutableIndex != null

    override fun endUpdate() {
        val index = mutableIndex.notNull { "UpdatablePropertiesCachedInstanceIterable was not mutated" }
        index.endWrite()
        mutableIndex = null
    }

    fun update(typeId: Int, localId: Long, oldValue: Comparable<Any>?, newValue: Comparable<Any>?) {
        if (this.typeId == -1) {
            this.typeId = typeId
        }
        val oldEntry = if (oldValue == null) null else IndexEntry(PropertyTypes.toLowerCase(oldValue), localId)
        val newEntry = if (newValue == null) null else IndexEntry(PropertyTypes.toLowerCase(newValue), localId)
        if (oldEntry == newEntry) {
            throw IllegalStateException("Can't update in-memory index: both oldValue and newValue are null")
        }
        val index = mutableIndex.notNull { "Mutate index before updating it" }
        if (oldEntry != null) {
            if (index.contains(oldEntry)) {
                index.exclude(oldEntry)
            } else if (newEntry != null && !index.contains(newEntry)) {
                logger.warn("In-memory index doesn't contain the value [$oldValue]. New value [$newValue]. Handle [$handle]")
            }
        }
        if (newEntry != null) {
            val entryNoDuplicates = IndexEntryNoDuplicates(newEntry.propValue, newEntry.localId)
            if (!index.contains(entryNoDuplicates)) {
                newEntry.propValue = entryNoDuplicates.propValue
                index.add(newEntry)
            }
            if (valueClass == null) {
                valueClass = newEntry.propValue.javaClass
            }
        }
    }

    fun getPropertyValueIterator(value: Comparable<Any>): EntityIteratorBase {
        return PropertyValueCachedInstanceIterator(value)
    }

    fun getPropertyRangeIterator(min: Comparable<Any>, max: Comparable<Any>): EntityIteratorBase {
        return PropertyRangeCachedInstanceIterator(min, max)
    }

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        return if (currentTree.isEmpty) EntityIteratorBase.EMPTY else PropertiesCachedInstanceIterator()
    }

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        return if (currentTree.isEmpty) EntityIteratorBase.EMPTY else ReversePropertiesCachedInstanceIterator()
    }

    override fun size() = currentTree.size().toLong()

    override fun countImpl(txn: PersistentStoreTransaction) = size()

    private val currentTree get() = mutableIndex ?: index.beginRead()

    private open class IndexEntry(var propValue: Comparable<Any>, val localId: Long) : Comparable<IndexEntry> {

        override fun compareTo(other: IndexEntry): Int {
            var result = propValue.compareTo(other.propValue)
            if (result == 0) {
                if (localId < other.localId) {
                    result = -1
                } else if (localId > other.localId) {
                    result = 1
                }
            }
            return result
        }
    }

    private class IndexEntryNoDuplicates(propValue: Comparable<Any>, localId: Long) : IndexEntry(propValue, localId) {

        override fun compareTo(other: IndexEntry): Int {
            var result = propValue.compareTo(other.propValue)
            if (result == 0) {
                propValue = other.propValue
                if (localId < other.localId) {
                    result = -1
                } else if (localId > other.localId) {
                    result = 1
                }
            }
            return result
        }
    }

    private abstract inner class PropertiesCachedInstanceIteratorBase(private val it: Iterator<IndexEntry>)
        : NonDisposableEntityIterator(this@UpdatablePropertiesCachedInstanceIterable), PropertyValueIterator {

        protected var next: IndexEntry? = null
        protected var hasNextValid: Boolean = false

        override fun hasNextImpl(): Boolean {
            if (!hasNextValid) {
                if (!it.hasNext()) {
                    return false
                }
                val next = it.next()
                if (!checkIndexEntry(next)) {
                    return false
                }
                this.next = next
                hasNextValid = true
            }
            return true
        }

        public override fun nextIdImpl(): EntityId? {
            if (!hasNextImpl()) {
                return null
            }
            val result = PersistentEntityId(entityTypeId, next!!.localId)
            hasNextValid = false
            return result
        }

        override fun currentValue() = next?.propValue

        protected open fun checkIndexEntry(entry: IndexEntry?) = entry != null
    }

    private inner class PropertiesCachedInstanceIterator : PropertiesCachedInstanceIteratorBase(currentTree.iterator())

    private inner class ReversePropertiesCachedInstanceIterator : PropertiesCachedInstanceIteratorBase(currentTree.reverseIterator())

    private inner class PropertyValueCachedInstanceIterator(private val value: Comparable<Any>)
        : PropertiesCachedInstanceIteratorBase(currentTree.tailIterator(IndexEntry(value, 0))) {

        override fun checkIndexEntry(entry: IndexEntry?) = entry != null && entry.propValue == value
    }

    private inner class PropertyRangeCachedInstanceIterator(min: Comparable<Any>, private val max: Comparable<Any>)
        : PropertiesCachedInstanceIteratorBase(currentTree.tailIterator(IndexEntry(min, 0))) {

        override fun checkIndexEntry(entry: IndexEntry?) = entry != null && entry.propValue <= max
    }
}
