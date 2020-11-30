/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
import jetbrains.exodus.entitystore.tables.PropertyTypes.toLowerCase
import jetbrains.exodus.kotlin.notNull
import org.slf4j.LoggerFactory

class UpdatablePropertiesCachedInstanceIterable
private constructor(txn: PersistentStoreTransaction?,
                    source: EntityIterableBase,
                    private var typeId: Int,
                    private var valueClass: Class<Comparable<Any>>?,
                    private val index: Persistent23Tree<IndexEntry<Comparable<Any>>>) : UpdatableCachedInstanceIterable(txn, source) {

    private var mutableIndex: Persistent23Tree.MutableTree<IndexEntry<Comparable<Any>>>? = null

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
        val oldEntry = if (oldValue == null) null else createEntry(toLowerCase(oldValue), localId) as IndexEntry<Comparable<Any>>
        val newEntry = if (newValue == null) null else createEntry(toLowerCase(newValue), localId) as IndexEntry<Comparable<Any>>
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
            if (!index.contains(newEntry)) {
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

    private abstract inner class PropertiesCachedInstanceIteratorBase(private val it: Iterator<IndexEntry<Comparable<Any>>>)
        : NonDisposableEntityIterator(this@UpdatablePropertiesCachedInstanceIterable), PropertyValueIterator {

        protected var next: IndexEntry<Comparable<Any>>? = null
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
            val result = PersistentEntityId(entityTypeId, next.notNull.localId)
            hasNextValid = false
            return result
        }

        override fun currentValue() = next?.propValue

        protected open fun checkIndexEntry(entry: IndexEntry<Comparable<Any>>?) = entry != null
    }

    private inner class PropertiesCachedInstanceIterator : PropertiesCachedInstanceIteratorBase(currentTree.iterator())

    private inner class ReversePropertiesCachedInstanceIterator : PropertiesCachedInstanceIteratorBase(currentTree.reverseIterator())

    private inner class PropertyValueCachedInstanceIterator(private val value: Comparable<Any>)
        : PropertiesCachedInstanceIteratorBase(currentTree.tailIterator(createEntry(value, 0) as IndexEntry<Comparable<Any>>)) {

        override fun checkIndexEntry(entry: IndexEntry<Comparable<Any>>?) = entry != null && entry.propValue == value
    }

    private inner class PropertyRangeCachedInstanceIterator(min: Comparable<Any>, private val max: Comparable<Any>)
        : PropertiesCachedInstanceIteratorBase(currentTree.tailIterator(createEntry(min, 0) as IndexEntry<Comparable<Any>>)) {

        override fun checkIndexEntry(entry: IndexEntry<Comparable<Any>>?) = entry != null && entry.propValue <= max
    }

    companion object {

        private val logger = LoggerFactory.getLogger(UpdatablePropertiesCachedInstanceIterable::class.java)
        private val PropertyValueIterator.nextId get() = this.nextId().notNull
        private val PropertyValueIterator.currentValue: Comparable<Any> get() = this.currentValue().notNull

        @JvmStatic
        fun newInstance(txn: PersistentStoreTransaction?,
                        it: PropertyValueIterator?,
                        source: EntityIterableBase): UpdatablePropertiesCachedInstanceIterable {
            try {
                if (it != null && it.hasNext()) {
                    val index = Persistent23Tree<IndexEntry<Comparable<Any>>>()
                    val id: EntityId = it.nextId
                    val typeId = id.typeId
                    var prevValue: Comparable<Any> = it.currentValue
                    val valueClass = prevValue.javaClass
                    val tempList = mutableListOf(createEntry(prevValue, id.localId))
                    while (it.hasNext()) {
                        val localId = it.nextId.localId
                        val currentValue: Comparable<Any> = it.currentValue
                        if (prevValue == currentValue) {
                            tempList.add(createEntry(prevValue, localId))
                        } else {
                            tempList.add(createEntry(currentValue, localId))
                            prevValue = currentValue
                            if (valueClass != currentValue.javaClass) {
                                throw EntityStoreException("Unexpected property value class")
                            }
                        }
                    }
                    index.beginWrite().run {
                        addAll(tempList as MutableIterable<IndexEntry<Comparable<Any>>>, tempList.size)
                        endWrite()
                    }
                    return UpdatablePropertiesCachedInstanceIterable(txn, source, typeId, valueClass, index)
                } else {
                    return UpdatablePropertiesCachedInstanceIterable(txn, source, -1, null, Persistent23Tree())
                }
            } finally {
                if (it is EntityIteratorBase) {
                    it.disposeIfShouldBe()
                }
            }
        }
    }
}

private fun createEntry(propValue: Comparable<*>, localId: Long): IndexEntry<*> {
    return when (propValue) {
        is Long -> LongIndexEntry(propValue, localId)
        is Int -> IntIndexEntry(propValue, localId)
        is Boolean -> BooleanIndexEntry(propValue, localId)
        is Float -> FloatIndexEntry(propValue, localId)
        is Double -> DoubleIndexEntry(propValue, localId)
        is Byte -> ByteIndexEntry(propValue, localId)
        is Short -> ShortIndexEntry(propValue, localId)
        else -> ComparableIndexEntry(propValue as Comparable<Any>, localId)
    }
}

private abstract class IndexEntry<T : Comparable<T>>(val localId: Long) : Comparable<IndexEntry<T>> {

    abstract val propValue: T

    override fun compareTo(other: IndexEntry<T>): Int {
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

private class ComparableIndexEntry(override val propValue: Comparable<Any>, localId: Long) : IndexEntry<Comparable<Any>>(localId)

private class LongIndexEntry(override val propValue: Long, localId: Long) : IndexEntry<Long>(localId) {

    override fun compareTo(other: IndexEntry<Long>): Int {
        val otherValue = (other as LongIndexEntry).propValue
        return when {
            propValue < otherValue -> -1
            propValue > otherValue -> 1
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class IntIndexEntry(override val propValue: Int, localId: Long) : IndexEntry<Int>(localId) {

    override fun compareTo(other: IndexEntry<Int>): Int {
        val otherValue = (other as IntIndexEntry).propValue
        return when {
            propValue < otherValue -> -1
            propValue > otherValue -> 1
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class BooleanIndexEntry(override val propValue: Boolean, localId: Long) : IndexEntry<Boolean>(localId) {

    override fun compareTo(other: IndexEntry<Boolean>): Int {
        val otherValue = (other as BooleanIndexEntry).propValue
        return when {
            propValue < otherValue -> -1
            propValue > otherValue -> 1
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class FloatIndexEntry(override val propValue: Float, localId: Long) : IndexEntry<Float>(localId) {

    override fun compareTo(other: IndexEntry<Float>): Int {
        val otherValue = (other as FloatIndexEntry).propValue
        return when {
            propValue < otherValue -> -1
            propValue > otherValue -> 1
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class DoubleIndexEntry(override val propValue: Double, localId: Long) : IndexEntry<Double>(localId) {

    override fun compareTo(other: IndexEntry<Double>): Int {
        val otherValue = (other as DoubleIndexEntry).propValue
        return when {
            propValue < otherValue -> -1
            propValue > otherValue -> 1
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class ByteIndexEntry(override val propValue: Byte, localId: Long) : IndexEntry<Byte>(localId) {

    override fun compareTo(other: IndexEntry<Byte>): Int {
        val result = propValue.toInt() - (other as ByteIndexEntry).propValue.toInt()
        return when {
            result != 0 -> result
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}

private class ShortIndexEntry(override val propValue: Short, localId: Long) : IndexEntry<Short>(localId) {

    override fun compareTo(other: IndexEntry<Short>): Int {
        val result = propValue.toInt() - (other as ShortIndexEntry).propValue.toInt()
        return when {
            result != 0 -> result
            else -> {
                val otherLocalId = other.localId
                when {
                    localId < otherLocalId -> -1
                    localId > otherLocalId -> 1
                    else -> 0
                }
            }
        }
    }
}
