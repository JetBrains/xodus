/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.tables.PropertyTypes
import jetbrains.exodus.env.Cursor

class PropertyValueIterable(
    txn: StoreTransaction,
    entityTypeId: Int,
    propertyId: Int,
    value: Comparable<*>
) : PropertyRangeOrValueIterableBase(txn, entityTypeId, propertyId) {

    private val value = PropertyTypes.toLowerCase(value)
    private val valueClass = value.javaClass
    private var forceCached = false

    companion object {
        init {
            registerType(EntityIterableType.ENTITIES_BY_PROP_VALUE) { txn, _, parameters ->
                try {
                    return@registerType PropertyValueIterable(
                        txn,
                        (parameters[0] as String).toInt(),
                        (parameters[1] as String).toInt(),
                        (parameters[2] as String).toLong()
                    )
                } catch (e: NumberFormatException) {
                    return@registerType PropertyValueIterable(
                        txn,
                        (parameters[0] as String).toInt(),
                        (parameters[1] as String).toInt(),
                        parameters[2] as Comparable<*>
                    )
                }
            }
        }
    }

    override fun canBeCached() = forceCached || value is Boolean || super.canBeCached()

    override fun getIteratorImpl(txn: StoreTransaction): EntityIteratorBase {
        val it = propertyValueIndex
        if (it.isCachedInstance) {
            val cached = it as UpdatablePropertiesCachedInstanceIterable
            return if (value.javaClass != cached.getPropertyValueClass()) {
                EntityIteratorBase.EMPTY
            } else {
                cached.getPropertyValueIterator(value as Comparable<Any>)
            }
        }
        val valueIdx = openCursor(txn) ?: return EntityIteratorBase.EMPTY
        return PropertyValueIterator(valueIdx, reverse = false)
    }

    override fun getReverseIteratorImpl(txn: StoreTransaction): EntityIterator {
        val it = propertyValueIndex
        if (it.isCachedInstance) {
            val cached = it as UpdatablePropertiesCachedInstanceIterable
            return if (value.javaClass != cached.getPropertyValueClass()) {
                EntityIteratorBase.EMPTY
            } else {
                forceCached = true
                getOrCreateCachedInstance(txn).getReverseIteratorImpl(txn)
            }
        }
        val valueIdx = openCursor(txn) ?: return EntityIteratorBase.EMPTY
        return PropertyValueIterator(valueIdx, reverse = true)
    }

    override fun nonCachedHasFastCountAndIsEmpty() = true

    override fun getHandleImpl(): EntityIterableHandle {
        val entityTypeId = entityTypeId
        val propertyId = propertyId
        return object : ConstantEntityIterableHandle(store, EntityIterableType.ENTITIES_BY_PROP_VALUE) {

            override fun getPropertyIds() = intArrayOf(propertyId)

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append(entityTypeId)
                builder.append('-')
                builder.append(propertyId)
                builder.append('-')
                builder.append(value.toString())
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.apply(entityTypeId)
                hash.applyDelimiter()
                hash.apply(propertyId)
                hash.applyDelimiter()
                hash.apply(value.toString())
            }

            override fun getEntityTypeId() = entityTypeId

            override fun isMatchedPropertyChanged(
                id: EntityId,
                propId: Int,
                oldValue: Comparable<*>?,
                newValue: Comparable<*>?
            ): Boolean {
                return propertyId == propId && entityTypeId == id.typeId && (isValueMatched(oldValue) || isValueMatched(
                    newValue
                ))
            }

            private fun isValueMatched(value: Comparable<*>?): Boolean {
                value ?: return false
                if (value is ComparableSet<*>) {
                    return value.containsItem(this@PropertyValueIterable.value)
                }
                if (value.javaClass != valueClass) return false
                return PropertyTypes.toLowerCase(value).compareTo(this@PropertyValueIterable.value) == 0
            }
        }
    }

    override fun countImpl(txn: StoreTransaction): Long {
        val key = storeImpl.propertyTypes.dataToPropertyValue(value).dataToEntry()
        val valueIdx = openCursor(txn)
        return if (valueIdx == null) 0 else SingleKeyCursorCounter(valueIdx, key).count
    }

    override fun isEmptyImpl(txn: StoreTransaction): Boolean {
        val key = storeImpl.propertyTypes.dataToPropertyValue(value).dataToEntry()
        val valueIdx = openCursor(txn)
        return valueIdx == null || SingleKeyCursorIsEmptyChecker(valueIdx, key).isEmpty
    }

    private inner class PropertyValueIterator(
        cursor: Cursor,
        val reverse: Boolean = false
    ) : EntityIteratorBase(this@PropertyValueIterable) {

        private var hasNext = false
        private val valueBytes: ArrayByteIterable

        init {
            setCursor(cursor)
            val binding = storeImpl.propertyTypes.dataToPropertyValue(value).binding
            valueBytes = binding.objectToEntry(value)
            val entry = cursor.getSearchKey(valueBytes)
            if (entry != null) {
                if (reverse) {
                    if (cursor.nextNoDup) {
                        checkHasNext(cursor.prev)
                    } else {
                        checkHasNext(cursor.last)
                    }
                } else {
                    checkHasNext(true)
                }
            }
        }

        public override fun hasNextImpl() = hasNext

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                explain(EntityIterableType.ENTITIES_BY_PROP_VALUE)
                val cursor = cursor
                return PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(cursor.value)).also {
                    checkHasNext(if (reverse) cursor.prevNoDup else cursor.nextDup)
                }
            }
            return null
        }

        private fun checkHasNext(success: Boolean) {
            hasNext = success && valueBytes.compareTo(cursor.key) == 0
        }
    }
}
