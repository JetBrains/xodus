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

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.registerType
import jetbrains.exodus.kotlin.notNull

class PropertyContainsValueEntityIterable(txn: PersistentStoreTransaction,
                                          entityTypeId: Int,
                                          propertyId: Int,
                                          private val value: String,
                                          private val ignoreCase: Boolean) : PropertyRangeOrValueIterableBase(txn, entityTypeId, propertyId) {

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator =
            PropertyContainsValueIterator(propertyValueIndex.iterator() as PropertyValueIterator)

    override fun getHandleImpl(): EntityIterableHandle {
        val entityTypeId = entityTypeId
        val propertyId = propertyId
        return object : ConstantEntityIterableHandle(store, getType()) {

            override fun getPropertyIds() = intArrayOf(propertyId)

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append(entityTypeId)
                builder.append('-')
                builder.append(propertyId)
                builder.append('-')
                builder.append(value)
                builder.append('-')
                builder.append(ignoreCase)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.apply(entityTypeId)
                hash.applyDelimiter()
                hash.apply(propertyId)
                hash.applyDelimiter()
                hash.apply(value)
                hash.applyDelimiter()
                hash.apply(ignoreCase.compareTo(false))
            }

            override fun getEntityTypeId() = entityTypeId

            override fun isMatchedPropertyChanged(id: EntityId,
                                                  propId: Int,
                                                  oldValue: Comparable<*>?,
                                                  newValue: Comparable<*>?): Boolean {
                return propertyId == propId && entityTypeId == id.typeId &&
                        (isValueMatched(oldValue) || isValueMatched(newValue))
            }

            private fun isValueMatched(value: Comparable<*>?): Boolean {
                return (value as? String)?.contains(this@PropertyContainsValueEntityIterable.value, ignoreCase) ?: false
            }
        }
    }

    private inner class PropertyContainsValueIterator(private val index: PropertyValueIterator) : EntityIteratorBase(this), PropertyValueIterator {

        private var nextId: EntityId? = null
        private var currentValue: String? = null

        public override fun hasNextImpl(): Boolean {
            advance()
            return nextId != PersistentEntityId.EMPTY_ID
        }

        public override fun nextIdImpl(): EntityId? {
            if (!hasNextImpl()) return null
            explain(getType())
            return nextId.also {
                nextId = null
            }
        }

        override fun currentValue(): Comparable<Nothing> = currentValue.notNull

        private fun advance() {
            nextId ?: run {
                while (index.hasNext()) {
                    nextId = index.nextId()
                    (index.currentValue() as String).let {
                        if (it.contains(value, ignoreCase)) {
                            currentValue = it
                            return
                        }
                    }
                }
                nextId = PersistentEntityId.EMPTY_ID
            }
        }
    }

    companion object {

        private fun getType(): EntityIterableType = EntityIterableType.ENTITIES_WITH_PROP_CONTAINING_VALUE

        init {
            registerType(getType()) { txn, store, parameters ->
                PropertyContainsValueEntityIterable(txn,
                        (parameters[0] as String).toInt(),
                        (parameters[1] as String).toInt(),
                        parameters[2] as String, true)
            }
        }
    }
}