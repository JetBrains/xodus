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

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.kotlin.*

class PropertyContainsValueEntityIterable(
    txn: PersistentStoreTransaction,
    entityTypeId: Int,
    propertyId: Int,
    private val value: String,
    private val ignoreCase: Boolean
) : PropertyRangeOrValueIterableBase(txn, entityTypeId, propertyId) {

    private val useOptimzedContains = txn.environmentTransaction.environment.environmentConfig.envQueryOptimizedContains

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val iterator = propertyValueIndex.iterator()

        return if (iterator.hasNext()) {
            return PropertyContainsValueIterator(iterator as PropertyValueIterator)
        } else {
            EntityIteratorBase.EMPTY
        }
    }


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

            override fun isMatchedPropertyChanged(
                id: EntityId,
                propId: Int,
                oldValue: Comparable<*>?,
                newValue: Comparable<*>?
            ): Boolean {
                return propertyId == propId && entityTypeId == id.typeId &&
                        (isValueMatched(oldValue) || isValueMatched(newValue))
            }

            private fun isValueMatched(value: Comparable<*>?): Boolean {
                return (value as? String)?.contains(this@PropertyContainsValueEntityIterable.value, ignoreCase) ?: false
            }
        }
    }

    override fun isSortedById(): Boolean {
        return false
    }

    private inner class PropertyContainsValueIterator(private val index: PropertyValueIterator) :
        EntityIteratorBase(this), PropertyValueIterator {

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
                        val contains =
                            if (useOptimzedContains && ignoreCase)
                                containsOptimized(it)
                            else it.contains(value, ignoreCase)

                        if (contains) {
                            currentValue = it
                            return
                        }
                    }
                }
                nextId = PersistentEntityId.EMPTY_ID
            }
        }

        /**
         * This is 'String.contains(other, ignoreCase = true)' implementation
         * that is twice as fast as the one from the standard library,
         * but it comes with the price of incorrect matching of some symbols such as `İ`.
         * Thus it is disabled by default and enabled where it is not an issue.
         * See JT-81377.
         */
        private fun containsOptimized(property: String): Boolean {
            val endLimit: Int = property.length - value.length + 1
            if (endLimit < 0) {
                return false
            }
            if (value.length == 0) {
                return true
            }

            outer@ for (i in 0 until endLimit) {
                for (j in 0 until value.length) {
                    if (property[i + j].lowercaseChar() != value[j].lowercaseChar()) {
                        continue@outer
                    }
                }
                return true
            }

            return false
        }
    }

    companion object {

        private fun getType(): EntityIterableType = EntityIterableType.ENTITIES_WITH_PROP_CONTAINING_VALUE

        init {
            registerType(getType()) { txn, store, parameters ->
                PropertyContainsValueEntityIterable(
                    txn,
                    (parameters[0] as String).toInt(),
                    (parameters[1] as String).toInt(),
                    parameters[2] as String, true
                )
            }
        }
    }
}