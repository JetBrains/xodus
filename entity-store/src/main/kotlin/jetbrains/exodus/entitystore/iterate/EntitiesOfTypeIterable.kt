/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.registerType
import jetbrains.exodus.env.Cursor

/**
 * Iterates all entities of specified entity type.
 */
open class EntitiesOfTypeIterable(txn: PersistentStoreTransaction, private val entityTypeId: Int) : EntityIterableBase(txn) {

    override fun getEntityTypeId() = entityTypeId

    override fun getIteratorImpl(txn: PersistentStoreTransaction) =
            EntitiesOfTypeIterator(this, store.getEntitiesIndexCursor(txn, entityTypeId))

    override fun nonCachedHasFastCountAndIsEmpty() = true

    override fun findLinks(entities: EntityIterable, linkName: String): EntityIterable {
        val txn = transaction
        val linkId = store.getLinkId(txn, linkName, false)
        if (linkId < 0) {
            return EMPTY
        }
        return FilterEntitiesWithCertainLinkIterable(
                txn, EntitiesWithCertainLinkIterable(txn, entityTypeId, linkId), entities as EntityIterableBase)
    }

    override fun isEmptyImpl(txn: PersistentStoreTransaction) = countImpl(txn) == 0L

    override fun getHandleImpl() = EntitiesOfTypeIterableHandle(this)

    override fun createCachedInstance(txn: PersistentStoreTransaction): CachedInstanceIterable =
            UpdatableEntityIdSortedSetCachedInstanceIterable(txn, this)

    override fun countImpl(txn: PersistentStoreTransaction) =
            store.getEntitiesTable(txn, entityTypeId).count(txn.environmentTransaction)

    class EntitiesOfTypeIterator(iterable: EntitiesOfTypeIterable,
                                 index: Cursor) : EntityIteratorBase(iterable) {

        private var hasNext = false
        private var hasNextValid = false
        private val entityTypeId = iterable.entityTypeId

        private val entityId: EntityId
            get() = PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(cursor.key))

        init {
            cursor = index
        }

        public override fun hasNextImpl(): Boolean {
            if (!hasNextValid) {
                hasNext = cursor.next
                hasNextValid = true
            }
            return hasNext
        }

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                iterable.explain(EntityIterableType.ALL_ENTITIES)
                val result = entityId
                hasNextValid = false
                return result
            }
            return null
        }

        override fun getLast(): EntityId? {
            return if (!cursor.prev) {
                null
            } else entityId
        }
    }

    open class EntitiesOfTypeIterableHandle(source: EntitiesOfTypeIterable)
        : ConstantEntityIterableHandle(source.store, EntityIterableType.ALL_ENTITIES) {

        private val typeId: Int = source.entityTypeId

        override fun toString(builder: StringBuilder) {
            super.toString(builder)
            builder.append(entityTypeId)
        }

        override fun hashCode(hash: EntityIterableHandleHash) {
            hash.apply(entityTypeId)
        }

        override fun getEntityTypeId() = typeId

        override fun getTypeIdsAffectingCreation() = intArrayOf(entityTypeId)

        override fun isMatchedEntityAdded(added: EntityId) = added.typeId == entityTypeId

        override fun isMatchedEntityDeleted(deleted: EntityId) = deleted.typeId == entityTypeId

        override fun onEntityAdded(handleChecker: EntityAddedOrDeletedHandleChecker): Boolean {
            val iterable = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatableEntityIdSortedSetCachedInstanceIterable::class.java)
            if (iterable != null) {
                iterable.addEntity(handleChecker.id)
                return true
            }
            return false
        }

        override fun onEntityDeleted(handleChecker: EntityAddedOrDeletedHandleChecker): Boolean {
            val iterable = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatableEntityIdSortedSetCachedInstanceIterable::class.java)
            if (iterable != null) {
                iterable.removeEntity(handleChecker.id)
                return true
            }
            return false
        }
    }

    companion object {

        init {
            registerType(EntityIterableType.ALL_ENTITIES) { txn, _, parameters ->
                EntitiesOfTypeIterable(txn, Integer.valueOf(parameters[0] as String))
            }
        }
    }
}
