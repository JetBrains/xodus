/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.tables.LinkValue
import jetbrains.exodus.entitystore.tables.PropertyKey
import jetbrains.exodus.env.Cursor

/**
 * Iterates all entities of specified type which are linked to specified entity with specified link.
 */
class EntityToLinksIterable(
    txn: PersistentStoreTransaction, entityId: EntityId, private val typeId: Int, private val linkId: Int
) : EntityLinksIterableBase(txn, entityId) {

    override fun getEntityTypeId() = typeId

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase = LinksIterator(openCursor(txn))

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase =
        LinksReverseIterator(openCursor(txn))

    override fun nonCachedHasFastCountAndIsEmpty() = true

    override fun getHandleImpl(): EntityIterableHandle {
        return object : ConstantEntityIterableHandle(store, EntityIterableType.ENTITY_TO_LINKS) {

            override fun getLinkIds() = intArrayOf(linkId)

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                (entityId as PersistentEntityId).toString(builder)
                builder.append('-')
                builder.append(typeId)
                builder.append('-')
                builder.append(linkId)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                (entityId as PersistentEntityId).toHash(hash)
                hash.applyDelimiter()
                hash.apply(typeId)
                hash.applyDelimiter()
                hash.apply(linkId)
            }

            override fun getEntityTypeId() = typeId

            override fun isMatchedLinkAdded(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return typeId == source.typeId && entityId == target
            }

            override fun isMatchedLinkDeleted(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return isMatchedLinkAdded(source, target, linkId)
            }
        }
    }

    override fun getLast(): Entity? {
        val txn = store.andCheckCurrentTransaction
        openCursor(txn).use { cursor ->
            if (navigateToLast(cursor))  {
                return txn.getEntity(
                    PersistentEntityId(typeId, PropertyKey.entryToPropertyKey(cursor.value).entityLocalId))
            }
        }
        return null
    }

    override fun countImpl(txn: PersistentStoreTransaction) = SingleKeyCursorCounter(openCursor(txn), firstKey).count

    override fun isEmptyImpl(txn: PersistentStoreTransaction) =
        SingleKeyCursorIsEmptyChecker(openCursor(txn), firstKey).isEmpty

    private fun openCursor(txn: PersistentStoreTransaction): Cursor {
        return store.getLinksSecondIndexCursor(txn, typeId)
    }

    private val firstKey: ByteIterable get() = getKey(entityId)

    private fun getKey(entityId: EntityId) = LinkValue.linkValueToEntry(LinkValue(entityId, linkId))

    private fun navigateToLast(cursor: Cursor): Boolean {
        val lastKey = getKey(PersistentEntityId(entityId.typeId, entityId.localId + 1))
        if (cursor.getSearchKeyRange(lastKey) == null) {
            if (!cursor.last) {
                return false
            }
        } else {
            if (!cursor.prev) {
                return false
            }
        }
        val key = LinkValue.entryToLinkValue(cursor.key)
        return key.entityId == entityId && key.linkId == linkId
    }

    private inner class LinksIterator(index: Cursor) : EntityIteratorBase(this@EntityToLinksIterable) {

        init {
            cursor = index
        }

        private var hasNext = index.getSearchKey(firstKey) != null

        public override fun hasNextImpl() = hasNext

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                explain(EntityIterableType.ENTITY_TO_LINKS)
                return PersistentEntityId(typeId, PropertyKey.entryToPropertyKey(cursor.value).entityLocalId).also {
                    hasNext = cursor.nextDup
                }
            }
            return null
        }
    }

    private inner class LinksReverseIterator(index: Cursor) : EntityIteratorBase(this@EntityToLinksIterable) {

        init {
            cursor = index
        }

        private var hasNext = navigateToLast(index)

        public override fun hasNextImpl() = hasNext

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                explain(EntityIterableType.ENTITY_TO_LINKS)
                return PersistentEntityId(typeId, PropertyKey.entryToPropertyKey(cursor.value).entityLocalId).also {
                    hasNext = cursor.prevDup
                }
            }
            return null
        }
    }

    companion object {
        init {
            registerType(EntityIterableType.ENTITY_TO_LINKS) { txn, store, parameters ->
                EntityToLinksIterable(
                    txn,
                    PersistentEntityId((parameters[0] as String).toInt(), (parameters[1] as String).toLong()),
                    (parameters[2] as String).toInt(),
                    (parameters[3] as String).toInt()
                )
            }
        }
    }
}
