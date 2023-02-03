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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.cached.SingleTypeUnsortedEntityIdArrayCachedInstanceIterable
import jetbrains.exodus.entitystore.tables.LinkValue
import jetbrains.exodus.entitystore.tables.PropertyKey
import jetbrains.exodus.kotlin.notNull

/**
 * Iterates over entities of specified entity type having specified link to a targetId.
 */
internal class EntitiesWithCertainLinkIterable(txn: PersistentStoreTransaction,
                                               private val entityTypeId: Int,
                                               internal val linkId: Int) : EntityIterableBase(txn) {

    override fun getEntityTypeId() = entityTypeId

    override fun getIteratorImpl(txn: PersistentStoreTransaction): LinksIteratorWithTarget = LinksIterator(txn)

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): LinksIteratorWithTarget = LinksIterator(txn, reverse = true)

    override fun getHandleImpl(): EntityIterableHandle {
        return object : ConstantEntityIterableHandle(store, type) {

            override fun getLinkIds() = intArrayOf(linkId)

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append('-')
                builder.append(entityTypeId)
                builder.append('-')
                builder.append(linkId)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.applyDelimiter()
                hash.apply(entityTypeId)
                hash.applyDelimiter()
                hash.apply(linkId)
            }

            override fun getEntityTypeId() = this@EntitiesWithCertainLinkIterable.entityTypeId

            override fun isMatchedLinkAdded(source: EntityId, target: EntityId, linkId: Int) =
                    entityTypeId == source.typeId && this@EntitiesWithCertainLinkIterable.linkId == linkId

            override fun isMatchedLinkDeleted(source: EntityId, target: EntityId, linkId: Int) =
                    isMatchedLinkAdded(source, target, linkId)
        }
    }

    override fun isSortedById() = false

    override fun createCachedInstance(txn: PersistentStoreTransaction): CachedInstanceIterable {
        val localIds = LongArrayList()
        val targets = ArrayList<EntityId>()
        val it = getIteratorImpl(txn)
        var min = java.lang.Long.MAX_VALUE
        var max = java.lang.Long.MIN_VALUE
        while (it.hasNext()) {
            val localId = it.nextId().notNull.localId
            localIds.add(localId)
            targets.add(it.targetId)
            if (min > localId) {
                min = localId
            }
            if (max < localId) {
                max = localId
            }
        }
        return CachedLinksIterable(txn, localIds.toArray(), targets.toTypedArray(), min, max)
    }

    private fun openCursor(txn: PersistentStoreTransaction) = store.getLinksSecondIndexCursor(txn, entityTypeId)

    private inner class LinksIterator constructor(txn: PersistentStoreTransaction, private val reverse: Boolean = false)
        : LinksIteratorWithTarget(this@EntitiesWithCertainLinkIterable) {

        private var key: PropertyKey? = null
        override lateinit var targetId: EntityId
            private set

        init {
            val index = openCursor(txn).also { cursor = it }
            val idBound = PersistentEntityId(0, 0L)
            if (reverse) {
                if (if (index.getSearchKeyRange(LinkValue.linkValueToEntry(LinkValue(idBound, linkId + 1))) == null) {
                            index.last
                        } else {
                            index.prev
                        }) {
                    loadCursorState()
                }
            } else {
                if (index.getSearchKeyRange(LinkValue.linkValueToEntry(LinkValue(idBound, linkId))) != null) {
                    loadCursorState()
                }
            }
        }

        public override fun hasNextImpl(): Boolean {
            key?.let {
                return true
            }
            if (if (reverse) cursor.prev else cursor.next) {
                loadCursorState()
            }
            return key != null
        }

        public override fun nextIdImpl(): EntityId? {
            key?.let {
                val result = PersistentEntityId(entityTypeId, it.entityLocalId)
                key = null
                return result
            }
            return null
        }

        private fun loadCursorState() {
            val cursor = cursor
            val link = LinkValue.entryToLinkValue(cursor.key)
            if (link.linkId == linkId) {
                key = PropertyKey.entryToPropertyKey(cursor.value)
                targetId = link.entityId
            }
        }
    }

    private inner class CachedLinksIterable internal constructor(txn: PersistentStoreTransaction,
                                                                 private val localIds: LongArray,
                                                                 private val targets: Array<EntityId>,
                                                                 min: Long, max: Long)
        : SingleTypeUnsortedEntityIdArrayCachedInstanceIterable(txn, this@EntitiesWithCertainLinkIterable, entityTypeId, localIds, null, min, max) {

        override fun getIteratorImpl(txn: PersistentStoreTransaction): LinksIteratorWithTarget {
            return object : LinksIteratorWithTarget(this@CachedLinksIterable) {

                private var i = 0
                override lateinit var targetId: EntityId
                    private set

                override fun hasNextImpl() = i < localIds.size

                override fun nextIdImpl(): EntityId? {
                    targetId = targets[i]
                    return PersistentEntityId(entityTypeId, localIds[i++])
                }
            }
        }
    }

    companion object {

        init {
            registerType(type) { txn, _, parameters ->
                EntitiesWithCertainLinkIterable(txn,
                        Integer.valueOf(parameters[0] as String),
                        Integer.valueOf(parameters[1] as String))
            }
        }

        val type: EntityIterableType
            get() = EntityIterableType.ENTITIES_WITH_CERTAIN_LINK
    }
}

internal abstract class LinksIteratorWithTarget(iterable: EntityIterableBase) : EntityIteratorBase(iterable) {

    abstract val targetId: EntityId
}