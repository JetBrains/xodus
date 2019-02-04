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

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.util.EntityIdSetFactory

internal class FilterEntitiesWithCertainLinkIterable(txn: PersistentStoreTransaction,
                                                     private val entityTypeId: Int,
                                                     private val linkId: Int,
                                                     filter: EntityIterableBase) : EntityIterableBase(txn) {

    private val entitiesWithLink = EntitiesWithCertainLinkIterable(txn, entityTypeId, linkId)
    private val filter: EntityIterableBase = filter.source

    override fun getEntityTypeId() = entityTypeId

    override fun isSortedById() = entitiesWithLink.isSortedById

    override fun canBeReordered() = true

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val idSet = filter.toSet(txn)
        val it = entitiesWithLink.iterator() as LinksIteratorWithTarget
        return object : EntityIteratorBase(this) {

            private var distinctIds = EntityIdSetFactory.newSet()
            private var id = nextAvailableId()

            override fun hasNextImpl() = id != null

            override fun nextIdImpl(): EntityId? {
                val result = id
                distinctIds = distinctIds.add(result)
                id = nextAvailableId()
                return result
            }

            private fun nextAvailableId(): EntityId? {
                while (it.hasNext()) {
                    val next = it.nextId()
                    if (!distinctIds.contains(next) && idSet.contains(it.targetId)) {
                        return next
                    }
                }
                return null
            }
        }.apply {
            it.cursor?.let {
                cursor = it
            }
        }
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return object : EntityIterableHandleDecorator(store, EntityIterableType.FILTER_LINKS, entitiesWithLink.handle) {

            private val linkIds = mergeFieldIds(intArrayOf(linkId), filter.handle.linkIds)

            override fun getLinkIds() = linkIds

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                applyDecoratedToBuilder(builder)
                builder.append('-')
                (filter.handle as EntityIterableHandleBase).toString(builder)
            }

            override fun hashCode(hash: EntityIterableHandleBase.EntityIterableHandleHash) {
                super.hashCode(hash)
                hash.applyDelimiter()
                hash.apply(filter.handle)
            }

            override fun isMatchedLinkAdded(source: EntityId, target: EntityId, linkId: Int) =
                    decorated.isMatchedLinkAdded(source, target, linkId) ||
                            filter.handle.isMatchedLinkAdded(source, target, linkId)

            override fun isMatchedLinkDeleted(source: EntityId, target: EntityId, id: Int) =
                    decorated.isMatchedLinkDeleted(source, target, id) ||
                            filter.handle.isMatchedLinkDeleted(source, target, id)
        }
    }
}