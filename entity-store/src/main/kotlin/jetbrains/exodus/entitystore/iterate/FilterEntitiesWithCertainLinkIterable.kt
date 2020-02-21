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
import jetbrains.exodus.entitystore.util.EntityIdSetFactory

// this iterable depth is rather great in order to prevent non-effective commutative binary operators mutation
private const val DEPTH = 1000

internal class FilterEntitiesWithCertainLinkIterable(txn: PersistentStoreTransaction,
                                                     private val entitiesWithLink: EntitiesWithCertainLinkIterable,
                                                     filter: EntityIterableBase) : EntityIterableBase(txn) {

    private val filter: EntityIterableBase = filter.source

    val linkId: Int get() = entitiesWithLink.linkId

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right is FilterEntitiesWithCertainLinkIterable) {
            if ((entitiesWithLink === right.entitiesWithLink) ||
                    (linkId == right.linkId && entityTypeId == right.entityTypeId)) {
                return FilterEntitiesWithCertainLinkIterable(
                        transaction, entitiesWithLink, filter.intersect(right.filter) as EntityIterableBase)
            }
        }
        return super.intersect(right)
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right is FilterEntitiesWithCertainLinkIterable) {
            if ((entitiesWithLink === right.entitiesWithLink) ||
                    (linkId == right.linkId && entityTypeId == right.entityTypeId)) {
                return FilterEntitiesWithCertainLinkIterable(
                        transaction, entitiesWithLink, filter.union(right.filter) as EntityIterableBase)
            }
        }
        return super.union(right)
    }

    override fun getEntityTypeId() = entitiesWithLink.entityTypeId

    override fun isSortedById() = entitiesWithLink.isSortedById

    override fun canBeReordered() = true

    override fun depth() = DEPTH

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        return getIteratorImpl(txn, reverse = false)
    }

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        return getIteratorImpl(txn, reverse = true)
    }

    private fun getIteratorImpl(txn: PersistentStoreTransaction, reverse: Boolean): EntityIteratorBase {
        val idSet = filter.toSet(txn)
        val it = if (reverse) entitiesWithLink.getReverseIteratorImpl(txn) else entitiesWithLink.getIteratorImpl(txn)
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
                    if (idSet.contains(it.targetId) && !distinctIds.contains(next)) {
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

            override fun hashCode(hash: EntityIterableHandleHash) {
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