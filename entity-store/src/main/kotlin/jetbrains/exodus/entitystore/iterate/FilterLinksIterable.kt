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

import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.tables.LinkValue
import jetbrains.exodus.entitystore.tables.PropertyKey
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.env.Cursor

class FilterLinksIterable(txn: PersistentStoreTransaction,
                          private val linkId: Int,
                          source: EntityIterableBase,
                          entities: EntityIterable) : EntityIterableDecoratorBase(txn, source) {

    private val entities: EntityIterableBase = (entities as EntityIterableBase).source

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        return EntityIteratorFixingDecorator(this, object : EntityIteratorBase(this) {

            private val sourceIt = source.iterator() as EntityIteratorBase
            private val usedCursors = IntHashMap<Cursor>(6, 2f)
            private val idSet by lazy { entities.toSet(txn) }
            private var nextId: EntityId? = PersistentEntityId.EMPTY_ID

            override fun hasNextImpl(): Boolean {
                if (nextId !== PersistentEntityId.EMPTY_ID) {
                    return true
                }
                while (sourceIt.hasNext()) {
                    val id = sourceIt.nextId()
                    nextId = id
                    if (id != null) {
                        val typeId = id.typeId
                        var cursor = usedCursors.get(typeId)
                        if (cursor == null) {
                            cursor = store.getLinksFirstIndexCursor(txn, typeId)
                            usedCursors[typeId] = cursor
                        }
                        val value = cursor.getSearchKey(PropertyKey.propertyKeyToEntry(PropertyKey(id.localId, linkId)))
                        if (value != null) {
                            val linkValue = LinkValue.entryToLinkValue(value)
                            val targetId = linkValue.entityId
                            if (idSet.contains(targetId)) {
                                return true
                            }
                        }
                    }
                }
                return false
            }

            override fun nextIdImpl(): EntityId? {
                val result = nextId
                nextId = PersistentEntityId.EMPTY_ID
                return result
            }

            override fun dispose(): Boolean {
                sourceIt.disposeIfShouldBe()
                return super.dispose() && usedCursors.forEachValue {
                    it.close()
                    true
                }
            }
        })
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return object : EntityIterableHandleDecorator(store, EntityIterableType.FILTER_LINKS, source.handle) {

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append(linkId)
                builder.append('-')
                applyDecoratedToBuilder(builder)
                builder.append('-')
                (entities.handle as EntityIterableHandleBase).toString(builder)
            }

            override fun hashCode(hash: EntityIterableHandleBase.EntityIterableHandleHash) {
                hash.apply(linkId)
                hash.applyDelimiter()
                super.hashCode(hash)
                hash.applyDelimiter()
                hash.apply(entities.handle)
            }
        }
    }

    override fun isSortedById(): Boolean {
        return source.isSortedById
    }

    override fun canBeCached(): Boolean {
        return false
    }

    companion object {

        init {
            EntityIterableBase.registerType(EntityIterableType.FILTER_LINKS) { txn, _, parameters ->
                FilterLinksIterable(txn, Integer.valueOf(parameters[0] as String),
                        parameters[1] as EntityIterableBase, parameters[2] as EntityIterable)
            }
        }
    }
}


class EntityIdSetIterable(txn: PersistentStoreTransaction) : EntityIterableBase(txn) {

    val h = EntityIterableHandleBase.EntityIterableHandleHash(store)
    var ids: EntityIdSet = EntityIdSetFactory.newSet()

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        throw NotImplementedError()
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return object : ConstantEntityIterableHandle(store, EntityIterableType.FILTER_ENTITY_TYPE) {
            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.apply(h)
            }
        }
    }

    override fun size() = ids.count().toLong()

    override fun count() = ids.count().toLong()

    override fun getRoughCount() = ids.count().toLong()

    override fun getRoughSize() = ids.count().toLong()

    override fun toSet(txn: PersistentStoreTransaction) = ids

    fun addTarget(id: EntityId) {
        ids = ids.add(id)
        h.apply(id.typeId)
        h.apply(id.localId)
    }
}