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

import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterableType
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.tables.LinkValue
import jetbrains.exodus.entitystore.tables.PropertyKey
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.util.LightOutputStream
import java.util.*

class SelectManyIterable(txn: PersistentStoreTransaction,
                         source: EntityIterableBase,
                         private val linkId: Int,
                         private val distinct: Boolean = true) : EntityIterableDecoratorBase(txn, source) {

    override fun canBeCached() = super.canBeCached() && distinct

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase = SelectManyDistinctIterator(txn)

    override fun getHandleImpl(): EntityIterableHandle {
        return object : EntityIterableHandleDecorator(store, type, source.handle) {

            private val linkIds = EntityIterableHandleBase.mergeFieldIds(intArrayOf(linkId), decorated.linkIds)

            override fun getLinkIds() = linkIds

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                applyDecoratedToBuilder(builder)
                builder.append('-')
                builder.append(linkId)
                builder.append('-')
                builder.append(distinct)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                super.hashCode(hash)
                hash.applyDelimiter()
                hash.apply(linkId)
                hash.applyDelimiter()
                hash.apply((if (distinct) 1 else 0).toByte())
            }

            override fun isMatchedLinkAdded(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return linkId == this@SelectManyIterable.linkId || decorated.isMatchedLinkAdded(source, target, linkId)
            }

            override fun isMatchedLinkDeleted(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return linkId == this@SelectManyIterable.linkId || decorated.isMatchedLinkDeleted(source, target, linkId)
            }
        }
    }

    private inner class SelectManyDistinctIterator constructor(private val txn: PersistentStoreTransaction) : EntityIteratorBase(this@SelectManyIterable), SourceMappingIterator {

        private val sourceIt = source.iterator() as EntityIteratorBase
        private val usedCursors = IntHashMap<Cursor>(6, 2f)
        private val ids = ArrayDeque<Pair<EntityId, EntityId?>>()
        private val auxStream = LightOutputStream()
        private val auxArray = IntArray(8)
        private var idsCollected = false
        private lateinit var srcId: EntityId

        override fun hasNextImpl(): Boolean {
            if (!idsCollected) {
                idsCollected = true
                collectIds()
            }
            return !ids.isEmpty()
        }

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                val pair = ids.poll()
                srcId = pair.first
                return pair.second
            }
            return null
        }

        override fun dispose(): Boolean {
            sourceIt.disposeIfShouldBe()
            return super.dispose() && usedCursors.values.forEach { it.close() }.let { true }
        }

        override fun getSourceId() = srcId

        private fun collectIds() {
            val sourceIt = this.sourceIt
            val linkId = this@SelectManyIterable.linkId
            if (linkId >= 0) {
                var usedIds = if (distinct) EntityIdSetFactory.newSet() else EntityIdSetFactory.newImmutableSet()
                while (sourceIt.hasNext()) {
                    val sourceId = sourceIt.nextId() ?: continue
                    val typeId = sourceId.typeId
                    val cursor = usedCursors.get(typeId)
                            ?: store.getLinksFirstIndexCursor(txn, typeId).also { usedCursors[typeId] = it }
                    val sourceLocalId = sourceId.localId
                    var value = cursor.getSearchKey(
                            PropertyKey.propertyKeyToEntry(auxStream, auxArray, sourceLocalId, linkId))
                    if (value == null) {
                        if (!usedIds.contains(null)) {
                            usedIds = usedIds.add(null)
                            ids.add(sourceId to null)
                        }
                    } else {
                        while (true) {
                            val linkValue = LinkValue.entryToLinkValue(value.notNull)
                            val nextId = linkValue.entityId
                            if (!usedIds.contains(nextId)) {
                                usedIds = usedIds.add(nextId)
                                ids.add(sourceId to nextId)
                            }
                            if (!cursor.next) {
                                break
                            }
                            val key = PropertyKey.entryToPropertyKey(cursor.key)
                            if (key.propertyId != linkId || key.entityLocalId != sourceLocalId) {
                                break
                            }
                            value = cursor.value
                        }
                    }
                }
            }
        }
    }

    companion object {

        init {
            EntityIterableBase.registerType(type) { txn, _, parameters ->
                SelectManyIterable(txn,
                        parameters[1] as EntityIterableBase, Integer.valueOf(parameters[0] as String))
            }
        }

        val type: EntityIterableType get() = EntityIterableType.SELECTMANY_DISTINCT
    }
}
