/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterableType
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.tables.LinkValue
import jetbrains.exodus.entitystore.tables.PropertyKey
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.util.LightOutputStream

class SelectDistinctIterable(txn: PersistentStoreTransaction,
                             source: EntityIterableBase,
                             private val linkId: Int) : EntityIterableDecoratorBase(txn, source) {

    override fun isEmpty() = source.isEmpty

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase = SelectDistinctIterator(txn)

    override fun getHandleImpl(): EntityIterableHandle {
        return object : EntityIterableHandleDecorator(store, type, source.handle) {

            private val linkIds = EntityIterableHandleBase.mergeFieldIds(intArrayOf(linkId), decorated.linkIds)

            override fun getLinkIds() = linkIds

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                applyDecoratedToBuilder(builder)
                builder.append('-')
                builder.append(linkId)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                super.hashCode(hash)
                hash.applyDelimiter()
                hash.apply(linkId)
            }

            override fun isMatchedLinkAdded(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return linkId == this@SelectDistinctIterable.linkId || decorated.isMatchedLinkAdded(source, target, linkId)
            }

            override fun isMatchedLinkDeleted(source: EntityId, target: EntityId, linkId: Int): Boolean {
                return linkId == this@SelectDistinctIterable.linkId || decorated.isMatchedLinkDeleted(source, target, linkId)
            }
        }
    }

    private inner class SelectDistinctIterator constructor(private val txn: PersistentStoreTransaction) : EntityIteratorBase(this@SelectDistinctIterable), SourceMappingIterator {

        private val sourceIt = source.iterator() as EntityIteratorBase
        private val usedCursors = IntHashMap<Cursor>(6, 2f)
        private val auxStream = LightOutputStream()
        private val auxArray = IntArray(8)
        private var iterated = EntityIdSetFactory.newSet()
        private lateinit var srcId: EntityId
        private var nextId: EntityId? = null
        private var hasNext: Boolean = false
        private var hasNextValid: Boolean = false

        override fun hasNextImpl(): Boolean {
            if (!hasNextValid) {
                hasNextValid = true
                hasNext = advance()
            }
            return hasNext
        }

        public override fun nextIdImpl(): EntityId? {
            val nextId = this.nextId
            iterated = iterated.add(nextId)
            hasNextValid = false
            return nextId
        }

        private fun advance(): Boolean {
            if (linkId < 0) {
                return !iterated.contains(null) && sourceIt.hasNext()
            }
            while (sourceIt.hasNext()) {
                val nextSourceId = sourceIt.nextId() ?: continue
                srcId = nextSourceId
                val typeId = nextSourceId.typeId
                val cursor = usedCursors.get(typeId)
                        ?: store.getLinksFirstIndexCursor(txn, typeId).also { usedCursors[typeId] = it }
                val keyEntry = PropertyKey.propertyKeyToEntry(auxStream, auxArray, nextSourceId.localId, linkId)
                val value = cursor.getSearchKey(keyEntry)
                if (value == null) {
                    if (!iterated.contains(null)) {
                        nextId = null
                        return true
                    }
                } else {
                    val linkValue = LinkValue.entryToLinkValue(value)
                    val nextId = linkValue.entityId
                    if (!iterated.contains(nextId)) {
                        this.nextId = nextId
                        return true
                    }
                }
            }
            return false
        }

        override fun dispose(): Boolean {
            sourceIt.disposeIfShouldBe()
            return super.dispose() && usedCursors.values.forEach { it.close() }.let { true }
        }

        override fun toSet() = iterated

        override fun getSourceId() = srcId
    }

    companion object {

        init {
            EntityIterableBase.registerType(type) { txn, _, parameters ->
                SelectDistinctIterable(txn,
                        parameters[1] as EntityIterableBase, Integer.valueOf(parameters[0] as String))
            }
        }

        val type: EntityIterableType get() = EntityIterableType.SELECT_DISTINCT
    }
}
