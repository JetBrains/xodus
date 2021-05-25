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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.registerType
import jetbrains.exodus.env.BitmapIterator
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.kotlin.notNull

/**
 * Iterates all entities of specified entity type in range of local ids.
 */
class EntitiesOfTypeRangeIterable(
    txn: PersistentStoreTransaction,
    private val entityTypeId: Int, private val min: Long, private val max: Long
) : EntityIterableBase(txn) {

    override fun getEntityTypeId() = entityTypeId

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        return if (store.useVersion1Format()) {
            EntitiesOfTypeIterator(this, openCursor(txn))
        } else {
            EntitiesOfTypeBitmapIterator(this, openBitmapIterator(txn))
        }
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return object : ConstantEntityIterableHandle(store, type) {
            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append(entityTypeId)
                builder.append('-')
                builder.append(min)
                builder.append('-')
                builder.append(max)
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.apply(entityTypeId)
                hash.applyDelimiter()
                hash.apply(min)
                hash.applyDelimiter()
                hash.apply(max)
            }

            override fun getTypeIdsAffectingCreation(): IntArray {
                // TODO: if open ranges are prohibited, we can improve this
                return intArrayOf(entityTypeId)
            }

            override fun getEntityTypeId() = this@EntitiesOfTypeRangeIterable.entityTypeId

            override fun isMatchedEntityAdded(added: EntityId) =
                added.typeId == entityTypeId && isRangeAffected(added.localId)

            override fun isMatchedEntityDeleted(deleted: EntityId) =
                deleted.typeId == entityTypeId && isRangeAffected(deleted.localId)

            private fun isRangeAffected(id: Long) = id in min..max
        }
    }

    override fun countImpl(txn: PersistentStoreTransaction): Long {
        return if (store.useVersion1Format()) {
            openCursor(txn).use { cursor ->
                val key: ByteIterable = LongBinding.longToCompressedEntry(min)
                var result: Long = 0
                var success: Boolean = cursor.getSearchKeyRange(key) != null
                while (success) {
                    if (max > LongBinding.compressedEntryToLong(cursor.key)) {
                        break
                    }
                    result++
                    success = cursor.nextNoDup
                }
                result
            }
        } else {
            store.getEntitiesBitmapTable(txn, entityTypeId).count(txn.environmentTransaction, min, max)
        }
    }

    private fun openCursor(txn: PersistentStoreTransaction) = store.getEntitiesIndexCursor(txn, entityTypeId)

    private fun openBitmapIterator(txn: PersistentStoreTransaction) = store.getEntitiesBitmapIterator(txn, entityTypeId)

    private inner class EntitiesOfTypeIterator(iterable: EntitiesOfTypeRangeIterable, index: Cursor) :
        EntityIteratorBase(iterable) {

        private var hasNext: Boolean = false

        init {
            cursor = index
            val key: ByteIterable = LongBinding.longToCompressedEntry(min)
            checkHasNext(cursor.getSearchKeyRange(key) != null)
        }

        public override fun hasNextImpl() = hasNext

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                explain(type)
                val cursor: Cursor = cursor
                val localId: Long = LongBinding.compressedEntryToLong(cursor.key)
                val result: EntityId = PersistentEntityId(entityTypeId, localId)
                checkHasNext(cursor.next)
                return result
            }
            return null
        }

        private fun checkHasNext(success: Boolean) {
            hasNext = success && max >= LongBinding.compressedEntryToLong(cursor.key)
        }
    }

    private inner class EntitiesOfTypeBitmapIterator(iterable: EntitiesOfTypeRangeIterable, val it: BitmapIterator) :
        EntityIteratorBase(iterable) {

        private var next: EntityId? = null

        init {
            cursor = it.cursor
            if (it.getSearchBit(min)) {
                checkNext()
            }
        }

        override fun hasNextImpl() = next != null

        override fun nextIdImpl() = next.also { checkNext() }

        private fun checkNext() {
            next = null
            if (it.hasNext()) {
                it.next().let { localId ->
                    if (max >= localId) {
                        next = PersistentEntityId(entityTypeId, localId)
                    }
                }
            }
        }
    }

    companion object {

        val type: EntityIterableType get() = EntityIterableType.ALL_ENTITIES_RANGE

        init {
            registerType(type) { txn, store, parameters ->
                val min: Long = (parameters[1] as String?).notNull.toLong()
                val max: Long = (parameters[2] as String?).notNull.toLong()
                EntitiesOfTypeRangeIterable(txn, (parameters[0] as String?).notNull.toInt(), min, max)
            }
        }
    }
}
