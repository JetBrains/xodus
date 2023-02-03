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
package jetbrains.exodus.entitystore.iterate.binop

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.*
import jetbrains.exodus.kotlin.notNull

class IntersectionIterable @JvmOverloads constructor(
    txn: PersistentStoreTransaction?,
    iterable1: EntityIterableBase,
    iterable2: EntityIterableBase,
    preserveRightOrder: Boolean = false
) : BinaryOperatorEntityIterable(txn, iterable1, iterable2, !preserveRightOrder) {

    init {
        if (preserveRightOrder) {
            if (this.iterable2.isSortedById) {
                depth += SORTED_BY_ID_FLAG
            }
        } else {
            if (iterable1.isSortedById || iterable2.isSortedById) {
                depth += SORTED_BY_ID_FLAG
            }
        }
    }

    override fun getIterableType() = EntityIterableType.INTERSECT

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        val iterator = if (isSortedById) {
            if (iterable1.isSortedById) {
                if (iterable2.isSortedById)
                    SortedIterator(this, iterable1, iterable2)
                else
                    UnsortedIterator(this, txn, iterable2, iterable1)
            } else {
                // iterable2 is sorted for sure
                UnsortedIterator(this, txn, iterable1, iterable2)
            }
        } else {
            // both unsorted or order preservation needed
            // XD-821: EntityIdSetIterable cannot be passed as second parameter to ctor of UnsortedIterator
            if (iterable2 is EntityIdSetIterable)
                UnsortedIterator(this, txn, iterable2, iterable1)
            else
                UnsortedIterator(this, txn, iterable1, iterable2)
        }
        return EntityIteratorFixingDecorator(this, iterator)
    }

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        return if (iterable1.isSortedById && iterable2.isSortedById) {
            EntityIteratorFixingDecorator(this, SortedReverseIterator(txn, this, iterable1, iterable2))
        } else {
            super.getReverseIteratorImpl(txn)
        }
    }

    override fun countImpl(txn: PersistentStoreTransaction) = if (isEmptyFast(txn)) 0 else super.countImpl(txn)

    override fun isEmptyImpl(txn: PersistentStoreTransaction) = isEmptyFast(txn) || super.isEmptyImpl(txn)

    override fun isEmptyFast(txn: PersistentStoreTransaction): Boolean {
        return super.isEmptyFast(txn) ||
            ((iterable1.isCached || iterable1.nonCachedHasFastCountAndIsEmpty()) && iterable1.isEmptyImpl(txn)) ||
            ((iterable2.isCached || iterable2.nonCachedHasFastCountAndIsEmpty()) && iterable2.isEmptyImpl(txn))
    }

    private abstract class SortedIteratorBase(
        iterable: EntityIterableBase,
        getIterators: () -> Pair<EntityIteratorBase, EntityIteratorBase>
    ) : NonDisposableEntityIterator(iterable) {

        private val iterator1: EntityIteratorBase
        private val iterator2: EntityIteratorBase
        protected var nextId: EntityId? = null

        init {
            val (it1, it2) = getIterators()
            iterator1 = it1
            iterator2 = it2
        }

        public override fun nextIdImpl() = nextId

        override fun hasNextImpl(): Boolean {
            var next = nextId
            if (next !== PersistentEntityId.EMPTY_ID) {
                next = PersistentEntityId.EMPTY_ID
                var e1: EntityId? = null
                var e2: EntityId? = null
                val iterator1 = this.iterator1
                val iterator2 = this.iterator2
                while (true) {
                    if (e1 == null) {
                        if (!iterator1.hasNext()) {
                            break
                        }
                        e1 = iterator1.nextId()
                    }
                    if (e2 == null) {
                        if (!iterator2.hasNext()) {
                            break
                        }
                        e2 = iterator2.nextId()
                    }
                    // check if single id is null, not both
                    if (e1 !== e2 && (e1 == null || e2 == null)) {
                        continue
                    }
                    val cmp = compare(e1.notNull, e2.notNull)
                    if (cmp < 0) {
                        e1 = null
                    } else if (cmp > 0) {
                        e2 = null
                    } else {
                        next = e1
                        break
                    }
                }
                nextId = next
                return next !== PersistentEntityId.EMPTY_ID
            }
            return false
        }

        abstract fun compare(e1: EntityId, e2: EntityId): Int
    }

    private class SortedIterator(
        iterable: EntityIterableBase, iterable1: EntityIterableBase, iterable2: EntityIterableBase
    ) : SortedIteratorBase(
        iterable,
        {
            (iterable1.iterator() as EntityIteratorBase) to
                (iterable2.iterator() as EntityIteratorBase)
        }) {

        override fun compare(e1: EntityId, e2: EntityId) = if (e1 === e2) 0 else e1.compareTo(e2)
    }

    private class SortedReverseIterator(
        txn: PersistentStoreTransaction,
        iterable: EntityIterableBase,
        iterable1: EntityIterableBase,
        iterable2: EntityIterableBase
    ) : SortedIteratorBase(
        iterable,
        {
            (iterable1.getReverseIteratorImpl(txn) as EntityIteratorBase) to
                (iterable2.getReverseIteratorImpl(txn) as EntityIteratorBase)
        }) {

        override fun compare(e1: EntityId, e2: EntityId) = if (e1 === e2) 0 else e2.compareTo(e1)
    }

    private class UnsortedIterator(
        iterable: EntityIterableBase,
        private val txn: PersistentStoreTransaction,
        private val iterable1: EntityIterableBase,
        iterable2: EntityIterableBase
    ) : NonDisposableEntityIterator(iterable) {
        private val iterator2 = iterable2.iterator() as EntityIteratorBase
        private var entityIdSet: EntityIdSet? = null
        private var nextId: EntityId? = null

        override fun hasNextImpl(): Boolean {
            while (iterator2.hasNext()) {
                val nextId = iterator2.nextId()
                if (getEntityIdSet().contains(nextId)) {
                    this.nextId = nextId
                    return true
                }
            }
            return false
        }

        public override fun nextIdImpl() = nextId

        private fun getEntityIdSet() =
            entityIdSet ?: iterable1.toSet(txn).apply { entityIdSet = this }
    }

    companion object {

        init {
            EntityIterableBase.registerType(EntityIterableType.INTERSECT) { txn, _, parameters ->
                IntersectionIterable(
                    txn,
                    parameters[0] as EntityIterableBase, parameters[1] as EntityIterableBase
                )
            }
        }
    }
}
