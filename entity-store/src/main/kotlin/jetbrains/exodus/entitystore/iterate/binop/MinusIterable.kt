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
package jetbrains.exodus.entitystore.iterate.binop

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorFixingDecorator
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator
import jetbrains.exodus.kotlin.notNull

class MinusIterable(
    txn: PersistentStoreTransaction?,
    minuend: EntityIterableBase,
    subtrahend: EntityIterableBase
) : BinaryOperatorEntityIterable(txn, minuend, subtrahend, false) {

    init {
        // minuend is always equal to iterable1, 'cause we are not commutative
        if (minuend.isSortedById) {
            depth += SORTED_BY_ID_FLAG
        }
    }

    override fun getIterableType(): EntityIterableType {
        return EntityIterableType.MINUS
    }

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        val iterable1 = iterable1
        val iterable2 = iterable2
        return EntityIteratorFixingDecorator(
            this,
            if (isSortedById && iterable2.isSortedById) SortedIterator(
                this,
                iterable1,
                iterable2
            ) else UnsortedIterator(this, txn, iterable1, iterable2)
        )
    }

    override fun getReverseIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        return if (isSortedById) {
            EntityIteratorFixingDecorator(
                this,
                SortedReverseIterator(this, txn, iterable1, iterable2)
            )
        } else super.getReverseIteratorImpl(txn)
    }

    private abstract class SortedIteratorBase(iterable: EntityIterableBase) : NonDisposableEntityIterator(iterable) {

        protected var nextId: EntityId? = null
        protected var currentMinuend: EntityId? = null
        protected var currentSubtrahend: EntityId? = null

        public override fun nextIdImpl(): EntityId? = nextId
    }

    private class SortedIterator(
        iterable: EntityIterableBase, minuend: EntityIterableBase, subtrahend: EntityIterableBase
    ) : SortedIteratorBase(iterable) {

        private val minuend = minuend.iterator() as EntityIteratorBase
        private val subtrahend = subtrahend.iterator() as EntityIteratorBase

        override fun hasNextImpl(): Boolean {
            var currentMinuend = currentMinuend
            var currentSubtrahend = currentSubtrahend
            while (currentMinuend !== PersistentEntityId.EMPTY_ID) {
                if (currentMinuend == null) {
                    currentMinuend = checkNextId(minuend)
                    this.currentMinuend = currentMinuend
                }
                if (currentSubtrahend == null) {
                    currentSubtrahend = checkNextId(subtrahend)
                    this.currentSubtrahend = currentSubtrahend
                }
                // no more ids in minuend
                if (currentMinuend.also { nextId = it } === PersistentEntityId.EMPTY_ID) {
                    break
                }
                // no more ids subtrahend
                if (currentSubtrahend === PersistentEntityId.EMPTY_ID) {
                    currentMinuend = null
                    break
                }
                if (currentMinuend !== currentSubtrahend && (currentMinuend == null || currentSubtrahend == null)) {
                    break
                }
                if (currentMinuend === currentSubtrahend) {
                    currentSubtrahend = null
                    currentMinuend = currentSubtrahend
                    continue
                }
                val cmp = currentMinuend?.compareTo(currentSubtrahend) ?: throw throw IllegalStateException("Can't be")
                if (cmp < 0) {
                    currentMinuend = null
                    break
                }
                currentSubtrahend = null
                if (cmp == 0) {
                    currentMinuend = null
                }
            }
            this.currentMinuend = currentMinuend
            this.currentSubtrahend = currentSubtrahend
            return this.currentMinuend !== PersistentEntityId.EMPTY_ID
        }
    }

    private class SortedReverseIterator(
        iterable: EntityIterableBase,
        txn: PersistentStoreTransaction,
        minuend: EntityIterableBase,
        subtrahend: EntityIterableBase
    ) : SortedIteratorBase(iterable) {

        private val minuend = minuend.getReverseIteratorImpl(txn) as EntityIteratorBase
        private val subtrahend = subtrahend.getReverseIteratorImpl(txn) as EntityIteratorBase

        override fun hasNextImpl(): Boolean {
            var currentMinuend = currentMinuend
            var currentSubtrahend = currentSubtrahend
            while (currentMinuend !== PersistentEntityId.EMPTY_ID) {
                if (currentMinuend == null) {
                    currentMinuend = checkNextId(minuend)
                    this.currentMinuend = currentMinuend
                }
                if (currentSubtrahend == null) {
                    currentSubtrahend = checkNextId(subtrahend)
                    this.currentSubtrahend = currentSubtrahend
                }
                // no more ids in minuend
                if (currentMinuend.also { nextId = it } === PersistentEntityId.EMPTY_ID) {
                    break
                }
                // no more ids subtrahend
                if (currentSubtrahend === PersistentEntityId.EMPTY_ID) {
                    currentMinuend = null
                    break
                }
                if (currentMinuend !== currentSubtrahend && (currentMinuend == null || currentSubtrahend == null)) {
                    break
                }
                if (currentMinuend === currentSubtrahend) {
                    currentSubtrahend = null
                    currentMinuend = currentSubtrahend
                    continue
                }
                val cmp = currentMinuend?.compareTo(currentSubtrahend) ?: throw throw IllegalStateException("Can't be")
                if (cmp > 0) {
                    currentMinuend = null
                    break
                }
                currentSubtrahend = null
                if (cmp == 0) {
                    currentMinuend = null
                }
            }
            this.currentMinuend = currentMinuend
            this.currentSubtrahend = currentSubtrahend
            return this.currentMinuend !== PersistentEntityId.EMPTY_ID
        }
    }

    private class UnsortedIterator constructor(
        iterable: EntityIterableBase,
        private val txn: PersistentStoreTransaction,
        minuend: EntityIterableBase,
        subtrahend: EntityIterableBase
    ) : NonDisposableEntityIterator(iterable) {

        private val minuend = minuend.iterator() as EntityIteratorBase
        private var subtrahend: EntityIterableBase? = subtrahend
        private val exceptSet by lazy(LazyThreadSafetyMode.NONE) {
            this.subtrahend.notNull.toSet(txn).also {
                // reclaim memory as early as possible
                this.subtrahend = null
            }
        }
        private var nextId: EntityId? = null

        override fun hasNextImpl(): Boolean {
            while (minuend.hasNext()) {
                val nextId = minuend.nextId()
                if (!exceptSet.contains(nextId)) {
                    this.nextId = nextId
                    return true
                }
            }
            nextId = null
            return false
        }

        public override fun nextIdImpl(): EntityId? = nextId
    }

    companion object {
        init {
            registerType(EntityIterableType.MINUS) { txn, store, parameters ->
                MinusIterable(
                    txn,
                    parameters[0] as EntityIterableBase, parameters[1] as EntityIterableBase
                )
            }
        }
    }
}

private fun checkNextId(it: EntityIterator): EntityId? = if (it.hasNext()) it.nextId() else PersistentEntityId.EMPTY_ID
