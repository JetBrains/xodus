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
package jetbrains.exodus.entitystore.iterate.binop

import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterableType
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorFixingDecorator
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator

class UnionIterable(txn: PersistentStoreTransaction?,
                    iterable1: EntityIterableBase,
                    iterable2: EntityIterableBase) : BinaryOperatorEntityIterable(txn, iterable1, iterable2, true) {

    init {
        // UnionIterable is always sorted by id
        depth += SORTED_BY_ID_FLAG
    }

    override fun getIterableType() = EntityIterableType.UNION

    override fun getIteratorImpl(txn: PersistentStoreTransaction) = EntityIteratorFixingDecorator(this, SortedIterator(this, iterable1, iterable2))

    private class SortedIterator(iterable: EntityIterableBase,
                                 iterable1: EntityIterableBase,
                                 iterable2: EntityIterableBase) : NonDisposableEntityIterator(iterable) {

        private var iterator1: Iterator<EntityId?>? = iterable1.iterator().let {
            if (iterable1.isSortedById) toEntityIdIterator(it) else toSortedEntityIdIterator(it)
        }
        private var iterator2: Iterator<EntityId?>? = iterable2.iterator().let {
            if (iterable2.isSortedById) toEntityIdIterator(it) else toSortedEntityIdIterator(it)
        }
        private var e1: EntityId? = null
        private var e2: EntityId? = null
        private var nextId: EntityId? = null

        override fun hasNextImpl(): Boolean {
            if (e1 == null) {
                iterator1?.let {
                    if (it.hasNext()) {
                        e1 = it.next()
                    } else {
                        iterator1 = null
                    }
                }
            }
            if (e2 == null) {
                iterator2?.let {
                    if (it.hasNext()) {
                        e2 = it.next()
                    } else {
                        iterator2 = null
                    }
                }
            }
            e1.let { e1 ->
                if (e1 == null) {
                    nextId = e2
                    e2 = null
                } else {
                    e2.let { e2 ->
                        if (e2 == null) {
                            nextId = e1
                            this.e1 = null
                        } else {
                            val cmp = e1.compareTo(e2)
                            when {
                                cmp < 0 -> {
                                    nextId = e1
                                    this.e1 = null
                                }
                                cmp > 0 -> {
                                    nextId = e2
                                    this.e2 = null
                                }
                                else -> {
                                    nextId = e1
                                    this.e1 = null
                                    this.e2 = null
                                }
                            }
                        }
                    }
                }
            }
            return nextId != null
        }

        public override fun nextIdImpl(): EntityId? {
            val nextId = this.nextId
            this.nextId = null
            return nextId
        }
    }

    companion object {

        init {
            EntityIterableBase.registerType(EntityIterableType.UNION) { txn, _, parameters ->
                UnionIterable(txn,
                        parameters[0] as EntityIterableBase, parameters[1] as EntityIterableBase)
            }
        }
    }
}