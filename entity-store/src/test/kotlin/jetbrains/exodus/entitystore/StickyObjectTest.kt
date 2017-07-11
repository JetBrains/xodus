/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore

import jetbrains.exodus.entitystore.iterate.*
import org.junit.Assert

class StickyObjectTest : EntityStoreTestBase() {

    override fun needsImplicitTxn() = false

    fun testNonIterable() {
        val handle = object : ConstantEntityIterableHandle(entityStore, EntityIterableType.ALL_ENTITIES_RANGE) {
            override fun hashCode(hash: EntityIterableHandleHash) {
            }

            override fun isMatchedEntityAdded(added: EntityId) = true

            override fun onEntityAdded(handleChecker: EntityAddedOrDeletedHandleChecker): Boolean {
                var counter = handleChecker.txn.getStickyObject(this) as Updatable
                if (!counter.isMutated) {
                    counter = counter.beginUpdate(handleChecker.txn)
                    handleChecker.beginUpdate(counter)
                }
                (counter as MutableCounter).value++
                return true
            }
        }

        fun PersistentStoreTransaction.getCount() = (getStickyObject(handle) as Counter).value

        transactionalExclusive {
            it.registerStickyObject(handle, ImmutableCounter(handle))
            it.newEntity("Issue")
            Assert.assertEquals(1, it.getCount())
        }
        transactional {
            it.newEntity("Issue")
            Assert.assertEquals(2, it.getCount())
            transactionalReadonly { it ->
                Assert.assertEquals(1, it.getCount())
            }
        }
        transactional {
            Assert.assertEquals(2, it.getCount())
        }
    }

    fun testAllOfTypeIterable() {
        transactional {
            it.newEntity("Issue")
        }
        transactionalExclusive {
            val all = makeIterable(it)
            it.registerStickyObject(all.handle, object : UpdatableEntityIdSortedSetCachedInstanceIterable(it, all) {
                override fun beginUpdate(txn: PersistentStoreTransaction): Updatable {
                    val result = beginUpdate()
                    txn.registerStickyObject(handle, result)
                    return result
                }
            })
            it.newEntity("Issue")
        }
        transactional {
            val all = makeIterable(it)
            Assert.assertEquals(2, all.size())
            val iterator = all.iterator()
            Assert.assertTrue(iterator.hasNext())
            iterator.next()
            Assert.assertTrue(iterator.hasNext())
        }
    }

    fun makeIterable(txn: PersistentStoreTransaction): EntitiesOfTypeIterable {
        return object : EntitiesOfTypeIterable(txn, txn.store.getEntityTypeId(txn, "Issue", true)) {
            override fun createCachedInstance(txn: PersistentStoreTransaction): CachedInstanceIterable {
                throw IllegalStateException("Must be created as sticky object")
            }

            override fun getHandleImpl(): EntityIterableHandle {
                return object : EntitiesOfTypeIterable.EntitiesOfTypeIterableHandle(this) {
                    override fun isSticky() = true
                }
            }
        }
    }

    interface Counter : Updatable {
        val value: Int
    }

    class ImmutableCounter(val handle: EntityIterableHandle, override val value: Int = 0) : Counter {

        override fun beginUpdate(txn: PersistentStoreTransaction): Counter {
            val result = MutableCounter(handle, value)
            txn.registerStickyObject(handle, result)
            return result
        }

        override fun isMutated() = false

        override fun endUpdate(txn: PersistentStoreTransaction) = throw UnsupportedOperationException()

    }

    class MutableCounter(val handle: EntityIterableHandle, override var value: Int) : Counter {

        override fun beginUpdate(txn: PersistentStoreTransaction) = throw UnsupportedOperationException()
        override fun isMutated() = true

        override fun endUpdate(txn: PersistentStoreTransaction) {
            txn.registerStickyObject(handle, ImmutableCounter(handle, value))
        }

    }

    fun transactional(block: (PersistentStoreTransaction) -> Unit) = entityStore.executeInTransaction {
        block(it as PersistentStoreTransaction)
    }

    fun transactionalExclusive(block: (PersistentStoreTransaction) -> Unit) = entityStore.executeInExclusiveTransaction {
        block(it as PersistentStoreTransaction)
    }

    fun transactionalReadonly(block: (PersistentStoreTransaction) -> Unit) = entityStore.executeInReadonlyTransaction {
        block(it as PersistentStoreTransaction)
    }
}
