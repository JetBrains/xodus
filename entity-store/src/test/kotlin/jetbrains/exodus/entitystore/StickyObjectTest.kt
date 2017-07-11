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

import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable
import jetbrains.exodus.entitystore.iterate.EntitiesOfTypeIterable
import jetbrains.exodus.entitystore.iterate.UpdatableEntityIdSortedSetCachedInstanceIterable
import org.junit.Assert

class StickyObjectTest : EntityStoreTestBase() {

    override fun needsImplicitTxn() = false

    fun testAllOfTypeIterable() {
        entityStore.executeInTransaction {
            it.newEntity("Issue")
        }
        entityStore.executeInExclusiveTransaction {
            val all = makeIterable(it as PersistentStoreTransaction)
            it.registerStickyObject(all.handle, object : UpdatableEntityIdSortedSetCachedInstanceIterable(it, all) {
                override fun beginUpdate(txn: PersistentStoreTransaction): Updatable {
                    val result = beginUpdate()
                    txn.registerStickyObject(handle, result)
                    return result
                }
            })
            it.newEntity("Issue")
        }
        entityStore.executeInTransaction {
            val all = makeIterable(it as PersistentStoreTransaction)
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
}
