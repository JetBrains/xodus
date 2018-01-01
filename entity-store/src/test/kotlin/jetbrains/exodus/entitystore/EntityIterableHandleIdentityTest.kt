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
package jetbrains.exodus.entitystore

import jetbrains.exodus.entitystore.iterate.EntitiesWithLinkSortedIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import org.junit.Assert
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore

class EntityIterableHandleIdentityTest : EntityStoreTestBase() {

    fun testDataRace() {
        val txn = storeTransaction
        val referenceHandle = makeIterable(txn).handle
        val reference = referenceHandle.identity.hashCode()

        val errors = Collections.newSetFromMap(ConcurrentHashMap<Throwable, Boolean>())
        val queue = ConcurrentLinkedQueue<EntityIterableBase>()
        val sema = Semaphore(0)
        val iterations = 1..100000

        val threads = (1..2).map {
            Thread({
                iterations.forEach {
                    sema.acquire()
                    val iterable = queue.poll()
                    val actualHandle = iterable.handle
                    val hash = actualHandle.identity.hashCode()
                    if (reference != hash) {
                        Assert.assertEquals("hash#$it mismatch for $referenceHandle vs $actualHandle", reference, hash)
                    }
                }
            }, "worker $it")
        }

        threads.forEach {
            it.start()
            it.setUncaughtExceptionHandler({ _, ex -> errors.add(ex) })
        }

        iterations.forEach {
            if (errors.isEmpty()) {
                val iterable = makeIterable(txn)
                queue.add(iterable)
                queue.add(iterable)
                sema.release(2)
            }
        }

        errors.forEach { it.printStackTrace() }

        Assert.assertTrue(errors.isEmpty())
    }

    private fun makeIterable(txn: PersistentStoreTransaction) = EntitiesWithLinkSortedIterable(txn, 27, 81, 10, 1113)
}
