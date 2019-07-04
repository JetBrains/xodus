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
package jetbrains.exodus.env

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import org.junit.Assert
import org.junit.Test
import java.nio.file.Files
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Semaphore

@TestFor(issue = "XD-782")
class XodusConcurrentCursorTest {

    @Test
    fun test() {
        val threadPool = Executors.newFixedThreadPool(20)
        val env = Environments.newInstance(Files.createTempDirectory("test").toFile())
        val limiter = Semaphore(100)
        env.assertCount(0L)

        //insert data
        env.executeInTransaction { txn ->
            val store = env.openStore(txn)
            repeat(10_000) {
                store.put(txn, IntegerBinding.intToEntry(it), StringBinding.stringToEntry("val_$it"))
            }
        }
        env.assertCount(10_000L)

        //iterate through data and submit each key to async deletion
        env.executeInReadonlyTransaction { txn ->
            val store = env.openStore(txn)
            val futures = mutableListOf<Future<out Any>>()
            store.openCursor(txn).use { cursor ->
                while (cursor.next) {
                    val key = cursor.key
                    limiter.acquire()
                    threadPool.submit {
                        println("deleting key=$key")
                        env.deleteEntryInSeparateTransaction(key)
                        limiter.release()
                    }.also { futures.add(it) }
                }
            }
            futures.forEach { it.get() }
        }

        //expecting that cursor has iterated through all keys and submitted them for deletion
        env.assertCount(0L)
    }

    private fun Environment.openStore(txn: Transaction) = openStore("myStore", StoreConfig.WITHOUT_DUPLICATES, txn)

    private fun Environment.deleteEntryInSeparateTransaction(key: ByteIterable) {
        executeInTransaction { txn ->
            openStore(txn).delete(txn, key)
        }
    }

    private fun Environment.assertCount(count: Long) {
        executeInTransaction { txn ->
            Assert.assertEquals(count, openStore(txn).count(txn))
        }
    }
}