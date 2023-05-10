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
package jetbrains.exodus.env

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.log.LogTestConfig
import jetbrains.exodus.log.TooBigLoggableException
import org.junit.Assert
import org.junit.Test

open class OutOfDiskSpaceTest : EnvironmentTestsBase() {

    @Test
    @TestFor(issue = "XD-733")
    fun `emulate out of disk space`() {
        var (store0, store1) = prepareStores()

        val highAddress = log.getHighAddress()
        for (l in highAddress * 2 downTo highAddress) {
            val logTestConfig = LogTestConfig().apply {
                maxHighAddress = l
            }
            environment!!.log.setLogTestConfig(logTestConfig)
            try {
                environment!!.executeInTransaction { txn ->
                    for (i in 10..100) {
                        store0.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
                        store1.put(txn, StringBinding.stringToEntry(i.toString()), IntegerBinding.intToEntry(i))
                    }
                }
            } catch (e: ExodusException) {
                //ignore
            }

            reopenEnvironment()

            store0 = environment!!.computeInTransaction { txn ->
                environment!!.openStore("store0", StoreConfig.WITHOUT_DUPLICATES, txn)
            }

            store1 = environment!!.computeInTransaction { txn ->
                environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
            }

            environment!!.log.setLogTestConfig(null)
            environment!!.executeInTransaction { txn ->
                store0.put(txn, StringBinding.stringToEntry(" "), ArrayByteIterable(ByteArray(4)))
            }
            assert(store0, store1)
            environment!!.executeInTransaction { txn ->
                Assert.assertEquals(ArrayByteIterable(ByteArray(4)), store0[txn, StringBinding.stringToEntry(" ")])
            }
        }
    }

    @Test
    @TestFor(issue = "XD-733")
    fun `too big loggable`() {
        var store0 = prepareStores().first

        println(environment!!.log.getHighAddress())
        for (i in 0 until 10) {
            try {
                environment!!.executeInTransaction { txn ->
                    store0.put(txn, StringBinding.stringToEntry(" "), ArrayByteIterable(ByteArray(1024)))
                }
            } catch (e: TooBigLoggableException) {
                reopenEnvironment()

                store0 = environment!!.computeInTransaction { txn ->
                    environment!!.openStore("store0", StoreConfig.WITHOUT_DUPLICATES, txn)
                }

                val store1 = environment!!.computeInTransaction { txn ->
                    environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
                }

                assert(store0, store1)

                environment!!.executeInTransaction { txn ->
                    store0.put(txn, StringBinding.stringToEntry(" "), ArrayByteIterable(ByteArray(128)))
                }
                assert(store0, store1)
                environment!!.executeInTransaction { txn ->
                    Assert.assertEquals(
                        ArrayByteIterable(ByteArray(128)),
                        store0[txn, StringBinding.stringToEntry(" ")]
                    )
                }
                println(environment!!.log.getHighAddress())
                continue
            }
            Assert.fail()
        }
    }

    private fun prepareStores(): Pair<Store, Store> {
        setLogFileSize(1)

        val store0 = environment!!.computeInTransaction { txn ->
            environment!!.openStore("store0", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        val store1 = environment!!.computeInTransaction { txn ->
            environment!!.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        for (i in 0..9) {
            environment!!.executeInTransaction { txn ->
                store0.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
            }
            environment!!.executeInTransaction { txn ->
                store1.put(txn, StringBinding.stringToEntry(i.toString()), IntegerBinding.intToEntry(i))
            }
        }
        return store0 to store1
    }

    private fun assert(store0: Store, store1: Store) {
        for (i in 0..9) {
            environment!!.executeInTransaction { txn ->
                Assert.assertEquals(
                    StringBinding.stringToEntry(i.toString()),
                    store0[txn, IntegerBinding.intToEntry(i)]
                )
            }
            environment!!.executeInTransaction { txn ->
                Assert.assertEquals(
                    IntegerBinding.intToEntry(i),
                    store1[txn, StringBinding.stringToEntry(i.toString())]
                )
            }
        }
    }
}