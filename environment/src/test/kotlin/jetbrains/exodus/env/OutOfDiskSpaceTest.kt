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
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.log.LogTestConfig
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test

class OutOfDiskSpaceTest : EnvironmentTestsBase() {

    @Test
    @TestFor(issues = ["XD-733"])
    @Ignore
    fun testOODS() {
        env.environmentConfig.logCachePageSize = 1024
        env.environmentConfig.logFileSize = 1
        invalidateSharedCaches()
        reopenEnvironment()
        val store0 = env.computeInTransaction { txn ->
            env.openStore("store0", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        val store1 = env.computeInTransaction { txn ->
            env.openStore("store1", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        for (i in 0..9) {
            env.executeInTransaction { txn ->
                store0.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
            }
            env.executeInTransaction { txn ->
                store1.put(txn, StringBinding.stringToEntry(i.toString()), IntegerBinding.intToEntry(i))
            }
        }
        val highAddress = log.highAddress
        for (l in highAddress..highAddress * 2) {
            val logTestConfig = LogTestConfig().apply {
                maxHighAddress = l
            }
            env.log.setLogTestConfig(logTestConfig)
            try {
                env.executeInTransaction { txn ->
                    for (i in 10..100) {
                        store0.put(txn, IntegerBinding.intToEntry(i), StringBinding.stringToEntry(i.toString()))
                        store1.put(txn, StringBinding.stringToEntry(i.toString()), IntegerBinding.intToEntry(i))
                    }
                }
            } catch (e: ExodusException) {
            }
            Assert.assertEquals(highAddress, log.highAddress)
            env.log.setLogTestConfig(null)
            for (i in 0..9) {
                env.executeInTransaction { txn ->
                    Assert.assertEquals(StringBinding.stringToEntry(i.toString()), store0[txn, IntegerBinding.intToEntry(i)])
                }
                env.executeInTransaction { txn ->
                    Assert.assertEquals(IntegerBinding.intToEntry(i), store1[txn, StringBinding.stringToEntry(i.toString())])
                }
            }
        }
    }
}