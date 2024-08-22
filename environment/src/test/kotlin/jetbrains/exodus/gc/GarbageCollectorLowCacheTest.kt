/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.gc

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.EnvironmentTestsBase
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.log.LogConfig
import org.junit.Test
import java.io.IOException

open class GarbageCollectorLowCacheTest : EnvironmentTestsBase() {

    protected open val config: StoreConfig
        get() = StoreConfig.WITHOUT_DUPLICATES

    override fun createEnvironment() {
        env = newEnvironmentInstance(LogConfig.create(reader, writer), EnvironmentConfig().setMemoryUsage(1).setMemoryUsagePercentage(0))
    }

    @Test
    @Throws(IOException::class, InterruptedException::class)
    fun collectExpiredPageWithKeyAddressPointingToDeletedFile() {
        /**
         * o. low cache, small file size
         * 1. create tree IP->BP(N1),BP(N2)
         * 2. save a lot of updates to last key of BP(N2),
         * so ther're a lot of files with expired version of BP(N2) and
         * links to min key of BP(N2), that was saved in a very first file
         * 3. clean first file, with min key of BP(N2)
         * 4. clean second file with expired version of BP(N2) and link to min key in removed file
         */

        set1KbFileWithoutGC()
        env.environmentConfig.treeMaxPageSize = 16
        env.environmentConfig.memoryUsage = 0
        reopenEnvironment()

        val store = openStoreAutoCommit("duplicates", config)

        putAutoCommit(store, IntegerBinding.intToEntry(1), StringBinding.stringToEntry("value1"))
        putAutoCommit(store, IntegerBinding.intToEntry(2), StringBinding.stringToEntry("value2"))
        putAutoCommit(store, IntegerBinding.intToEntry(3), StringBinding.stringToEntry("value3"))
        putAutoCommit(store, IntegerBinding.intToEntry(4), StringBinding.stringToEntry("value4"))
        putAutoCommit(store, IntegerBinding.intToEntry(5), StringBinding.stringToEntry("value5"))
        putAutoCommit(store, IntegerBinding.intToEntry(6), StringBinding.stringToEntry("value6"))

        for (i in 0..999) {
            putAutoCommit(store, IntegerBinding.intToEntry(6), StringBinding.stringToEntry("value6"))
        }

        val log = log
        val gc = environment.gc

        val highFileAddress = log.highFileAddress
        var fileAddress = log.lowAddress
        while (fileAddress != highFileAddress) {
            gc.doCleanFile(fileAddress)
            fileAddress = log.getNextFileAddress(fileAddress)
            gc.testDeletePendingFiles()
        }
    }
}
