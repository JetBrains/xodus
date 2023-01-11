/**
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
package jetbrains.exodus.gc

import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.TransactionBase

internal class ComputeUtilizationFromScratchJob(gc: GarbageCollector) : GcJob(gc) {

    override fun doJob() {
        val gc = this.gc ?: return
        val usedSpace = LongHashMap<Long>()
        val env = gc.environment
        val location = env.location
        GarbageCollector.loggingInfo { "Started calculation of log utilization from scratch at $location" }
        try {
            val up = gc.utilizationProfile
            var goon = true
            while (goon) {
                env.executeInReadonlyTransaction { txn ->
                    up.clear()
                    // optimistic clearing of files' utilization until no parallel writing transaction happens
                    if (txn.highAddress == env.computeInReadonlyTransaction { tx -> tx.highAddress }) {
                        val log = env.log
                        for (storeName in env.getAllStoreNames(txn) + GarbageCollector.UTILIZATION_PROFILE_STORE_NAME) {
                            // stop if environment is already closed
                            if (this.gc == null) {
                                break
                            }
                            if (env.storeExists(storeName, txn)) {
                                val store = env.openStore(storeName, StoreConfig.USE_EXISTING, txn)
                                val it = (txn as TransactionBase).getTree(store).addressIterator()
                                while (it.hasNext()) {
                                    val address = it.next()
                                    val loggable = log.read(address)
                                    val fileAddress = log.getFileAddress(address)
                                    usedSpace[fileAddress] = (usedSpace[fileAddress]
                                            ?: 0L) + loggable.length()
                                }
                            }
                        }
                        goon = false
                    }
                }
            }
            // if environment is not closed
            this.gc?.let {
                up.setUtilization(usedSpace)
                up.isDirty = true
                up.estimateTotalBytesAndWakeGcIfNecessary()
            }
        } finally {
            GarbageCollector.loggingInfo { "Finished calculation of log utilization from scratch at $location" }
        }
    }
}