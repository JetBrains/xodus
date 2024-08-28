/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
import jetbrains.exodus.runtime.OOMGuard
import mu.KLogger
import java.io.File
import java.util.*
import kotlin.math.min

fun Environment.copyTo(
    there: File,
    forcePrefixing: Boolean,
    logger: KLogger? = null,
    progress: ((Any?) -> Unit)? = null
) {
    if (there.list().run { this != null && isNotEmpty() }) {
        progress?.invoke("Target environment path is expected to be an empty directory: $there")
        return
    }
    val maxMemory = Runtime.getRuntime().maxMemory()
    val guard = OOMGuard(if (maxMemory == Long.MAX_VALUE) 20_000_000 else min(maxMemory / 50L, 1000_000_000).toInt())
    Environments.newInstance(there, environmentConfig).use { newEnv ->
        progress?.invoke("Copying environment to " + newEnv.location)
        val names = computeInReadonlyTransaction { txn -> getAllStoreNames(txn) }
        val storesCount = names.size
        progress?.invoke("Stores found: $storesCount")

        names.forEachIndexed { i, name ->
            val started = Date()
            print(copyStoreMessage(started, name, i + 1, storesCount, 0L))
            var storeSize = 0L
            var storeIsBroken: Throwable? = null
            try {
                newEnv.executeInExclusiveTransaction { targetTxn ->
                    try {
                        executeInReadonlyTransaction { sourceTxn ->

                            val sourceStore = openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                            val targetConfig = sourceStore.config.let { sourceConfig ->
                                if (forcePrefixing) StoreConfig.getStoreConfig(
                                    sourceConfig.duplicates,
                                    true
                                ) else sourceConfig
                            }
                            val targetStore = newEnv.openStore(name, targetConfig, targetTxn)
                            storeSize = sourceStore.count(sourceTxn)
                            sourceStore.openCursor(sourceTxn).forEachIndexed {
                                targetStore.putRight(targetTxn, ArrayByteIterable(key), ArrayByteIterable(value))
                                if ((it + 1) % 100_000 == 0 || guard.isItCloseToOOM()) {
                                    targetTxn.flush()
                                    guard.reset()
                                    print(
                                        copyStoreMessage(
                                            started,
                                            name,
                                            i + 1,
                                            storesCount,
                                            (it.toLong() * 100L / storeSize)
                                        )
                                    )
                                }

                            }
                        }
                    } catch (t: Throwable) {
                        targetTxn.flush()
                        throw t
                    }
                }
            } catch (t: Throwable) {
                logger?.warn(t) { "Failed to completely copy $name, proceeding in reverse order..." }
                if (t is VirtualMachineError) {
                    throw t
                }
                storeIsBroken = t
            }
            if (storeIsBroken != null) {
                try {
                    newEnv.executeInExclusiveTransaction { targetTxn ->
                        try {
                            executeInReadonlyTransaction { sourceTxn ->
                                val sourceStore = openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                                val targetConfig = sourceStore.config.let { sourceConfig ->
                                    if (forcePrefixing) StoreConfig.getStoreConfig(
                                        sourceConfig.duplicates,
                                        true
                                    ) else sourceConfig
                                }
                                val targetStore = newEnv.openStore(name, targetConfig, targetTxn)
                                storeSize = sourceStore.count(sourceTxn)
                                sourceStore.openCursor(sourceTxn).forEachReversed {
                                    targetStore.put(targetTxn, ArrayByteIterable(key), ArrayByteIterable(value))
                                    if (guard.isItCloseToOOM()) {
                                        targetTxn.flush()
                                        guard.reset()
                                    }
                                }
                            }
                        } catch (t: Throwable) {
                            targetTxn.flush()
                            throw t
                        }
                    }
                } catch (t: Throwable) {
                    logger?.error(t) { "Failed to completely copy $name" }
                    if (t is VirtualMachineError) {
                        throw t
                    }
                }
                println()
                logger?.error("Failed to completely copy store $name", storeIsBroken)
            }
            val actualSize = newEnv.computeInReadonlyTransaction { txn ->
                if (newEnv.storeExists(name, txn)) {
                    newEnv.openStore(name, StoreConfig.USE_EXISTING, txn).count(txn)
                } else 0L

            }
            progress?.invoke("\r$started Copying store $name (${i + 1} of $storesCount): saved store size = $storeSize, actual number of pairs = $actualSize")
        }
    }
}

fun Environment.copyToAsWhole(
    there: File,
    forcePrefixing: Boolean,
    progress: ((Any?) -> Unit)? = null
) {
    if (there.list().run { this != null && isNotEmpty() }) {
        progress?.invoke("Target environment path is expected to be an empty directory: $there")
        return
    }

    Environments.newInstance(there, environmentConfig).use { newEnv ->
        progress?.invoke("Copying environment to " + newEnv.location)
        val names = computeInReadonlyTransaction { txn -> getAllStoreNames(txn) }
        val storesCount = names.size
        progress?.invoke("Stores found: $storesCount")

        newEnv.executeInExclusiveTransaction { targetTxn ->
            executeInReadonlyTransaction { sourceTxn ->
                names.forEach { name ->
                    val sourceStore = openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                    val targetConfig = sourceStore.config.let { sourceConfig ->
                        if (forcePrefixing) StoreConfig.getStoreConfig(
                            sourceConfig.duplicates,
                            true
                        ) else {
                            sourceConfig
                        }
                    }

                    newEnv.openStore(name, targetConfig, targetTxn)
                    progress?.invoke("\rStore $name has been created.")
                }
            }
        }

        executeInReadonlyTransaction { sourceTxn ->
            names.forEachIndexed { i, name ->
                newEnv.executeInExclusiveTransaction { targetTxn ->
                    val started = Date()
                    print(copyStoreMessage(started, name, i + 1, storesCount, 0L))

                    val sourceStore = openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                    val targetStore = newEnv.openStore(name, StoreConfig.USE_EXISTING, targetTxn)

                    sourceStore.openCursor(sourceTxn).forEach {
                        targetStore.putRight(targetTxn, ArrayByteIterable(key), ArrayByteIterable(value))
                    }

                    val actualSize = newEnv.computeInReadonlyTransaction { txn ->
                        if (newEnv.storeExists(name, txn)) {
                            newEnv.openStore(name, StoreConfig.USE_EXISTING, txn).count(txn)
                        } else {
                            0L
                        }

                    }
                    progress?.invoke("\r$started Copying store $name (${i + 1} of $storesCount): number of pairs = $actualSize")
                }
            }
        }
    }
}


private fun copyStoreMessage(started: Date, name: String, n: Int, totalCount: Int, percent: Long) =
    "\r$started Copying store $name ($n of $totalCount): $percent%"