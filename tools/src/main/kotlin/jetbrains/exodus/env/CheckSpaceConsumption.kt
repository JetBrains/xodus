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

import jetbrains.exodus.gc.GarbageCollector
import jetbrains.exodus.log.*
import java.util.*

fun checkSpaceConsumption(envPath: String) {
    println("Checking space consumption of $envPath")

    (Environments.newInstance(envPath) as EnvironmentImpl).use { env ->
        calculateUsingStores(env)
    }
}

private fun calculateUsingStores(env: EnvironmentImpl) {
    println("Calculation of space consumption using environment stores")

    env.executeInReadonlyTransaction { txn ->
        var blockSize = 0L
        var dataSize = 0L

        val spaceUsageByBlock = TreeMap<Long, Long>()
        val sizeByBlock = TreeMap<Long, Long>()
        val sizeByStructureId = TreeMap<Int, Long>()

        val log = env.log

        val reader = log.config.reader
        val blockIterator = reader.blocks.iterator()
        while (blockIterator.hasNext()) {
            val block = blockIterator.next()

            val usableBlockSize =
                (block.length() / log.cachePageSize) * log.cachePageSize - BufferedDataWriter.HASH_CODE_SIZE

            blockSize += usableBlockSize
            sizeByBlock[block.address] = usableBlockSize
        }

        for (storeName in env.getAllStoreNames(txn) + GarbageCollector.UTILIZATION_PROFILE_STORE_NAME) {
            val store = env.openStore(storeName, StoreConfig.USE_EXISTING, txn)
            val it = (txn as TransactionBase).getTree(store).addressIterator()
            while (it.hasNext()) {
                val address = it.next()
                val loggable = log.read(address)
                dataSize += loggable.length()

                val fileAddress = log.getFileAddress(address)
                spaceUsageByBlock.compute(fileAddress) { _, size ->
                    if (size == null) 0L else size + loggable.length()
                }

                val structureId = loggable.structureId
                sizeByStructureId.compute(structureId) { _, size ->
                    if (size == null) 0 else size + loggable.length()
                }
            }
        }

        println("Calculation of used space was completed")
        println("Space usage is ${100 * dataSize.toDouble() / blockSize}%")

        println("Space usage by block:")
        for ((blockAddress, blockDataSize) in spaceUsageByBlock) {
            val usableBlockSize = sizeByBlock[blockAddress]!!
            val fullBlockSize =
                usableBlockSize / (log.cachePageSize - BufferedDataWriter.HASH_CODE_SIZE) * log.cachePageSize

            println(
                "Block ${LogUtil.getLogFilename(blockAddress)}, space consumption is " +
                        "${100 * blockDataSize.toDouble() / usableBlockSize}%, data size is $blockDataSize bytes, " +
                        "usable block size is $usableBlockSize bytes " +
                        "(${usableBlockSize / (log.cachePageSize - BufferedDataWriter.HASH_CODE_SIZE)} pages)," +
                        " full block size is $fullBlockSize bytes"
            )
        }
        println("Space usage by store name in bytes:")
        val sizeByStoreName = TreeMap<String, Long>()
        for ((structureId, size) in sizeByStructureId) {
            val storeName: String =
                (txn as ReadonlyTransaction).metaTree.getStoreNameByStructureId(structureId, env)!!
            sizeByStoreName[storeName] = size
        }
        for ((storeName, size) in sizeByStoreName) {
            println("$storeName,$size")
        }
    }
}


