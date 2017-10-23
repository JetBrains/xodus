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
package jetbrains.exodus.env

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet
import jetbrains.exodus.gc.GarbageCollector
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.io.FileDataWriter
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.NullLoggable
import jetbrains.exodus.runtime.OOMGuard
import jetbrains.exodus.tree.LongIterator
import jetbrains.exodus.tree.patricia.PatriciaTreeBase
import mu.KLogging
import java.io.File
import java.io.PrintWriter
import java.util.*
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
    }
    var envPath: String? = null
    var envPath2: String? = null
    var dumpUtilizationToFile: String? = null
    var hasOptions = false
    var gatherLogStats = false
    var validateRoots = false
    var traverse = false
    var copy = false
    var utilizationInfo = false
    val files2Clean = LinkedHashSet<String>()
    for (arg in args) {
        if (arg.startsWith('-')) {
            hasOptions = true
            when (arg.toLowerCase().substring(1)) {
                "ls" -> gatherLogStats = true
                "r" -> validateRoots = true
                "t" -> traverse = true
                "c" -> copy = true
                "u" -> utilizationInfo = true
                else -> {
                    if (arg.startsWith("-cl")) {
                        files2Clean.add(arg.substring(3))
                    } else if (arg.startsWith("-d")) {
                        dumpUtilizationToFile = arg.substring(2)
                    } else {
                        printUsage()
                    }
                }
            }
        } else {
            if (envPath == null) {
                envPath = arg
            } else {
                envPath2 = arg
                break
            }
        }
    }
    if (envPath == null || copy && envPath2 == null) {
        printUsage()
    }

    println("Investigating $envPath")

    try {
        val reflect = Reflect(File(envPath))
        if (files2Clean.size > 0) {
            for (file in files2Clean) {
                reflect.cleanFile(file)
            }
        }
        if (!hasOptions) {
            reflect.gatherLogStats()
            exitProcess(reflect.traverse(dumpUtilizationToFile))
        } else {
            if (validateRoots) {
                reflect.roots()
            }
            if (gatherLogStats) {
                reflect.gatherLogStats()
            }
            if (traverse) {
                exitProcess(reflect.traverse(dumpUtilizationToFile))
            }
            if (copy) {
                reflect.copy(File(envPath2))
            }
            if (utilizationInfo) {
                reflect.spaceInfoFromUtilization()
            }
        }
    } catch (t: Throwable) {
        println(t)
        exitProcess(-1)
    }
    exitProcess(0)
}

internal fun printUsage() {
    println("Usage: Reflect [-options] <environment path> [environment path 2]")
    println("Options:")
    println("  -ls             gather Log Stats")
    println("  -r              validate Roots")
    println("  -t              Traverse actual root")
    println("  -d<file name>   Dump utilization to a file (can be used with the '-t' option)")
    println("  -c              Copy actual root to a new environment (environment path 2 is mandatory)")
    println("  -u              display stored Utilization")
    println("  -cl<file name>  CLean particular file before any reflection")
    exitProcess(1)
}

internal class Reflect(directory: File) {

    companion object : KLogging() {

        private val DEFAULT_PAGE_SIZE = EnvironmentConfig.DEFAULT.logCachePageSize
        private val MAX_VALID_LOGGABLE_TYPE = PatriciaTreeBase.MAX_VALID_LOGGABLE_TYPE.toInt()

        private fun inc(counts: IntHashMap<Int>, key: Int) {
            val count = counts.get(key)
            if (count == null) {
                counts.put(key, 1)
            } else {
                counts.put(key, count + 1)
            }
        }

        private fun dumpCounts(message: String, counts: IntHashMap<Int>) {
            println("\n$message")
            val sortedKeys = TreeSet(Comparator<Int> { o1, o2 ->
                val count1 = counts[o1]
                val count2 = counts[o2]
                if (count1 < count2) {
                    return@Comparator 1
                }
                if (count2 < count1) {
                    return@Comparator -1
                }
                0
            })
            sortedKeys.addAll(counts.keys)
            sortedKeys.forEachIndexed { i, it ->
                if (i > 0) print(" ")
                print("$it:${counts.get(it)}")
            }
            println()
        }
    }

    private val env: EnvironmentImpl
    private val log: Log

    init {
        val files = LogUtil.listFiles(directory)
        files.sortWith(Comparator { left, right ->
            val cmp = LogUtil.getAddress(left.name) - LogUtil.getAddress(right.name)
            if (cmp < 0) -1 else if (cmp > 0) 1 else 0
        })
        val filesLength = files.size
        if (filesLength == 0) {
            throw ExodusException("No database files found at $directory")
        }
        logger.info { "Files found: $filesLength" }

        var maxFileSize = 0L
        files.forEachIndexed { i, f ->
            val length = f.length()
            if (i < filesLength - 1) {
                if (length % LogUtil.LOG_BLOCK_ALIGNMENT != 0L) {
                    throw ExodusException("Length of non-last file ${f.name}  is badly aligned: $length")
                }
            }
            maxFileSize = Math.max(maxFileSize, length)
        }
        logger.info { "Maximum file length: $maxFileSize" }

        val pageSize = if (maxFileSize % DEFAULT_PAGE_SIZE == 0L || filesLength == 1) DEFAULT_PAGE_SIZE else LogUtil.LOG_BLOCK_ALIGNMENT
        logger.info { "Computed page size: $pageSize" }

        val reader = FileDataReader(directory, 16)
        val writer = FileDataWriter(directory)
        val config = EnvironmentConfig().setLogCachePageSize(pageSize).setGcEnabled(false)
        if (config.logFileSize == EnvironmentConfig.DEFAULT.logFileSize) {
            val fileSizeInKB = if (files.size > 1)
                (maxFileSize + pageSize - 1) / pageSize * pageSize / LogUtil.LOG_BLOCK_ALIGNMENT else
                EnvironmentConfig.DEFAULT.logFileSize
            config.logFileSize = fileSizeInKB
        }

        env = Environments.newInstance(LogConfig.create(reader, writer), config) as EnvironmentImpl
        log = env.log
    }

    internal fun cleanFile(file: String) {
        env.suspendGC()
        try {
            logger.info { "Cleaning $file" }
            env.gc.doCleanFile(LogUtil.getAddress(file))
        } finally {
            env.resumeGC()
        }
    }

    internal fun roots() {
        var totalRoots = 0L
        log.allFileAddresses.reversed().forEach {
            val endAddress = it + log.getFileSize(it)
            log.getLoggableIterator(it).forEach {
                if (it.type == DatabaseRoot.DATABASE_ROOT_TYPE) {
                    ++totalRoots
                    if (!DatabaseRoot(it).isValid) {
                        logger.error("Invalid root at address: ${it.address}")
                    }
                }
                if (it.address + it.length() >= endAddress) return@forEach
            }
        }
        println("Roots found: ${totalRoots}")
    }

    internal fun gatherLogStats() {
        val dataLengths = IntHashMap<Int>()
        val structureIds = IntHashMap<Int>()
        val types = IntHashMap<Int>()
        var nullLoggables = 0
        val fileAddresses = log.allFileAddresses
        val fileCount = fileAddresses.size
        fileAddresses.reversed().forEachIndexed { i, address ->
            print("\rGathering log statistics, reading file + $i of $fileCount, ${i * 100 / fileCount}% complete")
            log.getLoggableIterator(address).forEach {
                val la = it.address
                if (i < fileCount - 1 && la >= fileAddresses[i + 1]) {
                    return@forEach
                }
                if (NullLoggable.isNullLoggable(it)) {
                    ++nullLoggables
                } else {
                    inc(dataLengths, it.dataLength)
                    inc(structureIds, it.structureId)
                    inc(types, it.type.toInt())
                }
            }
        }
        println("\n\nNull loggables: $nullLoggables")
        dumpCounts("Data lengths:", dataLengths)
        dumpCounts("Structure ids:", structureIds)
        dumpCounts("Loggable types:", types)
    }

    /**
     * @return exit code 0 if there were no problems traversing the database
     */
    internal fun traverse(dumpUtilizationToFile: String? = null): Int {
        val usedSpace = TreeMap<Long, Long?>()
        val usedSpacePerStore = TreeMap<String, Long?>()
        print("Analysing meta tree loggables... ")
        fetchUsedSpace("meta tree", env.metaTree.addressIterator(), usedSpace, usedSpacePerStore)
        val names = env.computeInReadonlyTransaction { txn -> env.getAllStoreNames(txn) }
        env.executeInReadonlyTransaction { txn ->
            if (env.storeExists(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, txn)) {
                names.add(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME)
            }
        }

        val size = names.size
        println("Done. Stores found: $size")

        var wereErrors = false
        names.forEachIndexed { i, name ->
            println("Traversing store $name ($i of $size)")
            try {
                env.executeInTransaction { txn ->
                    val store = env.openStore(name, StoreConfig.USE_EXISTING, txn)
                    var storeSize = 0L
                    store.openCursor(txn).forEach { ++storeSize }
                    val tree = (txn as TransactionBase).getTree(store)
                    fetchUsedSpace(name, tree.addressIterator(), usedSpace, usedSpacePerStore)
                    if (tree.size != storeSize) {
                        logger.error { "Stored size (${tree.size}) isn't equal to actual size ($storeSize)" }
                    }
                }
            } catch (t: Throwable) {
                println()
                logger.error("Can't fetch used space for store $name", t)
                wereErrors = true
            }
        }
        println()
        spaceInfo(usedSpace.entries)
        println()
        perStoreSpaceInfo(usedSpacePerStore.entries)
        dumpUtilizationToFile?.run {
            PrintWriter(dumpUtilizationToFile).use { out ->
                usedSpace.entries.map { "${it.key} ${it.value}" }.forEach { out.println(it) }
            }
        }
        return if (wereErrors) -1 else 0
    }

    fun copy(there: File) {
        val guard = OOMGuard(0x1000000) // free 16MB if OMM is near
        Environments.newInstance(there, env.environmentConfig).use { newEnv ->
            println("Copying environment to " + newEnv.location)
            val names = env.computeInReadonlyTransaction { txn -> env.getAllStoreNames(txn) }
            val size = names.size
            println("Stores found: " + size)
            names.forEachIndexed { i, name ->
                print("Copying store $name (${i + 1} of $size )")
                var config: StoreConfig
                var storeSize = 0L
                var storeIsBroken: Throwable? = null
                try {
                    newEnv.executeInExclusiveTransaction { targetTxn ->
                        try {
                            env.executeInReadonlyTransaction { sourceTxn ->
                                val sourceStore = env.openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                                config = sourceStore.config
                                val targetStore = newEnv.openStore(name, config, targetTxn)
                                storeSize = sourceStore.count(sourceTxn)
                                sourceStore.openCursor(sourceTxn).forEach {
                                    targetStore.putRight(targetTxn, ArrayByteIterable(key), ArrayByteIterable(value))
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
                    logger.warn(t, { "Failed to completely copy $name, proceeding in reverse order..." })
                    if (t is VirtualMachineError) {
                        throw t
                    }
                    storeIsBroken = t
                }
                if (storeIsBroken != null) {
                    try {
                        newEnv.executeInExclusiveTransaction { targetTxn ->
                            try {
                                env.executeInReadonlyTransaction { sourceTxn ->
                                    val sourceStore = env.openStore(name, StoreConfig.USE_EXISTING, sourceTxn)
                                    config = sourceStore.config
                                    val targetStore = newEnv.openStore(name, config, targetTxn)
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
                        logger.error(t, { "Failed to completely copy $name" })
                        if (t is VirtualMachineError) {
                            throw t
                        }
                    }
                    println()
                    logger.error("Failed to completely copy store $name", storeIsBroken)
                }
                val actualSize = newEnv.computeInReadonlyTransaction { txn ->
                    if (newEnv.storeExists(name, txn)) {
                        newEnv.openStore(name, StoreConfig.USE_EXISTING, txn).count(txn)
                    } else 0L

                }
                println(". Saved store size = $storeSize, actual number of pairs = $actualSize")
            }
        }
    }

    fun spaceInfoFromUtilization() {
        val storedSpace = TreeMap<Long, Long?>()
        log.allFileAddresses.reversed().forEach { address ->
            val freeBytes = env.gc.getFileFreeBytes(address)
            storedSpace[address] = if (freeBytes == Long.MAX_VALUE) null else log.getFileSize(address) - freeBytes
        }
        spaceInfo(storedSpace.entries)
    }

    private fun fetchUsedSpace(name: String, itr: LongIterator,
                               usedSpace: TreeMap<Long, Long?>,
                               usedSpacePerStore: TreeMap<String, Long?>) {
        itr.forEach {
            try {
                val loggable = log.read(it)
                val fileAddress = log.getFileAddress(it)
                val dataLength = loggable.length().toLong()
                usedSpace.put(fileAddress, (usedSpace[fileAddress] ?: 0L) + dataLength)
                usedSpacePerStore.put(name, (usedSpacePerStore[name] ?: 0L) + dataLength)
                val type = loggable.type.toInt()
                if (type > MAX_VALID_LOGGABLE_TYPE) {
                    logger.error("Wrong loggable type: " + type)
                }
            } catch (e: ExodusException) {
                logger.error("Can't read loggable", e)
            }
        }
    }

    private fun spaceInfo(usedSpace: Iterable<Map.Entry<Long, Long?>>) {
        for ((address, usedBytes) in usedSpace) {
            val size = log.getFileSize(address)
            if (size <= 0) {
                logger.error("Empty file unexpected")
            } else {
                val msg = if (usedBytes == null) {
                    ": unknown"
                } else {
                    String.format(" %8.2fKB, %4.1f%%", usedBytes.toDouble() / 1024, usedBytes.toDouble() / size * 100)
                }
                println("Used bytes in file " + LogUtil.getLogFilename(address) + msg)
            }
        }
    }

    private fun perStoreSpaceInfo(usedSpace: Iterable<Map.Entry<String, Long?>>) {
        for ((name, usedBytes) in usedSpace.sortedBy { -(it.value ?: 0) }) {
            println(if (usedBytes == null) {
                "Used bytes for store $name unknown"
            } else {
                String.format("Used bytes for store\t%110s\t%8.2fKB", name, usedBytes.toDouble() / 1024)
            })
        }
    }
}
