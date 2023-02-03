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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet
import jetbrains.exodus.gc.GarbageCollector
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.io.FileDataWriter
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.NullLoggable
import jetbrains.exodus.tree.LongIterator
import jetbrains.exodus.tree.patricia.PatriciaTreeBase
import java.io.File
import java.io.PrintWriter
import java.util.*
import kotlin.math.max
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
    }
    var envPath: String? = null
    var envPath2: String? = null
    var dumpUtilizationToFile: String? = null
    var hasOptions = false
    var collectLogStats = false
    var validateRoots = false
    var traverse = false
    var copy = false
    var forcePrefixing = false
    var utilizationInfo = false
    var persistentEntityStoreInfo = false
    val files2Clean = LinkedHashSet<String>()
    for (arg in args) {
        if (arg.startsWith('-')) {
            hasOptions = true
            when (arg.lowercase().substring(1)) {
                "ls" -> collectLogStats = true
                "r" -> validateRoots = true
                "t" -> traverse = true
                "c" -> copy = true
                "cp" -> {
                    copy = true; forcePrefixing = true
                }
                "u" -> utilizationInfo = true
                "p" -> persistentEntityStoreInfo = true
                else -> when {
                    arg.startsWith("-cl") -> files2Clean.add(arg.substring(3))
                    arg.startsWith("-d") -> dumpUtilizationToFile = arg.substring(2)
                    else -> printUsage()
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
            reflect.collectLogStats()
            exitProcess(reflect.traverse(dumpUtilizationToFile, persistentEntityStoreInfo))
        } else {
            if (validateRoots) {
                reflect.roots()
            }
            if (collectLogStats) {
                reflect.collectLogStats()
            }
            if (traverse) {
                exitProcess(reflect.traverse(dumpUtilizationToFile, persistentEntityStoreInfo))
            }
            if (copy) {
                reflect.env.copyTo(File(envPath2), forcePrefixing) {
                    println(it)
                }
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
    println("  -ls             collect Log Stats")
    println("  -r              validate Roots")
    println("  -t              Traverse actual root")
    println("  -d<file name>   Dump utilization to a file (can be used with the '-t' option)")
    println("  -c              Copy and compact actual root to a new environment (environment path 2 is mandatory)")
    println("  -cp             Copy and compact actual root to a new environment with forced prefixing (environment path 2 is mandatory)")
    println("  -u              display stored Utilization")
    println("  -p              print PersistentEntityStore tables usage (must be used with the '-t' option)")
    println("  -cl<file name>  CLean particular file before any reflection")
    exitProcess(1)
}

class Reflect(directory: File) {

    companion object {

        private val DEFAULT_PAGE_SIZE = EnvironmentConfig.DEFAULT.logCachePageSize
        private const val MAX_VALID_LOGGABLE_TYPE = PatriciaTreeBase.MAX_VALID_LOGGABLE_TYPE.toInt()

        private fun inc(counts: IntHashMap<Int>, key: Int) {
            val count = counts[key]
            if (count == null) {
                counts[key] = 1
            } else {
                counts[key] = count + 1
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
                o1 - o2
            })
            sortedKeys.addAll(counts.keys)
            sortedKeys.forEach {
                println("${it.toString().padEnd(7)}: count = ${counts.get(it).toString().padStart(10)}")
            }
            println()
        }

        private fun dumpLengths(message: String, counts: IntHashMap<Int>) {
            println("\n$message")
            val totalSpaces = IntHashMap<Long>().apply {
                counts.keys.forEach {
                    put(it, counts[it].toLong() * it)
                }
            }
            val sortedKeys = TreeSet(Comparator<Int> { len1, len2 ->
                val space1 = totalSpaces[len1]
                val space2 = totalSpaces[len2]
                if (space1 < space2) {
                    return@Comparator 1
                }
                if (space2 < space1) {
                    return@Comparator -1
                }
                len1 - len2
            })
            sortedKeys.addAll(counts.keys)
            sortedKeys.forEach { i ->
                println("${i.toString().padEnd(7)}: count = ${counts.get(i).toString().padStart(10)}, space = ${totalSpaces[i].toString().padStart(20)}")
            }
            println()
        }

        fun openEnvironment(directory: File,
                            readonly: Boolean = false,
                            cipherId: String? = null,
                            cipherKey: String? = null,
                            cipherBasicIV: Long? = null): EnvironmentImpl {
            val files = LogUtil.listFiles(directory)
            files.sortWith { left, right ->
                val cmp = LogUtil.getAddress(left.name) - LogUtil.getAddress(right.name)
                if (cmp < 0) -1 else if (cmp > 0) 1 else 0
            }
            val filesLength = files.size
            if (filesLength == 0) {
                throw ExodusException("No database files found at $directory")
            }
            println("Files found: $filesLength")

            var maxFileSize = 0L
            files.forEachIndexed { i, f ->
                val length = f.length()
                if (i < filesLength - 1) {
                    if (length % LogUtil.LOG_BLOCK_ALIGNMENT != 0L) {
                        throw ExodusException("Length of non-last file ${f.name}  is badly aligned: $length")
                    }
                }
                maxFileSize = max(maxFileSize, length)
            }
            println("Maximum file length: $maxFileSize")

            val pageSize = if (maxFileSize % DEFAULT_PAGE_SIZE == 0L || filesLength == 1) DEFAULT_PAGE_SIZE else LogUtil.LOG_BLOCK_ALIGNMENT
            println("Computed page size: $pageSize")

            val reader = FileDataReader(directory)
            val writer = FileDataWriter(reader)
            val config = newEnvironmentConfig {
                logCachePageSize = pageSize
                isGcEnabled = false
                envIsReadonly = readonly
                cipherId?.run { setCipherId(this) }
                cipherKey?.run { setCipherKey(this) }
                cipherBasicIV?.run { setCipherBasicIV(this) }
                if (logFileSize == EnvironmentConfig.DEFAULT.logFileSize) {
                    val fileSizeInKB = if (files.size > 1)
                        (maxFileSize + pageSize - 1) / pageSize * pageSize / LogUtil.LOG_BLOCK_ALIGNMENT else
                        EnvironmentConfig.DEFAULT.logFileSize
                    logFileSize = fileSizeInKB
                }
            }
            return Environments.newInstance(LogConfig.create(reader, writer), config) as EnvironmentImpl
        }
    }

    internal val env: EnvironmentImpl
    private val log: Log

    init {
        env = openEnvironment(directory)
        log = env.log
    }

    internal fun cleanFile(file: String) {
        env.suspendGC()
        try {
            println("Cleaning $file")
            env.gc.doCleanFile(LogUtil.getAddress(file))
        } finally {
            env.resumeGC()
        }
    }

    internal fun roots() {
        var totalRoots = 0L
        log.allFileAddresses.reversed().forEach {
            val endAddress = it + log.getFileSize(it)
            log.getLoggableIterator(it).forEach { loggable ->
                if (loggable.type == DatabaseRoot.DATABASE_ROOT_TYPE) {
                    ++totalRoots
                    if (!DatabaseRoot(loggable).isValid) {
                        println("Invalid root at address: ${loggable.address}")
                    }
                }
                if (loggable.address + loggable.length() >= endAddress) return@forEach
            }
        }
        println("Roots found: $totalRoots")
    }

    internal fun collectLogStats() {
        val dataLengths = IntHashMap<Int>()
        val structureIds = IntHashMap<Int>()
        val types = IntHashMap<Int>()
        var totalLoggables = 0L
        var nullLoggables = 0L
        val fileAddresses = log.allFileAddresses
        val fileCount = fileAddresses.size
        fileAddresses.reversed().forEachIndexed { i, address ->
            print("\rCollecting log statistics, reading file ${i + 1} of $fileCount, ${i * 100 / fileCount}% complete")
            val nextFileAddress = address + log.fileLengthBound
            log.getLoggableIterator(address).forEach {
                val la = it.address
                if (la >= nextFileAddress) {
                    return@forEach
                }
                ++totalLoggables
                if (NullLoggable.isNullLoggable(it)) {
                    ++nullLoggables
                } else {
                    inc(dataLengths, it.dataLength)
                    inc(structureIds, it.structureId)
                    inc(types, it.type.toInt())
                }
            }
        }
        println("\n\nTotal loggables: $totalLoggables")
        println("Null loggables: $nullLoggables")
        dumpLengths("Data lengths:", dataLengths)
        dumpCounts("Structure ids:", structureIds)
        dumpCounts("Loggable types:", types)
    }

    /**
     * @return exit code 0 if there were no problems traversing the database
     */
    internal fun traverse(dumpUtilizationToFile: String? = null, dumpPersistentEntityStoreInfo: Boolean = false): Int {
        val usedSpace = TreeMap<Long, Long?>()
        val usedSpacePerStore = TreeMap<String, Long?>()
        print("Analysing meta tree loggables... ")
        fetchUsedSpace("meta tree", env.metaTreeInternal.addressIterator(), usedSpace, usedSpacePerStore)
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
            println("Traversing store $name (${i + 1} of $size)")
            try {
                env.executeInTransaction { txn ->
                    val store = env.openStore(name, StoreConfig.USE_EXISTING, txn)
                    var storeSize = 0L
                    store.openCursor(txn).forEach { ++storeSize }
                    val tree = (txn as TransactionBase).getTree(store)
                    fetchUsedSpace(name, tree.addressIterator(), usedSpace, usedSpacePerStore)
                    if (tree.size != storeSize) {
                        println("Stored size (${tree.size}) isn't equal to actual size ($storeSize)")
                    }
                }
            } catch (t: Throwable) {
                println("Can't fetch used space for store $name: $t")
                wereErrors = true
            }
        }
        println()
        spaceInfo(usedSpace.entries)
        println()
        perStoreSpaceInfo(usedSpacePerStore, dumpPersistentEntityStoreInfo)
        dumpUtilizationToFile?.run {
            PrintWriter(dumpUtilizationToFile).use { out ->
                usedSpace.entries.map { "${it.key} ${it.value}" }.forEach { out.println(it) }
            }
        }
        return if (wereErrors) -1 else 0
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
                usedSpace[fileAddress] = (usedSpace[fileAddress] ?: 0L) + dataLength
                usedSpacePerStore[name] = (usedSpacePerStore[name] ?: 0L) + dataLength
                val type = loggable.type.toInt()
                if (type > MAX_VALID_LOGGABLE_TYPE) {
                    println("Wrong loggable type: $type")
                }
            } catch (e: ExodusException) {
                println("Can't read loggable: $e")
            }
        }
    }

    private fun spaceInfo(usedSpace: Iterable<Map.Entry<Long, Long?>>) {
        for ((address, usedBytes) in usedSpace) {
            val size = log.getFileSize(address)
            if (size <= 0) {
                println("Empty file unexpected")
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

    private fun perStoreSpaceInfo(usedSpacePerStore: TreeMap<String, Long?>, dumpPersistentEntityStoreInfo: Boolean) {
        if (dumpPersistentEntityStoreInfo) {
            val replacements = mutableMapOf<String, String?>()
            try {
                env.executeInTransaction { txn ->
                    val store = env.openStore("teamsysstore.entity.types", StoreConfig.USE_EXISTING, txn)
                    store.openCursor(txn).forEach {
                        val name = StringBinding.entryToString(key)
                        val id = IntegerBinding.compressedEntryToInt(value)
                        replacements["teamsysstore.links#$id"] = "links for $name"
                        replacements["teamsysstore.blobs#$id"] = "blobs for $name"
                        replacements["teamsysstore.links#$id#reverse"] = "reverse links for $name"
                        replacements["teamsysstore.properties#$id"] = "props for $name"
                        replacements["teamsysstore.entities#$id"] = "entities of $name"
                    }
                }
            } catch (_: Throwable) {
            }
            printPerStoreSpaceInfo(usedSpacePerStore.entries) { name ->
                var replacement = replacements[name]
                if (replacement == null) {
                    val hashIndex = name.lastIndexOf('#')
                    if (hashIndex >= 0) {
                        replacement = replacements[name.substring(0, hashIndex)]
                        if (replacement != null) {
                            replacement = "${name.substring(hashIndex + 1)} for $replacement"
                        }
                    }
                }
                replacement
            }
        } else {
            printPerStoreSpaceInfo(usedSpacePerStore.entries)
        }
    }

    private fun printPerStoreSpaceInfo(usedSpace: Iterable<Map.Entry<String, Long?>>, replacement: (String) -> String? = { it }) {
        for ((name, usedBytes) in usedSpace.sortedBy { -(it.value ?: 0) }) {
            println(if (usedBytes == null) {
                "Used bytes for store $name unknown"
            } else {
                String.format("Used bytes for store\t%110s\t%10.2fKB", replacement(name)
                        ?: name, usedBytes.toDouble() / 1024)
            })
        }
    }
}

inline fun LongIterator.forEach(action: (Long) -> Unit) {
    while (hasNext()) {
        action(next())
    }
}
