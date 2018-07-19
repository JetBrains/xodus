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
package jetbrains.exodus.gc

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.env.*
import jetbrains.exodus.kotlin.synchronized
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable
import jetbrains.exodus.log.ExpiredLoggableInfo
import jetbrains.exodus.log.Log
import java.io.File
import java.lang.ref.WeakReference
import java.util.*

class UtilizationProfile(private val env: EnvironmentImpl, private val gc: GarbageCollector) {

    private val log: Log = env.log
    private val fileSize: Long // in bytes
    private val filesUtilization: LongHashMap<MutableLong> // file address -> number of free bytes
    private var totalBytes: Long = 0
    private var totalFreeBytes: Long = 0
    @Volatile
    var isDirty: Boolean = false

    init {
        fileSize = log.fileLengthBound
        filesUtilization = LongHashMap()
        log.addNewFileListener { fileAddress ->
            filesUtilization.synchronized {
                this[fileAddress] = MutableLong(0L)
            }
            estimateTotalBytes()
        }
    }

    internal fun clear() = filesUtilization.synchronized { clear() }.apply { estimateTotalBytes() }

    /**
     * Loads utilization profile.
     */
    fun load() {
        val ec = env.environmentConfig
        if (ec.gcUtilizationFromScratch) {
            computeUtilizationFromScratch()
        } else {
            val storedUtilization = ec.gcUtilizationFromFile
            if (!storedUtilization.isEmpty()) {
                loadUtilizationFromFile(storedUtilization)
            } else {
                env.executeInReadonlyTransaction { txn ->
                    if (!env.storeExists(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, txn)) {
                        if (env.allStoreCount == 0L && log.numberOfFiles <= 1) {
                            clearUtilization()
                        } else {
                            computeUtilizationFromScratch()
                        }
                    } else {
                        val filesUtilization = LongHashMap<MutableLong>()
                        val store = env.openStore(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn)
                        store.openCursor(txn).use { cursor ->
                            while (cursor.next) {
                                val fileAddress = LongBinding.compressedEntryToLong(cursor.key)
                                val freeBytes = CompressedUnsignedLongByteIterable.getLong(cursor.value)
                                // don't update utilization of files being reset but not deleted
                                // if they were not actually deleted they will be collected first
                                if (freeBytes != 0L) {
                                    filesUtilization[fileAddress] = MutableLong(freeBytes)
                                }
                            }
                        }

                        // check of saved utilization is consistent, at least if it contains same files as the log
                        var inconsistent = false
                        with(PackedLongHashSet(filesUtilization.keys)) {
                            log.allFileAddresses.forEach {
                                if (!remove(it)) {
                                    inconsistent = true
                                    return@with
                                }
                            }
                            if (isNotEmpty()) {
                                inconsistent = true
                            }
                        }

                        if (inconsistent) {
                            computeUtilizationFromScratch()
                        } else {
                            this@UtilizationProfile.filesUtilization.synchronized {
                                clear()
                                putAll(filesUtilization)
                            }
                        }
                        estimateFreeBytesAndWakeGcIfNecessary()
                    }
                }
            }
        }
    }

    /**
     * Saves utilization profile in internal store within specified transaction.
     */
    fun save(txn: Transaction) {
        if (isDirty) {
            val store = env.openStore(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME,
                    StoreConfig.WITHOUT_DUPLICATES, txn)
            // clear entries for already deleted files
            store.openCursor(txn).use { cursor ->
                while (cursor.next) {
                    val fileAddress = LongBinding.compressedEntryToLong(cursor.key)
                    if (filesUtilization.synchronized { !containsKey(fileAddress) }) {
                        cursor.deleteCurrent()
                    }
                }
            }
            // save profile of up-to-date files
            for (entry in filesUtilization.synchronized { ArrayList(entries) }) {
                store.put(txn,
                        LongBinding.longToCompressedEntry(entry.key),
                        CompressedUnsignedLongByteIterable.getIterable(entry.value.value))
            }
        }
    }

    internal fun totalFreeSpacePercent(): Int {
        val totalBytes = this.totalBytes
        return (if (totalBytes == 0L) 0 else totalFreeBytes * 100L / totalBytes).toInt()
    }

    fun totalUtilizationPercent(): Int {
        return 100 - totalFreeSpacePercent()
    }

    internal fun getFileFreeBytes(fileAddress: Long) = filesUtilization.synchronized {
        this[fileAddress]?.value ?: Long.MAX_VALUE
    }

    /**
     * Updates utilization profile with new expired loggables.
     *
     * @param loggables expired loggables.
     */
    internal fun fetchExpiredLoggables(loggables: Iterable<ExpiredLoggableInfo>) {
        var prevFileAddress = -1L
        var prevFreeBytes: MutableLong? = null
        filesUtilization.synchronized {
            for (loggable in loggables) {
                val fileAddress = log.getFileAddress(loggable.address)
                val freeBytes =
                        (if (prevFileAddress == fileAddress) prevFreeBytes else this[fileAddress])
                                ?: MutableLong(0L).also { this[fileAddress] = it }
                freeBytes.value += loggable.length.toLong()
                prevFreeBytes = freeBytes
                prevFileAddress = fileAddress
            }
        }
    }

    internal fun removeFile(fileAddress: Long): Unit = filesUtilization.synchronized { remove(fileAddress) }

    internal fun resetFile(fileAddress: Long) = filesUtilization.synchronized { this[fileAddress]?.value = 0L }

    internal fun estimateTotalBytes() {
        // at first, estimate total bytes
        val fileAddresses = log.allFileAddresses
        val filesCount = fileAddresses.size
        val minFileAge = gc.minFileAge
        totalBytes = if (filesCount > minFileAge) (filesCount - minFileAge) * fileSize else 0
        // then, estimate total free bytes
        var totalFreeBytes: Long = 0
        filesUtilization.synchronized {
            (minFileAge until filesCount).forEach { i ->
                val freeBytes = this[fileAddresses[i]]
                totalFreeBytes += freeBytes?.value ?: fileSize
            }
        }
        this.totalFreeBytes = totalFreeBytes
    }

    internal fun getFilesSortedByUtilization(highFile: Long): Iterator<Long> {
        val fileAddresses = log.allFileAddresses
        val maxFreeBytes = fileSize * gc.maximumFreeSpacePercent.toLong() / 100L
        val fragmentedFiles = PriorityQueue(10, Comparator<Pair<Long, Long>> { leftPair, rightPair ->
            val leftFreeBytes = leftPair.second
            val rightFreeBytes = rightPair.second
            if (leftFreeBytes == rightFreeBytes) {
                return@Comparator 0
            }
            if (leftFreeBytes > rightFreeBytes) -1 else 1
        })
        var totalCleanableBytes = 0L
        var totalFreeBytes = 0L
        filesUtilization.synchronized {
            (gc.minFileAge until fileAddresses.size).forEach { i ->
                val file = fileAddresses[i]
                if (file < highFile && !gc.isFileCleaned(file)) {
                    totalCleanableBytes += fileSize
                    val freeBytes = this[file]
                    totalFreeBytes += if (freeBytes == null) {
                        fragmentedFiles.add(Pair(file, fileSize))
                        fileSize
                    } else {
                        val freeBytesValue = freeBytes.value
                        if (freeBytesValue > maxFreeBytes) {
                            fragmentedFiles.add(Pair(file, freeBytesValue))
                        }
                        freeBytesValue
                    }
                }
            }
        }
        return object : Iterator<Long> {

            override fun hasNext(): Boolean {
                return !fragmentedFiles.isEmpty() && totalFreeBytes > totalCleanableBytes * gc.maximumFreeSpacePercent / 100L
            }

            override fun next(): Long {
                val pair = fragmentedFiles.poll()
                totalFreeBytes -= pair.second
                return pair.first
            }
        }
    }

    /**
     * Loads utilization profile from file.
     *
     * @param path external file with utilization info in the format as created by the `"-d"` option
     * of the `Reflect` tool
     * @see EnvironmentConfig.setGcUtilizationFromFile
     */
    fun loadUtilizationFromFile(path: String) {
        gc.cleaner.getJobProcessor().queueAt(object : Job() {
            override fun execute() {
                val usedSpace = LongHashMap<Long>()
                try {
                    Scanner(File(path)).use { scanner ->
                        while (scanner.hasNextLong()) {
                            val address = scanner.nextLong()
                            val usedBytes = scanner.nextLong()
                            usedSpace[address] = usedBytes
                        }
                    }
                } catch (t: Throwable) {
                    GarbageCollector.loggingError({ "Failed to load utilization from $path" }, t)
                }

                // if an error occurs during reading the file, then GC will be too pessimistic, i.e. it will clean
                // first the files which are missed in the utilization profile.
                setUtilization(usedSpace)
            }
        }, gc.startTime)
    }

    /**
     * Reloads utilization profile.
     */
    fun computeUtilizationFromScratch() {
        gc.cleaner.getJobProcessor().queueAt(ComputeUtilizationFromScratchJob(this), gc.startTime)
    }

    private fun clearUtilization() = filesUtilization.synchronized { clear() }

    private fun setUtilization(usedSpace: LongHashMap<Long>) = filesUtilization.synchronized {
        clear()
        for ((key, value) in usedSpace) {
            this[key] = MutableLong(fileSize - value)
        }
    }

    private fun estimateFreeBytesAndWakeGcIfNecessary() {
        estimateTotalBytes()
        if (gc.isTooMuchFreeSpace) {
            gc.wake()
        }
    }

    /**
     * Is used instead of [Long] for saving free bytes per file in  order to update the value in-place, so
     * reducing number of lookups in the [LongHashMap][.filesUtilization].
     */
    private class MutableLong internal constructor(var value: Long) {

        override fun toString(): String {
            return java.lang.Long.toString(value)
        }
    }

    private class ComputeUtilizationFromScratchJob(up: UtilizationProfile) : Job() {

        private val up: WeakReference<UtilizationProfile> = WeakReference(up)

        override fun execute() {
            val up = this.up.get() ?: return
            val usedSpace = LongHashMap<Long>()
            val env = up.env
            env.executeInReadonlyTransaction { txn ->
                val log = up.log
                for (storeName in env.getAllStoreNames(txn)) {
                    val store = env.openStore(storeName, StoreConfig.USE_EXISTING, txn)
                    val it = (txn as TransactionBase).getTree(store).addressIterator()
                    while (it.hasNext()) {
                        val address = it.next()
                        val loggable = log.read(address)
                        val fileAddress = log.getFileAddress(address)
                        var usedBytes = usedSpace.get(fileAddress)
                        if (usedBytes == null) {
                            usedBytes = 0L
                        }
                        usedBytes += loggable.length().toLong()
                        usedSpace[fileAddress] = usedBytes
                    }
                }
            }
            up.setUtilization(usedSpace)
        }
    }
}