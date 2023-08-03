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
package jetbrains.exodus.gc

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.core.dataStructures.Priority
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessorAdapter
import jetbrains.exodus.core.execution.LatchJob
import jetbrains.exodus.core.execution.SharedTimer
import jetbrains.exodus.env.*
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.AbstractBlockListener
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.runtime.OOMGuard
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.util.DeferredIO
import mu.KLogging
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class GarbageCollector(internal val environment: EnvironmentImpl) {

    // the last time when background cleaning job was invoked
    var lastInvocationTime = 0L
    private val ec: EnvironmentConfig = environment.environmentConfig
    val utilizationProfile = UtilizationProfile(environment, this)
    private val pendingFilesToDelete = PackedLongHashSet()
    private val deletionQueue = ConcurrentLinkedQueue<Long>()
    internal val cleaner = BackgroundCleaner(this)
    private val openStoresCache = IntHashMap<StoreImpl>()
    private val brokenFiles = Collections.newSetFromMap(ConcurrentHashMap<Long, Boolean>())
    private var lastBrokenMessage = 0L

    init {
        environment.log.addBlockListener(object : AbstractBlockListener() {

            override fun blockCreated(block: Block) {
                utilizationProfile.estimateTotalBytes()
                if (!cleaner.isCleaning && isTooMuchFreeSpace) {
                    wake()
                }
            }
        })
        SharedTimer.registerPeriodicTask(PeriodicGc(this))
    }

    internal val maximumFreeSpacePercent: Int
        get() = 100 - ec.gcMinUtilization

    internal val isTooMuchFreeSpace: Boolean
        get() = utilizationProfile.totalFreeSpacePercent() > maximumFreeSpacePercent

    internal val minFileAge: Int
        get() = ec.gcFileMinAge

    internal val log: Log
        get() = environment.log

    internal val startTime: Long
        get() = environment.created + ec.gcStartIn

    val isSuspended: Boolean
        get() = cleaner.isSuspended

    fun clear() {
        utilizationProfile.clear()
        pendingFilesToDelete.clear()
        deletionQueue.clear()
        openStoresCache.clear()
    }

    fun getCleanerJobProcessor() = cleaner.getJobProcessor()

    @Suppress("unused")
    fun setCleanerJobProcessor(processor: JobProcessorAdapter) {
        getCleanerJobProcessor().queue(object : Job() {
            override fun execute() {
                cleaner.setJobProcessor(processor)
                wake(true)
            }
        }, Priority.highest)
    }

    fun addBeforeGcAction(action: Runnable) = cleaner.addBeforeGcAction(action)

    fun wake(estimateTotalUtilization: Boolean = false) {
        if (ec.isGcEnabled) {
            environment.executeTransactionSafeTask {
                if (estimateTotalUtilization) {
                    utilizationProfile.estimateTotalBytes()
                }
                cleaner.queueCleaningJob()
            }
        }
    }

    internal fun wakeAt(millis: Long) {
        if (ec.isGcEnabled) {
            cleaner.queueCleaningJobAt(millis)
        }
    }

    fun fetchExpiredLoggables(loggables: ExpiredLoggableCollection) {
        utilizationProfile.fetchExpiredLoggables(loggables)
    }

    fun getFileFreeBytes(fileAddress: Long) = utilizationProfile.getFileFreeBytes(fileAddress)

    fun suspend() = cleaner.suspend()

    fun resume() = cleaner.resume()

    fun finish() = cleaner.finish()

    /* public access is necessary to invoke the method from the Reflect class */
    fun doCleanFile(fileAddress: Long) = doCleanFiles(setOf(fileAddress).iterator())

    /**
     * Cleans fragmented files. It is expected that the files are sorted by utilization, i.e.
     * the first files are more fragmented. In order to avoid race conditions and synchronization issues,
     * this method should be called from the thread of background cleaner.
     *
     * @param fragmentedFiles fragmented files
     * @return `false` if there was unsuccessful attempt to clean a file (GC txn wasn't acquired or flushed)
     */
    internal fun cleanFiles(fragmentedFiles: Iterator<Long>): Boolean {
        cleaner.checkThread()
        return doCleanFiles(fragmentedFiles)
    }

    internal fun isFileCleaned(file: Long) = pendingFilesToDelete.contains(file)

    /**
     * For tests only!!!
     */
    fun waitForPendingGC() {
        getCleanerJobProcessor().waitForLatchJob(object : LatchJob() {
            override fun execute() {
                release()
            }
        }, 100, Priority.lowest)
    }

    /**
     * For tests only!!!
     */
    fun cleanEntireLog() {
        cleaner.cleanEntireLog()
    }

    /**
     * For tests only!!!
     */
    fun testDeletePendingFiles() {
        val files = pendingFilesToDelete.toLongArray()
        var aFileWasDeleted = false
        val currentFile = LongArray(1)
        for (file in files) {
            utilizationProfile.removeFile(file)
            currentFile[0] = file
            environment.removeFiles(
                currentFile,
                if (ec.gcRenameFiles) RemoveBlockType.Rename else RemoveBlockType.Delete
            )
            aFileWasDeleted = true
        }
        if (aFileWasDeleted) {
            pendingFilesToDelete.clear()
            utilizationProfile.estimateTotalBytes()
        }
    }

    internal fun deletePendingFiles() {
        if (!cleaner.isCurrentThread) {
            getCleanerJobProcessor().queue(GcJob(this) {
                cleaner.deletePendingFiles()
            })
        } else {
            cleaner.deletePendingFiles()
        }
    }

    fun doDeletePendingFiles() {
        val filesToDelete = LongArrayList()
        while (true) {
            (deletionQueue.poll() ?: break).apply {
                if (pendingFilesToDelete.remove(this)) {
                    filesToDelete.add(this)
                }
            }
        }

        if (!filesToDelete.isEmpty) {
            // force flush and fsync in order to fix XD-249
            // in order to avoid data loss, it's necessary to make sure that any GC transaction is flushed
            // to underlying storage device before any file is deleted
            environment.flushAndSync()
            val filesArray = filesToDelete.toArray()
            environment.removeFiles(
                filesArray,
                if (ec.gcRenameFiles) RemoveBlockType.Rename else RemoveBlockType.Delete
            )
            filesArray.forEach { utilizationProfile.removeFile(it) }
            utilizationProfile.estimateTotalBytesAndWakeGcIfNecessary()
        }
    }

    private fun doCleanFiles(fragmentedFiles: Iterator<Long>): Boolean {
        // if there are no more files then even don't start a txn
        if (!fragmentedFiles.hasNext()) {
            return true
        }

        val guard = OOMGuard(softRef = false)

        val txn: ReadWriteTransaction = try {
            environment.beginGCTransaction()
        } catch (_: ReadonlyTransactionException) {
            return false
        } catch (_: TransactionAcquireTimeoutException) {
            return false
        }

        val cleanedFiles = PackedLongHashSet()
        val isTxnExclusive = txn.isExclusive
        try {
            val started = System.currentTimeMillis()
            val printBrokenMessage = lastBrokenMessage + 15 * 60 * 1000 < started

            while (fragmentedFiles.hasNext()) {
                val file = fragmentedFiles.next()

                if (brokenFiles.contains(file)) {
                    if (printBrokenMessage) {
                        logger.error {
                            "File ${LogUtil.getLogFilename(file)} " +
                                    "is skipped by GC because it was processed with exception during previous " +
                                    "iteration of cleaning cycle. " +
                                    "To avoid given problems in future: please check the log, " +
                                    "find the log entry which contains the file name and the exception" +
                                    " and create the issue for the developers."
                        }
                        lastBrokenMessage = started
                    }

                    continue
                }

                try {
                    cleanSingleFile(file, txn)
                } catch (e: Exception) {
                    brokenFiles.add(file)
                    throw e
                }

                cleanedFiles.add(file)

                if (!isTxnExclusive) {
                    break // do not process more than one file in a non-exclusive txn
                }
                if (started + ec.gcTransactionTimeout <= System.currentTimeMillis()) {
                    break // break by timeout
                }
                if (guard.isItCloseToOOM()) {
                    break // break because of the risk of OutOfMemoryError
                }
            }
            if (!txn.forceFlush()) {
                // paranoiac check
                if (isTxnExclusive) {
                    throw ExodusException("Can't be: exclusive txn should be successfully flushed")
                }
                return false
            }
        } catch (_: ReadonlyTransactionException) {
            return false
        } catch (e: Throwable) {
            throw ExodusException.toExodusException(e)
        } finally {
            txn.abort()
        }
        if (cleanedFiles.isNotEmpty()) {
            for (file in cleanedFiles) {
                if (isTxnExclusive) {
                    log.clearFileFromLogCache(file, 0)
                }
                pendingFilesToDelete.add(file)
                utilizationProfile.resetFile(file)
            }
            utilizationProfile.estimateTotalBytes()
            environment.executeTransactionSafeTask {
                val filesDeletionDelay = ec.gcFilesDeletionDelay
                if (filesDeletionDelay == 0) {
                    queueDeletionOfFiles(cleanedFiles)
                } else {
                    DeferredIO.getJobProcessor().queueIn(object : Job() {
                        override fun execute() {
                            queueDeletionOfFiles(cleanedFiles)
                        }
                    }, filesDeletionDelay.toLong())
                }
            }
        }
        return true
    }

    private fun queueDeletionOfFiles(cleanedFiles: PackedLongHashSet) {
        for (file in cleanedFiles) {
            deletionQueue.offer(file)
        }
        deletePendingFiles()
    }

    /**
     * @param fileAddress address of the file to clean
     * @param txn         transaction
     */
    private fun cleanSingleFile(fileAddress: Long, txn: ReadWriteTransaction) {
        // the file can be already cleaned
        if (isFileCleaned(fileAddress)) {
            throw ExodusException("Attempt to clean already cleaned file")
        }
        loggingInfo {
            "start cleanFile(${environment.location}${File.separatorChar}${LogUtil.getLogFilename(fileAddress)})" +
                    ", free bytes = ${formatBytes(getFileFreeBytes(fileAddress))}"
        }
        val log = log
        if (logger.isDebugEnabled) {
            val high = log.highAddress
            val highFile = log.highFileAddress
            logger.debug(
                String.format(
                    "Cleaner acquired txn when log high address was: %d (%s@%d) when cleaning file %s",
                    high, LogUtil.getLogFilename(highFile), high - highFile, LogUtil.getLogFilename(fileAddress)
                )
            )
        }
        try {
            val nextFileAddress = fileAddress + log.fileLengthBound
            val loggables = log.getLoggableIterator(fileAddress)
            while (loggables.hasNext()) {
                val loggable = loggables.next()
                if (loggable == null || loggable.address >= nextFileAddress) {
                    break
                }
                val structureId = loggable.structureId
                if (structureId != Loggable.NO_STRUCTURE_ID && structureId != EnvironmentImpl.META_TREE_ID) {
                    var store = openStoresCache.get(structureId)
                    if (store == null) {
                        // TODO: remove openStoresCache when txn.openStoreByStructureId() is fast enough (XD-381)
                        store = txn.openStoreByStructureId(structureId)
                        openStoresCache[structureId] = store
                    }
                    store.reclaim(txn, loggable, loggables)
                }
            }
        } catch (e: Throwable) {
            logger.error("cleanFile(" + LogUtil.getLogFilename(fileAddress) + ')'.toString(), e)
            throw e
        }
    }

    companion object : KLogging() {

        const val UTILIZATION_PROFILE_STORE_NAME = "exodus.gc.up"

        @JvmStatic
        fun isUtilizationProfile(storeName: String): Boolean {
            return UTILIZATION_PROFILE_STORE_NAME == storeName
        }

        internal fun loggingInfo(message: () -> String) {
            logger.info { message() }
        }

        internal fun loggingError(t: Throwable?, message: () -> String) {
            if (t == null) {
                logger.error { message() }
            } else {
                logger.error(t) { message() }
            }
        }

        internal fun loggingDebug(message: () -> String) {
            logger.debug { message() }
        }

        internal fun formatBytes(bytes: Long) = if (bytes == Long.MAX_VALUE) "Unknown" else "${bytes / 1000}Kb"
    }
}