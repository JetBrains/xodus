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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.core.dataStructures.Priority
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessorAdapter
import jetbrains.exodus.env.*
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.*
import jetbrains.exodus.runtime.OOMGuard
import jetbrains.exodus.util.DeferredIO
import mu.KLogging
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue

class GarbageCollector(internal val environment: EnvironmentImpl) {

    private val ec: EnvironmentConfig = environment.environmentConfig
    val utilizationProfile = UtilizationProfile(environment, this)
    private val pendingFilesToDelete = PackedLongHashSet()
    private val deletionQueue = ConcurrentLinkedQueue<Long>()
    internal val cleaner = BackgroundCleaner(this)
    @Volatile
    private var newFiles = ec.gcFilesInterval + 1 // number of new files appeared after last cleaning job
    private val openStoresCache =IntHashMap<StoreImpl>()
    private var useRegularTxn: Boolean = false

    init {
        environment.log.addBlockListener (object : AbstractBlockListener() {

            override fun blockCreated(block: Block, reader: DataReader, writer: DataWriter) {
                val newFiles = newFiles + 1
                this@GarbageCollector.newFiles = newFiles
                utilizationProfile.estimateTotalBytes()
                if (!cleaner.isCleaning && newFiles > ec.gcFilesInterval && isTooMuchFreeSpace) {
                    wake()
                }
            }
        })
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

    fun clear() {
        utilizationProfile.clear()
        pendingFilesToDelete.clear()
        deletionQueue.clear()
        openStoresCache.clear()
        resetNewFiles()
    }

    @Suppress("unused")
    fun setCleanerJobProcessor(processor: JobProcessorAdapter) {
        cleaner.getJobProcessor().queue(object : Job() {
            override fun execute() {
                cleaner.setJobProcessor(processor)
            }
        }, Priority.highest)
    }

    fun wake() {
        if (ec.isGcEnabled) {
            environment.executeTransactionSafeTask { cleaner.queueCleaningJob() }
        }
    }

    internal fun wakeAt(millis: Long) {
        if (ec.isGcEnabled) {
            cleaner.queueCleaningJobAt(millis)
        }
    }

    fun fetchExpiredLoggables(loggables: Iterable<ExpiredLoggableInfo>) {
        utilizationProfile.fetchExpiredLoggables(loggables)
    }

    fun getFileFreeBytes(fileAddress: Long): Long {
        return utilizationProfile.getFileFreeBytes(fileAddress)
    }

    fun suspend() {
        cleaner.suspend()
    }

    fun resume() {
        cleaner.resume()
    }

    fun finish() {
        cleaner.finish()
    }

    /* public access is necessary to invoke the method from the Reflect class */
    fun doCleanFile(fileAddress: Long): Boolean {
        return doCleanFiles(setOf(fileAddress).iterator())
    }

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

    internal fun isFileCleaned(file: Long): Boolean {
        return pendingFilesToDelete.contains(file)
    }

    internal fun resetNewFiles() {
        newFiles = 0
    }

    internal fun setUseRegularTxn(useRegularTxn: Boolean) {
        this.useRegularTxn = useRegularTxn
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
            environment.removeFiles(currentFile, if (ec.gcRenameFiles) RemoveBlockType.Rename else RemoveBlockType.Delete)
            aFileWasDeleted = true
        }
        if (aFileWasDeleted) {
            pendingFilesToDelete.clear()
            utilizationProfile.estimateTotalBytes()
        }
    }

    internal fun deletePendingFiles() {
        if (!cleaner.isCurrentThread) {
            cleaner.getJobProcessor().queue(object : Job() {
                override fun execute() {
                    deletePendingFiles()
                }
            })
        } else {
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
                environment.removeFiles(filesArray, if (ec.gcRenameFiles) RemoveBlockType.Rename else RemoveBlockType.Delete)
                filesArray.forEach { utilizationProfile.removeFile(it) }
                utilizationProfile.estimateTotalBytesAndWakeGcIfNecessary()
            }
        }
    }

    private fun doCleanFiles(fragmentedFiles: Iterator<Long>): Boolean {
        // if there are no more files then even don't start a txn
        if (!fragmentedFiles.hasNext()) {
            return true
        }
        val cleanedFiles = PackedLongHashSet()
        val txn: ReadWriteTransaction
        try {
            val tx = if (useRegularTxn) environment.beginTransaction() else environment.beginGCTransaction()
            // tx can be read-only, so we should manually finish it (see XD-667)
            if (tx.isReadonly) {
                tx.abort()
                return false
            }
            txn = tx as ReadWriteTransaction
        } catch (ignore: TransactionAcquireTimeoutException) {
            return false
        }

        val isTxnExclusive = txn.isExclusive
        try {
            val guard = OOMGuard()
            val started = System.currentTimeMillis()
            while (fragmentedFiles.hasNext()) {
                fragmentedFiles.next().apply {
                    cleanSingleFile(this, txn)
                    cleanedFiles.add(this)
                }
                if (!isTxnExclusive) {
                    break // do not process more than one file in a non-exclusive txn
                }
                if (started + ec.gcTransactionTimeout < System.currentTimeMillis()) {
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
        } catch (e: Throwable) {
            throw ExodusException.toExodusException(e)
        } finally {
            txn.abort()
        }
        if (!cleanedFiles.isEmpty()) {
            for (file in cleanedFiles) {
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
            logger.debug(String.format(
                    "Cleaner acquired txn when log high address was: %d (%s@%d) when cleaning file %s",
                    high, LogUtil.getLogFilename(highFile), high - highFile, LogUtil.getLogFilename(fileAddress)
            ))
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

        internal fun loggingError(message: () -> String, t: Throwable? = null) {
            if (t == null) {
                logger.error { message() }
            } else {
                logger.error(t, { message() })
            }
        }

        internal fun formatBytes(bytes: Long) = if (bytes == Long.MAX_VALUE) "Unknown" else "${bytes / 1000}Kb"
    }
}