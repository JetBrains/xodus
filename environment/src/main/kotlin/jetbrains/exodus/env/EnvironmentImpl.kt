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

import jetbrains.exodus.ConfigSettingChangeListener
import jetbrains.exodus.ExodusException
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.core.dataStructures.ObjectCacheBase
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.execution.SharedTimer.ensureIdle
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.debug.TxnProfiler
import jetbrains.exodus.env.MetaTreeImpl.Proto
import jetbrains.exodus.env.management.BackupController
import jetbrains.exodus.env.management.DatabaseProfiler
import jetbrains.exodus.env.management.EnvironmentConfigWithOperations
import jetbrains.exodus.gc.GarbageCollector
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.io.StorageTypeNotAllowedException
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.tree.TreeMetaInfo
import jetbrains.exodus.tree.btree.BTree
import jetbrains.exodus.tree.btree.BTreeBalancePolicy
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.IOUtil.isRamDiskFile
import jetbrains.exodus.util.IOUtil.isRemoteFile
import jetbrains.exodus.util.IOUtil.isRemovableFile
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock
import javax.management.InstanceAlreadyExistsException

open class EnvironmentImpl internal constructor(log: Log, ec: EnvironmentConfig) : Environment {
    @JvmField
    val log: Log
    private val ec: EnvironmentConfig
    private var balancePolicy: BTreeBalancePolicy? = null

    internal var metaTreeInternal: MetaTreeImpl
    private val structureId: AtomicInteger
    private val txns: TransactionSet
    private val txnSafeTasks: LinkedList<RunnableWithTxnRoot>
    var storeGetCache: StoreGetCache? = null
        private set
    private var envSettingsListener: EnvironmentSettingsListener? = null
    var gc: GarbageCollector
    val commitLock = Any()
    private val metaReadLock: ReadLock
    val metaWriteLock: WriteLock
    private val txnDispatcher: ReentrantTransactionDispatcher
    private var statistics: EnvironmentStatistics
    var txnProfiler: TxnProfiler? = null
    private var configMBean: jetbrains.exodus.env.management.EnvironmentConfig? = null
    private var statisticsMBean: jetbrains.exodus.env.management.EnvironmentStatistics? = null
    private var profilerMBean: DatabaseProfiler? = null
    val backupController: BackupController

    val metaTree: MetaTree
        get() = metaTreeInternal

    /**
     * Reference to the task which periodically ensures that stored data are synced to the disk.
     */
    private var syncTask: ScheduledFuture<*>? = null

    /**
     * Throwable caught during commit after which rollback of highAddress failed.
     * Generally, it should ne null, otherwise environment is inoperative:
     * no transaction can be started or committed in that state. Once environment became inoperative,
     * it will remain inoperative forever.
     */
    @Volatile
    var throwableOnCommit: Throwable? = null
    private var throwableOnClose: Throwable? = null
    private var stuckTxnMonitor: StuckTransactionMonitor? = null
    private var streamCipherProvider: StreamCipherProvider? = null
    private val cipherKey: ByteArray?
    private var cipherBasicIV: Long = 0

    @JvmField
    var checkBlobs = false
    var isCheckLuceneDirectory = false

    init {
        try {
            this.log = log
            this.ec = ec
            val logLocation = log.location
            applyEnvironmentSettings(logLocation, ec)
            checkStorageType(logLocation, ec)
            val readerWriterProvider = log.config.getReaderWriterProvider()
            @Suppress("LeakingThis")
            readerWriterProvider.onEnvironmentCreated(this)
            val meta: Pair<MetaTreeImpl, Int>
            val expired = ExpiredLoggableCollection.newInstance(log)
            synchronized(commitLock) { meta = MetaTreeImpl.create(this, expired) }
            metaTreeInternal = meta.getFirst()
            structureId = AtomicInteger(meta.getSecond())
            txns = TransactionSet()
            txnSafeTasks = LinkedList()
            invalidateStoreGetCache()
            envSettingsListener = EnvironmentSettingsListener()
            ec.addChangedSettingsListener(envSettingsListener!!)
            @Suppress("LeakingThis")
            gc = GarbageCollector(this)
            val metaLock = ReentrantReadWriteLock()
            metaReadLock = metaLock.readLock()
            metaWriteLock = metaLock.writeLock()
            txnDispatcher = ReentrantTransactionDispatcher(ec.envMaxParallelTxns)
            @Suppress("LeakingThis")
            statistics = EnvironmentStatistics(this)
            txnProfiler = if (ec.profilerEnabled) TxnProfiler() else null
            @Suppress("LeakingThis")
            val configMBean = if (ec.isManagementEnabled) createConfigMBean(this) else null
            if (configMBean != null) {
                this.configMBean = configMBean
                // if we don't gather statistics then we should not expose corresponding managed bean
                @Suppress("LeakingThis")
                statisticsMBean =
                    if (ec.envGatherStatistics) jetbrains.exodus.env.management.EnvironmentStatistics(this) else null
                @Suppress("LeakingThis")
                profilerMBean = if (txnProfiler == null) null else DatabaseProfiler(this)
            } else {
                this.configMBean = null
                statisticsMBean = null
                profilerMBean = null
            }
            @Suppress("LeakingThis")
            backupController = BackupController(this)
            throwableOnCommit = null
            throwableOnClose = null
            @Suppress("LeakingThis")
            stuckTxnMonitor =
                if (transactionTimeout() > 0 || transactionExpirationTimeout() > 0) StuckTransactionMonitor(this) else null
            val logConfig = log.config
            streamCipherProvider = logConfig.streamCipherProvider
            cipherKey = logConfig.cipherKey
            cipherBasicIV = logConfig.cipherBasicIV
            syncTask = if (!isReadOnly) {
                @Suppress("LeakingThis")
                SyncIO.scheduleSyncLoop(this)
            } else {
                null
            }
            gc.fetchExpiredLoggables(expired)
            loggerInfo("Exodus environment created: $logLocation")
        } catch (e: Exception) {
            logger.error("Error during opening the environment " + log.location, e)
            log.switchToReadOnlyMode()
            log.release()
            throw e
        }
    }

    override fun getCreated(): Long {
        return log.created
    }

    override fun getLocation(): String {
        return log.location
    }

    override fun openBitmap(
        name: String,
        config: StoreConfig,
        transaction: Transaction
    ): BitmapImpl {
        if (config.duplicates) {
            throw ExodusException("Bitmap can't be opened at top of the store with duplicates")
        }
        val store = openStore("$name#bitmap", config, transaction)
        return BitmapImpl(store)
    }

    override fun getEnvironmentConfig(): EnvironmentConfig {
        return ec
    }

    override fun getStatistics(): EnvironmentStatistics {
        return statistics
    }

    override fun openStore(
        name: String,
        config: StoreConfig,
        transaction: Transaction
    ): StoreImpl {
        val txn = transaction as TransactionBase
        return openStoreImpl(name, config, txn, txn.getTreeMetaInfo(name))
    }

    override fun openStore(
        name: String,
        config: StoreConfig,
        transaction: Transaction,
        creationRequired: Boolean
    ): StoreImpl? {
        val txn = transaction as TransactionBase
        val metaInfo = txn.getTreeMetaInfo(name)
        return if (metaInfo == null && !creationRequired) {
            null
        } else openStoreImpl(name, config, txn, metaInfo)
    }

    override fun beginTransaction(): TransactionBase {
        return beginTransaction(null, exclusive = false, cloneMeta = false)
    }

    override fun beginTransaction(beginHook: Runnable?): TransactionBase {
        return beginTransaction(beginHook, exclusive = false, cloneMeta = false)
    }

    override fun beginExclusiveTransaction(): Transaction {
        return beginTransaction(null, exclusive = true, cloneMeta = false)
    }

    override fun beginExclusiveTransaction(beginHook: Runnable): Transaction {
        return beginTransaction(beginHook, exclusive = true, cloneMeta = false)
    }

    override fun beginReadonlyTransaction(): Transaction {
        return beginReadonlyTransaction(null)
    }

    override fun beginReadonlyTransaction(beginHook: Runnable?): TransactionBase {
        checkIsOperative()
        return ReadonlyTransaction(this, false, beginHook)
    }

    open fun beginGCTransaction(): ReadWriteTransaction {
        if (isReadOnly) {
            throw ReadonlyTransactionException("Can't start GC transaction on read-only Environment")
        }
        return object : ReadWriteTransaction(this@EnvironmentImpl, null, ec.gcUseExclusiveTransaction, true) {
            override val isGCTransaction: Boolean
                get() = true
        }
    }

    override fun executeInTransaction(executable: TransactionalExecutable) {
        executeInTransaction(executable, beginTransaction())
    }

    override fun executeInExclusiveTransaction(executable: TransactionalExecutable) {
        executeInTransaction(executable, beginExclusiveTransaction())
    }

    override fun executeInReadonlyTransaction(executable: TransactionalExecutable) {
        val txn = beginReadonlyTransaction()
        try {
            executable.execute(txn)
        } finally {
            abortIfNotFinished(txn)
        }
    }

    override fun <T> computeInTransaction(computable: TransactionalComputable<T>): T {
        return computeInTransaction(computable, beginTransaction())
    }

    override fun <T> computeInExclusiveTransaction(computable: TransactionalComputable<T>): T {
        return computeInTransaction(computable, beginExclusiveTransaction())
    }

    override fun <T> computeInReadonlyTransaction(computable: TransactionalComputable<T>): T {
        val txn = beginReadonlyTransaction()
        return try {
            computable.compute(txn)
        } finally {
            abortIfNotFinished(txn)
        }
    }

    override fun executeTransactionSafeTask(task: Runnable) {
        val newestTxnRoot = txns.newestTxnRootAddress
        if (newestTxnRoot == Long.MIN_VALUE) {
            task.run()
        } else {
            synchronized(txnSafeTasks) { txnSafeTasks.addLast(RunnableWithTxnRoot(task, newestTxnRoot)) }
        }
    }

    val stuckTransactionCount: Int
        get() {
            val stuckTxnMonitor = this.stuckTxnMonitor
            return stuckTxnMonitor?.stuckTxnCount ?: 0
        }

    override fun getCipherProvider(): StreamCipherProvider? {
        return streamCipherProvider
    }

    override fun getCipherKey(): ByteArray? {
        return cipherKey
    }

    override fun getCipherBasicIV(): Long {
        return cipherBasicIV
    }

    override fun clear() {
        val currentThread = Thread.currentThread()
        if (txnDispatcher.getThreadPermits(currentThread) != 0) {
            throw ExodusException("Environment.clear() can't proceed if there is a transaction in current thread")
        }
        runAllTransactionSafeTasks()
        synchronized(txnSafeTasks) { txnSafeTasks.clear() }
        suspendGC()
        try {
            val permits =
                txnDispatcher.acquireExclusiveTransaction(currentThread) // wait for and stop all writing transactions
            try {
                synchronized(commitLock) {
                    metaWriteLock.lock()
                    try {
                        gc.clear()
                        log.clear()
                        invalidateStoreGetCache()
                        throwableOnCommit = null
                        val expired = ExpiredLoggableCollection.newInstance(log)
                        val meta: Pair<MetaTreeImpl, Int> = MetaTreeImpl.create(this, expired)
                        metaTreeInternal = meta.getFirst()
                        structureId.set(meta.getSecond())
                        gc.fetchExpiredLoggables(expired)
                    } finally {
                        metaWriteLock.unlock()
                    }
                }
            } finally {
                txnDispatcher.releaseTransaction(currentThread, permits)
            }
        } finally {
            resumeGC()
        }
    }

    override fun isReadOnly(): Boolean {
        return ec.envIsReadonly || log.isReadOnly
    }

    override fun close() {
        // if this is already closed do nothing
        synchronized(commitLock) {
            if (!isOpen) {
                return
            }
        }
        val metaServer = ec.metaServer
        metaServer?.stop(this)
        backupController.unregister()
        configMBean?.unregister()
        if (statisticsMBean != null) {
            statisticsMBean!!.unregister()
        }
        if (profilerMBean != null) {
            profilerMBean!!.unregister()
        }
        runAllTransactionSafeTasks()
        // in order to avoid deadlock, do not finish gc inside lock
        // it is safe to invoke gc.finish() several times
        gc.finish()
        val logCacheHitRate: Float
        val storeGetCacheHitRate: Float
        synchronized(commitLock) {

            // concurrent close() detected
            if (throwableOnClose != null) {
                throw EnvironmentClosedException(throwableOnClose) // add combined stack trace information
            }
            val closeForcedly = ec.envCloseForcedly
            checkInactive(closeForcedly)
            try {
                if (!closeForcedly && !isReadOnly && ec.isGcEnabled) {
                    executeInTransaction { txn: Transaction? ->
                        gc.utilizationProfile.forceSave(
                            txn!!
                        )
                    }
                }
                ec.removeChangedSettingsListener(envSettingsListener!!)
                logCacheHitRate = log.cacheHitRate
                if (!isReadOnly) {
                    metaReadLock.lock()
                    try {
                        log.updateStartUpDbRoot(metaTreeInternal.rootAddress())
                    } finally {
                        metaReadLock.unlock()
                    }
                }
                if (syncTask != null) {
                    syncTask!!.cancel(false)
                }
                log.close()
            } finally {
                log.release()
            }
            if (storeGetCache == null) {
                storeGetCacheHitRate = 0f
            } else {
                storeGetCacheHitRate = storeGetCache!!.hitRate()
                storeGetCache!!.close()
            }
            if (txnProfiler != null) {
                txnProfiler!!.dump()
            }
            throwableOnClose = EnvironmentClosedException()
            throwableOnCommit = throwableOnClose
        }
        loggerDebug("Store get cache hit rate: " + ObjectCacheBase.formatHitRate(storeGetCacheHitRate))
        loggerDebug("Exodus log cache hit rate: " + ObjectCacheBase.formatHitRate(logCacheHitRate))
    }

    override fun isOpen(): Boolean {
        return throwableOnClose == null
    }

    override fun getBackupStrategy(): BackupStrategy {
        return EnvironmentBackupStrategyImpl(this)
    }

    override fun truncateStore(storeName: String, txn: Transaction) {
        val t = throwIfReadonly(txn, "Can't truncate a store in read-only transaction")
        var store = openStore(storeName, StoreConfig.USE_EXISTING, t, false)
            ?: throw ExodusException("Attempt to truncate unknown store '$storeName'")
        t.storeRemoved(store)
        val metaInfoCloned = store.metaInfo.clone(allocateStructureId())
        store = StoreImpl(this, storeName, metaInfoCloned)
        t.storeCreated(store)
    }

    override fun removeStore(storeName: String, txn: Transaction) {
        val t = throwIfReadonly(txn, "Can't remove a store in read-only transaction")
        val store = openStore(storeName, StoreConfig.USE_EXISTING, t, false)
            ?: throw ExodusException("Attempt to remove unknown store '$storeName'")
        t.storeRemoved(store)
    }

    val allStoreCount: Long
        get() {
            metaReadLock.lock()
            return try {
                metaTreeInternal.allStoreCount
            } finally {
                metaReadLock.unlock()
            }
        }

    override fun getAllStoreNames(txn: Transaction): List<String> {
        checkIfTransactionCreatedAgainstThis(txn)
        return (txn as TransactionBase).allStoreNames
    }

    override fun storeExists(storeName: String, txn: Transaction): Boolean {
        return (txn as TransactionBase).getTreeMetaInfo(storeName) != null
    }

    override fun gc() {
        gc.wake(true)
    }

    override fun suspendGC() {
        gc.suspend()
    }

    override fun resumeGC() {
        gc.resume()
    }

    override fun executeBeforeGc(action: Runnable) {
        gc.addBeforeGcAction(action)
    }

    val bTreeBalancePolicy: BTreeBalancePolicy
        get() {
            // we don't care of possible race condition here
            if (balancePolicy == null) {
                balancePolicy = BTreeBalancePolicy(ec.treeMaxPageSize, ec.treeDupMaxPageSize)
            }
            return balancePolicy!!
        }

    /**
     * Flushes Log's data writer exclusively in commit lock. This guarantees that the data writer is in committed state.
     * Also performs syncing cached by OS data to storage device.
     */
    fun flushAndSync() {
        synchronized(commitLock) {
            if (isOpen) {
                val log = log
                log.beginWrite()
                try {
                    log.sync()
                } finally {
                    log.endWrite()
                }
            }
        }
    }

    fun flushSyncAndFillPagesWithNulls(): LongArray {
        var highAddress: Long
        var rootAddress: Long
        var expiredLoggables: ExpiredLoggableCollection
        synchronized(commitLock) {
            val log = log
            expiredLoggables = ExpiredLoggableCollection.newInstance(log)
            log.padPageWithNulls(expiredLoggables)
            log.beginWrite()
            try {
                log.sync()
            } finally {
                log.endWrite()
            }
            highAddress = log.highAddress
            rootAddress = metaTreeInternal.root
        }
        gc.fetchExpiredLoggables(expiredLoggables)
        return longArrayOf(highAddress, rootAddress)
    }

    fun prepareForBackup() {
        if (isOpen) {
            gc.suspend()
            val highAddressAndRoot = flushSyncAndFillPagesWithNulls()
            val fileAddress = log.getFileAddress(highAddressAndRoot[0])
            val fileOffset = highAddressAndRoot[0] - fileAddress
            val metadata = BackupMetadata.serialize(
                1, CURRENT_FORMAT_VERSION,
                highAddressAndRoot[1], log.cachePageSize, log.fileLengthBound,
                true, fileAddress, fileOffset
            )
            val backupMetadata = Paths.get(log.location).resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME)
            try {
                Files.deleteIfExists(backupMetadata)
            } catch (e: IOException) {
                throw ExodusException("Error deletion of previous backup metadata", e)
            }
            try {
                FileChannel.open(
                    backupMetadata, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                ).use { channel ->
                    while (metadata.remaining() > 0) {
                        channel.write(metadata)
                    }
                }
            } catch (e: IOException) {
                throw ExodusException("Error during generation of backup metadata", e)
            }
            return
        }
        throw IllegalStateException("Environment is closed")
    }

    fun finishBackup() {
        if (isOpen) {
            gc.resume()
        }
        val backupMetadata = Paths.get(log.location).resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME)
        try {
            Files.deleteIfExists(backupMetadata)
        } catch (e: IOException) {
            throw ExodusException("Error deletion of previous backup metadata", e)
        }
    }

    fun removeFiles(files: LongArray, rbt: RemoveBlockType) {
        synchronized(commitLock) {
            log.beginWrite()
            try {
                log.forgetFiles(files)
                log.endWrite()
            } catch (t: Throwable) {
                throw ExodusException.toExodusException(t, "Failed to forget files in log")
            }
        }
        for (file in files) {
            log.removeFile(file, rbt)
        }
    }

    val storeGetCacheHitRate: Float
        get() = if (storeGetCache == null) 0.0f else storeGetCache!!.hitRate()

    protected open fun createStore(name: String, metaInfo: TreeMetaInfo): StoreImpl {
        return StoreImpl(this, name, metaInfo)
    }

    open fun finishTransaction(txn: TransactionBase) {
        if (!txn.isReadonly) {
            releaseTransaction(txn)
        }
        txns.remove(txn)
        txn.setIsFinished()
        val duration = System.currentTimeMillis() - txn.created
        if (txn.isReadonly) {
            statistics.getStatisticsItem(EnvironmentStatistics.Type.READONLY_TRANSACTIONS).incTotal()
            statistics.getStatisticsItem(EnvironmentStatistics.Type.READONLY_TRANSACTIONS_DURATION).addTotal(duration)
        } else if (txn.isGCTransaction) {
            statistics.getStatisticsItem(EnvironmentStatistics.Type.GC_TRANSACTIONS).incTotal()
            statistics.getStatisticsItem(EnvironmentStatistics.Type.GC_TRANSACTIONS_DURATION).addTotal(duration)
        } else {
            statistics.getStatisticsItem(EnvironmentStatistics.Type.TRANSACTIONS).incTotal()
            statistics.getStatisticsItem(EnvironmentStatistics.Type.TRANSACTIONS_DURATION).addTotal(duration)
        }
        runTransactionSafeTasks()
    }

    protected open fun beginTransaction(beginHook: Runnable?, exclusive: Boolean, cloneMeta: Boolean): TransactionBase {
        checkIsOperative()
        return if (isReadOnly && ec.envFailFastInReadonly) ReadonlyTransaction(
            this,
            exclusive,
            beginHook
        ) else ReadWriteTransaction(this, beginHook, exclusive, cloneMeta)
    }

    val diskUsage: Long
        get() = log.diskUsage

    fun acquireTransaction(txn: TransactionBase) {
        checkIfTransactionCreatedAgainstThis(txn)
        txnDispatcher.acquireTransaction(
            throwIfReadonly(
                txn, "TxnDispatcher can't acquire permits for read-only transaction"
            ), this
        )
    }

    fun releaseTransaction(txn: TransactionBase) {
        checkIfTransactionCreatedAgainstThis(txn)
        txnDispatcher.releaseTransaction(
            throwIfReadonly(
                txn, "TxnDispatcher can't release permits for read-only transaction"
            )
        )
    }

    fun downgradeTransaction(txn: TransactionBase) {
        txnDispatcher.downgradeTransaction(
            throwIfReadonly(
                txn, "TxnDispatcher can't downgrade read-only transaction"
            )
        )
    }

    fun shouldTransactionBeExclusive(txn: ReadWriteTransaction): Boolean {
        val replayCount = txn.replayCount
        return replayCount >= ec.envTxnReplayMaxCount ||
                System.currentTimeMillis() - txn.created >= ec.envTxnReplayTimeout
    }

    /**
     * @return timeout for a transaction in milliseconds, or 0 if no timeout is configured
     */
    fun transactionTimeout(): Int {
        return ec.envMonitorTxnsTimeout
    }

    /**
     * @return expiration timeout for a transaction in milliseconds, or 0 if no timeout is configured
     */
    fun transactionExpirationTimeout(): Int {
        return ec.envMonitorTxnsExpirationTimeout
    }

    fun loadMetaTree(rootAddress: Long): BTree {
        return object : BTree(log, bTreeBalancePolicy, rootAddress, false, META_TREE_ID) {
            override fun getDataIterator(address: Long): DataIterator {
                return DataIterator(log, address)
            }
        }
    }

    fun commitTransaction(txn: ReadWriteTransaction): Boolean {
        if (flushTransaction(txn, false)) {
            finishTransaction(txn)
            return true
        }
        return false
    }

    fun flushTransaction(txn: ReadWriteTransaction, forceCommit: Boolean): Boolean {
        checkIfTransactionCreatedAgainstThis(txn)
        if (!forceCommit && txn.isIdempotent) {
            return true
        }
        val expiredLoggables: ExpiredLoggableCollection?
        val initialHighAddress: Long
        val resultingHighAddress: Long
        val isGcTransaction = txn.isGCTransaction
        var wasUpSaved = false
        val up = gc.utilizationProfile
        if (!isGcTransaction && up.isDirty) {
            up.save(txn)
            wasUpSaved = true
        }
        synchronized(commitLock) {
            if (isReadOnly) {
                throw ReadonlyTransactionException()
            }
            checkIsOperative()
            if (txn.invalidVersion(metaTreeInternal.root)) {
                // meta lock not needed 'cause write can only occur in another commit lock
                return false
            }
            txn.executeBeforeTransactionFlushAction()
            if (wasUpSaved) {
                up.isDirty = false
            }
            initialHighAddress = log.beginWrite()
            try {
                val tree = arrayOfNulls<Proto>(1)
                val updatedHighAddress: Long
                expiredLoggables = try {
                    txn.doCommit(tree, log)
                } finally {
                    log.flush()
                    updatedHighAddress = log.endWrite()
                }
                val proto = tree[0]
                metaWriteLock.lock()
                try {
                    resultingHighAddress = updatedHighAddress
                    txn.metaTree = MetaTreeImpl.create(this, proto!!).also { metaTreeInternal = it }
                    txn.executeCommitHook()
                } finally {
                    metaWriteLock.unlock()
                }
                // update txn profiler within commitLock
                updateTxnProfiler(txn, initialHighAddress, resultingHighAddress)
            } catch (t: Throwable) {
                val errorMessage = "Failed to flush transaction. Please close and open environment " +
                        "to trigger environment recovery routine"
                loggerError(errorMessage, t)
                log.switchToReadOnlyMode()
                throwableOnCommit = t
                throw ExodusException.toExodusException(t, errorMessage)
            }
        }
        gc.fetchExpiredLoggables(expiredLoggables!!)

        // update statistics
        statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_WRITTEN).total = resultingHighAddress
        if (isGcTransaction) {
            statistics.getStatisticsItem(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC)
                .addTotal(resultingHighAddress - initialHighAddress)
        }
        statistics.getStatisticsItem(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS).incTotal()
        return true
    }

    @JvmOverloads
    fun holdNewestSnapshotBy(txn: TransactionBase, acquireTxn: Boolean = true): MetaTreeImpl? {
        if (acquireTxn) {
            acquireTransaction(txn)
        }
        val beginHook = txn.beginHook
        metaReadLock.lock()
        return try {
            beginHook?.run()
            metaTreeInternal
        } finally {
            metaReadLock.unlock()
        }
    }

    /**
     * Opens or creates store just like openStore() with the same parameters does, but gets parameters
     * that are not annotated. This allows to pass, e.g., nullable transaction.
     *
     * @param name     store name
     * @param config   store configuration
     * @param txn      transaction, should not null if store doesn't exists
     * @param metaInfo target meta information
     * @return store object
     */
    fun openStoreImpl(
        name: String,
        config: StoreConfig,
        txn: TransactionBase,
        metaInfo: TreeMetaInfo?
    ): StoreImpl {
        var resultConfig = config
        var resultMetaInfo = metaInfo
        checkIfTransactionCreatedAgainstThis(txn)
        if (resultConfig.useExisting) { // this parameter requires to recalculate
            resultConfig = if (resultMetaInfo == null) {
                throw ExodusException("Can't restore meta information for store $name")
            } else {
                TreeMetaInfo.toConfig(resultMetaInfo)
            }
        }
        val result: StoreImpl
        if (resultMetaInfo == null) {
            if (txn.isReadonly && ec.envReadonlyEmptyStores) {
                return createTemporaryEmptyStore(name)
            }
            val structureId = allocateStructureId()
            resultMetaInfo = TreeMetaInfo.load(this, resultConfig.duplicates, resultConfig.prefixing, structureId)
            result = createStore(name, resultMetaInfo)
            val tx = throwIfReadonly(txn, "Can't create a store in read-only transaction")
            tx.getMutableTree(result)
            tx.storeCreated(result)
        } else {
            val hasDuplicates = resultMetaInfo.hasDuplicates()
            if (hasDuplicates != resultConfig.duplicates) {
                throw ExodusException(
                    "Attempt to open store '" + name + "' with duplicates = " +
                            resultConfig.duplicates + " while it was created with duplicates =" + hasDuplicates
                )
            }
            if (resultMetaInfo.isKeyPrefixing() != resultConfig.prefixing) {
                if (!resultConfig.prefixing) {
                    throw ExodusException(
                        "Attempt to open store '" + name +
                                "' with prefixing = false while it was created with prefixing = true"
                    )
                }
                // if we're trying to open existing store with prefixing which actually wasn't created as store
                // with prefixing due to lack of the PatriciaTree feature, then open store with existing config
                resultMetaInfo = TreeMetaInfo.load(
                    this, hasDuplicates, false,
                    resultMetaInfo.structureId
                )
            }
            result = createStore(name, resultMetaInfo)
            // XD-774: if the store was just removed in the same txn forget the removal
            if (txn is ReadWriteTransaction) {
                txn.storeOpened(result)
            }
        }
        return result
    }

    val lastStructureId: Int
        get() = structureId.get()

    fun registerTransaction(txn: TransactionBase) {
        checkIfTransactionCreatedAgainstThis(txn)
        // N.B! due to TransactionImpl.revert(), there can appear a txn which is already in the transaction set
        // any implementation of transaction set should process this well
        txns.add(txn)
    }

    fun isRegistered(txn: ReadWriteTransaction): Boolean {
        checkIfTransactionCreatedAgainstThis(txn)
        return txns.contains(txn)
    }

    fun activeTransactions(): Int {
        return txns.size()
    }

    fun runTransactionSafeTasks() {
        if (throwableOnCommit == null) {
            var tasksToRun: MutableList<Runnable>? = null
            val oldestTxnRoot = txns.oldestTxnRootAddress
            synchronized(txnSafeTasks) {
                while (true) {
                    if (!txnSafeTasks.isEmpty()) {
                        val r = txnSafeTasks.first
                        if (r.txnRoot < oldestTxnRoot) {
                            txnSafeTasks.removeFirst()
                            if (tasksToRun == null) {
                                tasksToRun = ArrayList(4)
                            }
                            tasksToRun!!.add(r.runnable)
                            continue
                        }
                    }
                    break
                }
            }
            if (tasksToRun != null) {
                for (task in tasksToRun!!) {
                    task.run()
                }
            }
        }
    }

    fun forEachActiveTransaction(executable: TransactionalExecutable) {
        txns.forEach(executable)
    }

    protected open fun createTemporaryEmptyStore(name: String): StoreImpl {
        return TemporaryEmptyStore(this, name)
    }

    private fun runAllTransactionSafeTasks() {
        if (throwableOnCommit == null) {
            synchronized(txnSafeTasks) {
                for (r in txnSafeTasks) {
                    r.runnable.run()
                }
            }
            DeferredIO.getJobProcessor().waitForJobs(100)
        }
    }

    private fun checkIfTransactionCreatedAgainstThis(txn: Transaction) {
        if (txn.environment !== this) {
            throw ExodusException("Transaction is created against another Environment")
        }
    }

    private fun checkInactive(exceptionSafe: Boolean) {
        var txnCount = txns.size()
        if (!exceptionSafe && txnCount > 0) {
            ensureIdle()
            txnCount = txns.size()
        }
        if (txnCount > 0) {
            val errorString = "Environment[$location] is active: $txnCount transaction(s) not finished"
            if (!exceptionSafe) {
                loggerError(errorString)
            } else {
                loggerInfo(errorString)
            }
            if (!exceptionSafe) {
                reportAliveTransactions(false)
            } else if (logger.isDebugEnabled) {
                reportAliveTransactions(true)
            }
        }
        if (!exceptionSafe) {
            if (txnCount > 0) {
                throw ExodusException("Finish all transactions before closing database environment")
            }
        }
    }

    private fun reportAliveTransactions(debug: Boolean) {
        if (transactionTimeout() == 0) {
            val stacksUnavailable = "Transactions stack traces are not available, " +
                    "set '" + EnvironmentConfig.ENV_MONITOR_TXNS_TIMEOUT + " > 0'"
            if (debug) {
                loggerDebug(stacksUnavailable)
            } else {
                loggerError(stacksUnavailable)
            }
        } else {
            forEachActiveTransaction { txn: Transaction ->
                val trace = (txn as TransactionBase).trace
                if (debug) {
                    loggerDebug("Alive transaction:\n$trace")
                } else {
                    loggerError("Alive transaction:\n$trace")
                }
            }
        }
    }

    private fun checkIsOperative() {
        val t = throwableOnCommit
        if (t != null) {
            if (t is EnvironmentClosedException) {
                throw ExodusException("Environment is inoperative", t)
            }
            throw ExodusException.toExodusException(t, "Environment is inoperative")
        }
    }

    private fun allocateStructureId(): Int {
        /*
         * <TRICK>
         * Allocates structure id so that 256 doesn't factor it. This ensures that corresponding byte iterable
         * will never end with zero byte, and any such id can be used as a key in meta tree without collision
         * with a string key (store name). String keys (according to StringBinding) do always end with zero byte.
         * </TRICK>
         */
        while (true) {
            val result = structureId.incrementAndGet()
            if (result and 0xff != 0) {
                return result
            }
        }
    }

    private fun invalidateStoreGetCache() {
        val storeGetCacheSize = ec.envStoreGetCacheSize
        storeGetCache = if (storeGetCacheSize == 0) null else StoreGetCache(
            storeGetCacheSize,
            ec.envStoreGetCacheMinTreeSize,
            ec.envStoreGetCacheMaxValueSize
        )
    }

    private fun updateTxnProfiler(txn: TransactionBase, initialHighAddress: Long, resultingHighAddress: Long) {
        val txnProfiler = this.txnProfiler
        if (txnProfiler != null) {
            val writtenBytes = resultingHighAddress - initialHighAddress
            if (txn.isGCTransaction) {
                txnProfiler.incGcTransaction()
                txnProfiler.addGcMovedBytes(writtenBytes)
            } else if (txn.isReadonly) {
                txnProfiler.addReadonlyTxn(txn)
            } else {
                txnProfiler.addTxn(txn, writtenBytes)
            }
        }
    }

    private inner class EnvironmentSettingsListener : ConfigSettingChangeListener {
        override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
            if (key == EnvironmentConfig.ENV_IS_READONLY) {
                if (java.lang.Boolean.TRUE == value) {
                    suspendGC()
                    val txn = beginTransaction()
                    try {
                        if (!txn.isReadonly) {
                            gc.utilizationProfile.forceSave(txn)
                            txn.setCommitHook {
                                EnvironmentConfig.suppressConfigChangeListenersForThread()
                                ec.setEnvIsReadonly(true)
                                EnvironmentConfig.resumeConfigChangeListenersForThread()
                            }
                            (txn as ReadWriteTransaction).forceFlush()
                        }
                    } finally {
                        txn.abort()
                    }
                    if (!log.isReadOnly) {
                        log.updateStartUpDbRoot(metaTreeInternal.root)
                        flushSyncAndFillPagesWithNulls()
                    }
                }
            }
        }

        override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
            if (key == EnvironmentConfig.ENV_STOREGET_CACHE_SIZE || key == EnvironmentConfig.ENV_STOREGET_CACHE_MIN_TREE_SIZE || key == EnvironmentConfig.ENV_STOREGET_CACHE_MAX_VALUE_SIZE) {
                invalidateStoreGetCache()
            } else if (key == EnvironmentConfig.LOG_SYNC_PERIOD) {
                log.config.setSyncPeriod(ec.logSyncPeriod)
            } else if (key == EnvironmentConfig.LOG_DURABLE_WRITE) {
                log.config.setDurableWrite(ec.logDurableWrite)
            } else if (key == EnvironmentConfig.ENV_IS_READONLY && !isReadOnly) {
                resumeGC()
            } else if (key == EnvironmentConfig.GC_UTILIZATION_FROM_SCRATCH && ec.gcUtilizationFromScratch) {
                gc.utilizationProfile.computeUtilizationFromScratch()
            } else if (key == EnvironmentConfig.GC_UTILIZATION_FROM_FILE) {
                gc.utilizationProfile.loadUtilizationFromFile((value as String))
            } else if (key == EnvironmentConfig.TREE_MAX_PAGE_SIZE) {
                balancePolicy = null
            } else if (key == EnvironmentConfig.TREE_DUP_MAX_PAGE_SIZE) {
                balancePolicy = null
            } else if (key == EnvironmentConfig.LOG_CACHE_READ_AHEAD_MULTIPLE) {
                log.config.cacheReadAheadMultiple = ec.logCacheReadAheadMultiple
            }
        }
    }

    private class RunnableWithTxnRoot(val runnable: Runnable, val txnRoot: Long)
    private object SyncIO {
        @Volatile
        private var syncExecutor: ScheduledExecutorService? = null
            get() {
                if (field == null) {
                    lock.lock()
                    try {
                        if (field == null) {
                            field = Executors.newScheduledThreadPool(1, SyncIOThreadFactory())
                        }
                    } finally {
                        lock.unlock()
                    }
                }
                return field
            }
        private val lock = ReentrantLock()
        fun scheduleSyncLoop(environment: EnvironmentImpl): ScheduledFuture<*> {
            val executor = syncExecutor
            val syncPeriod = environment.ec.logSyncPeriod
            return executor!!.scheduleWithFixedDelay({
                try {
                    if (environment.log.needsToBeSynchronized()) {
                        synchronized(environment.commitLock) {
                            if (environment.isOpen) {
                                if (environment.log.needsToBeSynchronized()) {
                                    environment.flushAndSync()
                                }
                            } else if (environment.syncTask != null) {
                                environment.syncTask!!.cancel(false)
                            }
                        }
                    }
                } catch (t: Throwable) {
                    logger.error(
                        "Error during synchronization of content of log " +
                                environment.log.location, t
                    )
                    environment.throwableOnCommit = t
                }
            }, syncPeriod, (syncPeriod / 10).coerceAtMost(1000), TimeUnit.MILLISECONDS)
        }
    }

    private class SyncIOThreadFactory : ThreadFactory {
        override fun newThread(r: Runnable): Thread {
            val thread = Thread(r)
            thread.name = "Scheduled Xodus data sync thread #" + idGen.getAndIncrement()
            thread.uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { t: Thread, e: Throwable? ->
                logger.error(
                    "Uncaught exception in thread$t", e
                )
            }
            thread.isDaemon = true
            return thread
        }

        companion object {
            private val idGen = AtomicLong()
        }
    }

    companion object {
        const val META_TREE_ID = 1
        private val logger = LoggerFactory.getLogger(EnvironmentImpl::class.java)

        const val CURRENT_FORMAT_VERSION = 2
        private const val ENVIRONMENT_PROPERTIES_FILE = "exodus.properties"
        fun isUtilizationProfile(storeName: String): Boolean {
            return GarbageCollector.isUtilizationProfile(storeName)
        }

        fun throwIfReadonly(txn: Transaction, exceptionMessage: String): ReadWriteTransaction {
            if (txn.isReadonly) {
                throw ReadonlyTransactionException(exceptionMessage)
            }
            return txn as ReadWriteTransaction
        }

        @JvmOverloads
        fun loggerError(errorMessage: String, t: Throwable? = null) {
            if (t == null) {
                logger.error(errorMessage)
            } else {
                logger.error(errorMessage, t)
            }
        }

        fun loggerInfo(message: String) {
            if (logger.isInfoEnabled) {
                logger.info(message)
            }
        }

        @JvmOverloads
        fun loggerDebug(message: String, t: Throwable? = null) {
            if (logger.isDebugEnabled) {
                if (t == null) {
                    logger.debug(message)
                } else {
                    logger.debug(message, t)
                }
            }
        }

        private fun applyEnvironmentSettings(
            location: String,
            ec: EnvironmentConfig
        ) {
            val propsFile = File(location, ENVIRONMENT_PROPERTIES_FILE)
            if (propsFile.exists() && propsFile.isFile) {
                try {
                    FileInputStream(propsFile).use { propsStream ->
                        val envProps = Properties()
                        envProps.load(propsStream)
                        for ((key, value) in envProps) {
                            ec.setSetting(key.toString(), value!!)
                        }
                    }
                } catch (e: IOException) {
                    throw ExodusException.toExodusException(e)
                }
            }
        }

        private fun checkStorageType(location: String, ec: EnvironmentConfig) {
            val provider = ec.logDataReaderWriterProvider
            if (provider == DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER) {
                val databaseDir = File(location)
                if (!ec.isLogAllowRemovable && isRemovableFile(databaseDir)) {
                    throw StorageTypeNotAllowedException("Database on removable storage is not allowed")
                }
                if (!ec.isLogAllowRemote && isRemoteFile(databaseDir)) {
                    throw StorageTypeNotAllowedException("Database on remote storage is not allowed")
                }
                if (!ec.isLogAllowRamDisk && isRamDiskFile(databaseDir)) {
                    throw StorageTypeNotAllowedException("Database on RAM disk is not allowed")
                }
            }
        }

        private fun createConfigMBean(e: EnvironmentImpl): jetbrains.exodus.env.management.EnvironmentConfig? {
            return try {
                if (e.ec.managementOperationsRestricted) jetbrains.exodus.env.management.EnvironmentConfig(e) else EnvironmentConfigWithOperations(
                    e
                )
            } catch (ex: RuntimeException) {
                if (ex.cause is InstanceAlreadyExistsException) {
                    return null
                }
                throw ex
            }
        }

        private fun executeInTransaction(
            executable: TransactionalExecutable,
            txn: Transaction
        ) {
            try {
                while (true) {
                    executable.execute(txn)
                    if (txn.isReadonly ||  // txn can be read-only if Environment is in read-only mode
                        txn.isFinished ||  // txn can be finished if, e.g., it was aborted within executable
                        txn.flush()
                    ) {
                        break
                    }
                    txn.revert()
                }
            } finally {
                abortIfNotFinished(txn)
            }
        }

        private fun <T> computeInTransaction(
            computable: TransactionalComputable<T>,
            txn: Transaction
        ): T {
            try {
                while (true) {
                    val result = computable.compute(txn)
                    if (txn.isReadonly ||  // txn can be read-only if Environment is in read-only mode
                        txn.isFinished ||  // txn can be finished if, e.g., it was aborted within computable
                        txn.flush()
                    ) {
                        return result
                    }
                    txn.revert()
                }
            } finally {
                abortIfNotFinished(txn)
            }
        }

        private fun abortIfNotFinished(txn: Transaction) {
            if (!txn.isFinished) {
                txn.abort()
            }
        }
    }
}
