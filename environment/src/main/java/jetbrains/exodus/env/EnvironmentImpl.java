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
package jetbrains.exodus.env;

import jetbrains.exodus.ConfigSettingChangeListener;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.execution.SharedTimer;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.debug.StackTrace;
import jetbrains.exodus.debug.TxnProfiler;
import jetbrains.exodus.entitystore.MetaServer;
import jetbrains.exodus.env.management.DatabaseProfiler;
import jetbrains.exodus.env.management.EnvironmentConfigWithOperations;
import jetbrains.exodus.gc.GarbageCollector;
import jetbrains.exodus.gc.UtilizationProfile;
import jetbrains.exodus.io.DataReaderWriterProvider;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.io.StorageTypeNotAllowedException;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import jetbrains.exodus.tree.TreeMetaInfo;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.*;

public class EnvironmentImpl implements Environment {

    public static final int META_TREE_ID = 1;

    private static final Logger logger = LoggerFactory.getLogger(EnvironmentImpl.class);

    public static final int CURRENT_FORMAT_VERSION = 2;

    private static final String ENVIRONMENT_PROPERTIES_FILE = "exodus.properties";

    @NotNull
    private final Log log;
    @NotNull
    private final EnvironmentConfig ec;
    private BTreeBalancePolicy balancePolicy;
    private MetaTreeImpl metaTree;
    private final AtomicInteger structureId;
    @NotNull
    private final TransactionSet txns;
    private final LinkedList<RunnableWithTxnRoot> txnSafeTasks;
    @Nullable
    private StoreGetCache storeGetCache;
    private final EnvironmentSettingsListener envSettingsListener;
    private final GarbageCollector gc;
    final Object commitLock = new Object();
    private final ReentrantReadWriteLock.ReadLock metaReadLock;
    final ReentrantReadWriteLock.WriteLock metaWriteLock;
    private final ReentrantTransactionDispatcher txnDispatcher;
    @NotNull
    private final EnvironmentStatistics statistics;
    @Nullable
    private final TxnProfiler txnProfiler;
    @Nullable
    private final jetbrains.exodus.env.management.EnvironmentConfig configMBean;
    @Nullable
    private final jetbrains.exodus.env.management.EnvironmentStatistics statisticsMBean;
    @Nullable
    private final DatabaseProfiler profilerMBean;

    /**
     * Reference to the task which periodically ensures that stored data are synced to the disk.
     */
    private final @Nullable ScheduledFuture<?> syncTask;

    /**
     * Throwable caught during commit after which rollback of highAddress failed.
     * Generally, it should ne null, otherwise environment is inoperative:
     * no transaction can be started or committed in that state. Once environment became inoperative,
     * it will remain inoperative forever.
     */
    volatile Throwable throwableOnCommit;
    private Throwable throwableOnClose;

    @Nullable
    private final StuckTransactionMonitor stuckTxnMonitor;

    @Nullable
    private final StreamCipherProvider streamCipherProvider;
    private final byte @Nullable [] cipherKey;
    private final long cipherBasicIV;

    @SuppressWarnings({"ThisEscapedInObjectConstruction"})
    EnvironmentImpl(@NotNull final Log log, @NotNull final EnvironmentConfig ec) {
        try {
            this.log = log;
            this.ec = ec;
            final String logLocation = log.getLocation();
            applyEnvironmentSettings(logLocation, ec);

            checkStorageType(logLocation, ec);

            final DataReaderWriterProvider readerWriterProvider = log.getConfig().getReaderWriterProvider();
            assert readerWriterProvider != null;
            readerWriterProvider.onEnvironmentCreated(this);

            final Pair<MetaTreeImpl, Integer> meta;
            final ExpiredLoggableCollection expired = ExpiredLoggableCollection.newInstance(log);

            synchronized (commitLock) {
                meta = MetaTreeImpl.create(this, expired);
            }
            metaTree = meta.getFirst();
            structureId = new AtomicInteger(meta.getSecond());
            txns = new TransactionSet();
            txnSafeTasks = new LinkedList<>();
            invalidateStoreGetCache();
            envSettingsListener = new EnvironmentSettingsListener();
            ec.addChangedSettingsListener(envSettingsListener);

            gc = new GarbageCollector(this);

            ReentrantReadWriteLock metaLock = new ReentrantReadWriteLock();
            metaReadLock = metaLock.readLock();
            metaWriteLock = metaLock.writeLock();

            txnDispatcher = new ReentrantTransactionDispatcher(ec.getEnvMaxParallelTxns());

            statistics = new EnvironmentStatistics(this);
            txnProfiler = ec.getProfilerEnabled() ? new TxnProfiler() : null;
            final jetbrains.exodus.env.management.EnvironmentConfig configMBean =
                    ec.isManagementEnabled() ? createConfigMBean(this) : null;
            if (configMBean != null) {
                this.configMBean = configMBean;
                // if we don't gather statistics then we should not expose corresponding managed bean
                statisticsMBean = ec.getEnvGatherStatistics() ? new jetbrains.exodus.env.management.EnvironmentStatistics(this) : null;
                profilerMBean = txnProfiler == null ? null : new DatabaseProfiler(this);
            } else {
                this.configMBean = null;
                statisticsMBean = null;
                profilerMBean = null;
            }

            throwableOnCommit = null;
            throwableOnClose = null;

            stuckTxnMonitor = (transactionTimeout() > 0 || transactionExpirationTimeout() > 0) ? new StuckTransactionMonitor(this) : null;

            final LogConfig logConfig = log.getConfig();
            streamCipherProvider = logConfig.getCipherProvider();
            cipherKey = logConfig.getCipherKey();
            cipherBasicIV = logConfig.getCipherBasicIV();

            if (!isReadOnly()) {
                syncTask = SyncIO.scheduleSyncLoop(this);
            } else {
                syncTask = null;
            }

            gc.fetchExpiredLoggables(expired);

            loggerInfo("Exodus environment created: " + logLocation);

            if (!log.getFormatWithHashCodeIsUsed()) {
                if (isReadOnly()) {
                    throw new ExodusException("Environment " + logLocation +
                            " uses out of dated binary format but can not be migrated because " +
                            "is opened in read-only mode.");

                }
            }
        } catch (Exception e) {
            logger.error("Error during opening the environment " + log.getLocation(), e);

            log.switchToReadOnlyMode();
            log.release();
            throw e;
        }
    }

    @Override
    public long getCreated() {
        return log.getCreated();
    }

    @Override
    @NotNull
    public String getLocation() {
        return log.getLocation();
    }

    @Override
    public @NotNull BitmapImpl openBitmap(@NotNull String name,
                                          @NotNull final StoreConfig config,
                                          @NotNull Transaction transaction) {
        if (config.duplicates) {
            throw new ExodusException("Bitmap can't be opened at top of the store with duplicates");
        }
        final StoreImpl store = openStore(name.concat("#bitmap"), config, transaction);
        return new BitmapImpl(store);
    }

    @Override
    @NotNull
    public EnvironmentConfig getEnvironmentConfig() {
        return ec;
    }

    @Override
    @NotNull
    public EnvironmentStatistics getStatistics() {
        return statistics;
    }

    public GarbageCollector getGC() {
        return gc;
    }

    @SuppressWarnings("MethodMayBeStatic")
    public int getCurrentFormatVersion() {
        return CURRENT_FORMAT_VERSION;
    }

    @Nullable
    public TxnProfiler getTxnProfiler() {
        return txnProfiler;
    }

    @Override
    @NotNull
    public StoreImpl openStore(@NotNull final String name,
                               @NotNull final StoreConfig config,
                               @NotNull final Transaction transaction) {
        final TransactionBase txn = (TransactionBase) transaction;
        return openStoreImpl(name, config, txn, txn.getTreeMetaInfo(name));
    }

    @Override
    @Nullable
    public StoreImpl openStore(@NotNull final String name,
                               @NotNull final StoreConfig config,
                               @NotNull final Transaction transaction,
                               final boolean creationRequired) {
        final TransactionBase txn = (TransactionBase) transaction;
        final TreeMetaInfo metaInfo = txn.getTreeMetaInfo(name);
        if (metaInfo == null && !creationRequired) {
            return null;
        }
        return openStoreImpl(name, config, txn, metaInfo);
    }

    @Override
    @NotNull
    public TransactionBase beginTransaction() {
        return beginTransaction(null, false, false);
    }

    @Override
    @NotNull
    public TransactionBase beginTransaction(final Runnable beginHook) {
        return beginTransaction(beginHook, false, false);
    }

    @NotNull
    @Override
    public Transaction beginExclusiveTransaction() {
        return beginTransaction(null, true, false);
    }

    @NotNull
    @Override
    public Transaction beginExclusiveTransaction(Runnable beginHook) {
        return beginTransaction(beginHook, true, false);
    }

    @NotNull
    @Override
    public Transaction beginReadonlyTransaction() {
        return beginReadonlyTransaction(null);
    }

    @NotNull
    @Override
    public TransactionBase beginReadonlyTransaction(final Runnable beginHook) {
        checkIsOperative();
        return new ReadonlyTransaction(this, false, beginHook);
    }

    @NotNull
    public ReadWriteTransaction beginGCTransaction() {
        if (isReadOnly()) {
            throw new ReadonlyTransactionException("Can't start GC transaction on read-only Environment");
        }

        return new ReadWriteTransaction(this, null, ec.getGcUseExclusiveTransaction(), true) {

            @Override
            boolean isGCTransaction() {
                return true;
            }
        };
    }

    public ReadonlyTransaction beginTransactionAt(final long highAddress) {
        checkIsOperative();
        return new ReadonlyTransaction(this, highAddress);
    }

    @Override
    public void executeInTransaction(@NotNull final TransactionalExecutable executable) {
        executeInTransaction(executable, beginTransaction());
    }

    @Override
    public void executeInExclusiveTransaction(@NotNull final TransactionalExecutable executable) {
        executeInTransaction(executable, beginExclusiveTransaction());
    }

    @Override
    public void executeInReadonlyTransaction(@NotNull TransactionalExecutable executable) {
        final Transaction txn = beginReadonlyTransaction();
        try {
            executable.execute(txn);
        } finally {
            abortIfNotFinished(txn);
        }
    }

    @Override
    public <T> T computeInTransaction(@NotNull TransactionalComputable<T> computable) {
        return computeInTransaction(computable, beginTransaction());
    }

    @Override
    public <T> T computeInExclusiveTransaction(@NotNull TransactionalComputable<T> computable) {
        return computeInTransaction(computable, beginExclusiveTransaction());
    }

    @Override
    public <T> T computeInReadonlyTransaction(@NotNull TransactionalComputable<T> computable) {
        final Transaction txn = beginReadonlyTransaction();
        try {
            return computable.compute(txn);
        } finally {
            abortIfNotFinished(txn);
        }
    }

    @Override
    public void executeTransactionSafeTask(@NotNull final Runnable task) {
        final long newestTxnRoot = txns.getNewestTxnRootAddress();
        if (newestTxnRoot == Long.MIN_VALUE) {
            task.run();
        } else {
            synchronized (txnSafeTasks) {
                txnSafeTasks.addLast(new RunnableWithTxnRoot(task, newestTxnRoot));
            }
        }
    }

    public int getStuckTransactionCount() {
        return stuckTxnMonitor == null ? 0 : stuckTxnMonitor.getStuckTxnCount();
    }

    @Override
    @Nullable
    public StreamCipherProvider getCipherProvider() {
        return streamCipherProvider;
    }

    @Override
    public byte @Nullable [] getCipherKey() {
        return cipherKey;
    }

    @Override
    public long getCipherBasicIV() {
        return cipherBasicIV;
    }

    @Override
    public void clear() {
        final Thread currentThread = Thread.currentThread();
        if (txnDispatcher.getThreadPermits(currentThread) != 0) {
            throw new ExodusException("Environment.clear() can't proceed if there is a transaction in current thread");
        }
        runAllTransactionSafeTasks();
        synchronized (txnSafeTasks) {
            txnSafeTasks.clear();
        }
        suspendGC();
        try {
            final int permits = txnDispatcher.acquireExclusiveTransaction(currentThread);// wait for and stop all writing transactions
            try {
                synchronized (commitLock) {
                    metaWriteLock.lock();
                    try {
                        gc.clear();
                        log.clear();
                        invalidateStoreGetCache();
                        throwableOnCommit = null;
                        final ExpiredLoggableCollection expired = ExpiredLoggableCollection.newInstance(log);
                        final Pair<MetaTreeImpl, Integer> meta = MetaTreeImpl.create(this, expired);
                        metaTree = meta.getFirst();
                        structureId.set(meta.getSecond());
                        gc.fetchExpiredLoggables(expired);
                    } finally {
                        metaWriteLock.unlock();
                    }
                }
            } finally {
                txnDispatcher.releaseTransaction(currentThread, permits);
            }
        } finally {
            resumeGC();
        }

    }

    public boolean isReadOnly() {
        return ec.getEnvIsReadonly() || log.isReadOnly();
    }

    @Override
    public void close() {
        // if this is already closed do nothing
        synchronized (commitLock) {
            if (!isOpen()) {
                return;
            }
        }
        final MetaServer metaServer = ec.getMetaServer();
        if (metaServer != null) {
            metaServer.stop(this);
        }
        if (configMBean != null) {
            configMBean.unregister();
        }
        if (statisticsMBean != null) {
            statisticsMBean.unregister();
        }
        if (profilerMBean != null) {
            profilerMBean.unregister();
        }
        runAllTransactionSafeTasks();
        // in order to avoid deadlock, do not finish gc inside lock
        // it is safe to invoke gc.finish() several times
        gc.finish();
        final float logCacheHitRate;
        final float storeGetCacheHitRate;
        synchronized (commitLock) {
            // concurrent close() detected
            if (throwableOnClose != null) {
                throw new EnvironmentClosedException(throwableOnClose); // add combined stack trace information
            }
            final boolean closeForcedly = ec.getEnvCloseForcedly();
            checkInactive(closeForcedly);
            try {
                if (!closeForcedly && !isReadOnly() && ec.isGcEnabled()) {
                    executeInTransaction(txn -> gc.getUtilizationProfile().forceSave(txn));
                }
                ec.removeChangedSettingsListener(envSettingsListener);
                logCacheHitRate = log.getCacheHitRate();

                if (!isReadOnly()) {
                    metaReadLock.lock();
                    try {
                        log.updateStartUpDbRoot(metaTree.rootAddress());
                    } finally {
                        metaReadLock.unlock();
                    }
                }

                if (syncTask != null) {
                    syncTask.cancel(false);
                }

                log.close();
            } finally {
                log.release();
            }
            if (storeGetCache == null) {
                storeGetCacheHitRate = 0;
            } else {
                storeGetCacheHitRate = storeGetCache.hitRate();
                storeGetCache.close();
            }
            if (txnProfiler != null) {
                txnProfiler.dump();
            }
            throwableOnClose = new EnvironmentClosedException();
            throwableOnCommit = throwableOnClose;
        }
        loggerDebug("Store get cache hit rate: " + ObjectCacheBase.formatHitRate(storeGetCacheHitRate));
        loggerDebug("Exodus log cache hit rate: " + ObjectCacheBase.formatHitRate(logCacheHitRate));
    }

    @Override
    public boolean isOpen() {
        return throwableOnClose == null;
    }

    @NotNull
    @Override
    public BackupStrategy getBackupStrategy() {
        return new EnvironmentBackupStrategyImpl(this);
    }

    @Override
    public void truncateStore(@NotNull final String storeName, @NotNull final Transaction txn) {
        final ReadWriteTransaction t = throwIfReadonly(txn, "Can't truncate a store in read-only transaction");
        StoreImpl store = openStore(storeName, StoreConfig.USE_EXISTING, t, false);
        if (store == null) {
            throw new ExodusException("Attempt to truncate unknown store '" + storeName + '\'');
        }
        t.storeRemoved(store);
        final TreeMetaInfo metaInfoCloned = store.getMetaInfo().clone(allocateStructureId());
        store = new StoreImpl(this, storeName, metaInfoCloned);
        t.storeCreated(store);
    }

    @Override
    public void removeStore(@NotNull final String storeName, @NotNull final Transaction txn) {
        final ReadWriteTransaction t = throwIfReadonly(txn, "Can't remove a store in read-only transaction");
        final StoreImpl store = openStore(storeName, StoreConfig.USE_EXISTING, t, false);
        if (store == null) {
            throw new ExodusException("Attempt to remove unknown store '" + storeName + '\'');
        }
        t.storeRemoved(store);
    }

    public long getAllStoreCount() {
        metaReadLock.lock();
        try {
            return metaTree.getAllStoreCount();
        } finally {
            metaReadLock.unlock();
        }
    }

    @Override
    @NotNull
    public List<String> getAllStoreNames(@NotNull final Transaction txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        return ((TransactionBase) txn).getAllStoreNames();
    }

    public boolean storeExists(@NotNull final String storeName, @NotNull final Transaction txn) {
        return ((TransactionBase) txn).getTreeMetaInfo(storeName) != null;
    }

    @NotNull
    public Log getLog() {
        return log;
    }

    @Override
    public void gc() {
        gc.wake(true);
    }

    @Override
    public void suspendGC() {
        gc.suspend();
    }

    @Override
    public void resumeGC() {
        gc.resume();
    }

    @Override
    public void executeBeforeGc(Runnable action) {
        gc.addBeforeGcAction(action);
    }

    public BTreeBalancePolicy getBTreeBalancePolicy() {
        // we don't care of possible race condition here
        if (balancePolicy == null) {
            balancePolicy = new BTreeBalancePolicy(ec.getTreeMaxPageSize(), ec.getTreeDupMaxPageSize());
        }
        return balancePolicy;
    }

    /**
     * Flushes Log's data writer exclusively in commit lock. This guarantees that the data writer is in committed state.
     * Also performs syncing cached by OS data to storage device.
     */
    public void flushAndSync() {
        synchronized (commitLock) {
            if (isOpen()) {
                var log = this.log;

                log.beginWrite();
                try {
                    log.sync();
                } finally {
                    log.endWrite();
                }
            }
        }
    }

    public void removeFiles(final long[] files, @NotNull final RemoveBlockType rbt) {
        synchronized (commitLock) {
            log.beginWrite();
            try {
                log.forgetFiles(files);
                log.endWrite();
            } catch (Throwable t) {
                throw ExodusException.toExodusException(t, "Failed to forget files in log");
            }
        }
        for (long file : files) {
            log.removeFile(file, rbt);
        }
    }

    public float getStoreGetCacheHitRate() {
        return storeGetCache == null ? 0 : storeGetCache.hitRate();
    }

    protected StoreImpl createStore(@NotNull final String name, @NotNull final TreeMetaInfo metaInfo) {
        return new StoreImpl(this, name, metaInfo);
    }

    protected void finishTransaction(@NotNull final TransactionBase txn) {
        if (!txn.isReadonly()) {
            releaseTransaction(txn);
        }
        txns.remove(txn);
        txn.setIsFinished();
        final long duration = System.currentTimeMillis() - txn.getCreated();
        if (txn.isReadonly()) {
            statistics.getStatisticsItem(READONLY_TRANSACTIONS).incTotal();
            statistics.getStatisticsItem(READONLY_TRANSACTIONS_DURATION).addTotal(duration);
        } else if (txn.isGCTransaction()) {
            statistics.getStatisticsItem(GC_TRANSACTIONS).incTotal();
            statistics.getStatisticsItem(GC_TRANSACTIONS_DURATION).addTotal(duration);
        } else {
            statistics.getStatisticsItem(TRANSACTIONS).incTotal();
            statistics.getStatisticsItem(TRANSACTIONS_DURATION).addTotal(duration);
        }
        runTransactionSafeTasks();
    }

    @NotNull
    protected TransactionBase beginTransaction(Runnable beginHook, boolean exclusive, boolean cloneMeta) {
        checkIsOperative();
        return isReadOnly() && ec.getEnvFailFastInReadonly() ?
                new ReadonlyTransaction(this, exclusive, beginHook) :
                new ReadWriteTransaction(this, beginHook, exclusive, cloneMeta);
    }

    @Nullable
    StoreGetCache getStoreGetCache() {
        return storeGetCache;
    }

    long getDiskUsage() {
        return log.getDiskUsage();
    }

    void acquireTransaction(@NotNull final TransactionBase txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        txnDispatcher.acquireTransaction(throwIfReadonly(
                txn, "TxnDispatcher can't acquire permits for read-only transaction"), this);
    }

    void releaseTransaction(@NotNull final TransactionBase txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        txnDispatcher.releaseTransaction(throwIfReadonly(
                txn, "TxnDispatcher can't release permits for read-only transaction"));
    }

    void downgradeTransaction(@NotNull final TransactionBase txn) {
        txnDispatcher.downgradeTransaction(throwIfReadonly(
                txn, "TxnDispatcher can't downgrade read-only transaction"));
    }

    boolean shouldTransactionBeExclusive(@NotNull final ReadWriteTransaction txn) {
        final int replayCount = txn.getReplayCount();
        return replayCount >= ec.getEnvTxnReplayMaxCount() ||
                System.currentTimeMillis() - txn.getCreated() >= ec.getEnvTxnReplayTimeout();
    }

    /**
     * @return timeout for a transaction in milliseconds, or 0 if no timeout is configured
     */
    int transactionTimeout() {
        return ec.getEnvMonitorTxnsTimeout();
    }

    /**
     * @return expiration timeout for a transaction in milliseconds, or 0 if no timeout is configured
     */
    int transactionExpirationTimeout() {
        return ec.getEnvMonitorTxnsExpirationTimeout();
    }

    /**
     * Tries to load meta tree located at specified rootAddress.
     *
     * @param rootAddress tree root address.
     * @return tree instance or null if the address is not valid.
     */
    @Nullable
    BTree loadMetaTree(final long rootAddress, final long highAddress) {
        if (rootAddress < 0 || rootAddress >= highAddress) return null;
        return new BTree(log, getBTreeBalancePolicy(), rootAddress, false, META_TREE_ID) {
            @NotNull
            @Override
            public DataIterator getDataIterator(long address) {
                return new DataIterator(log, address);
            }
        };
    }

    boolean commitTransaction(@NotNull final ReadWriteTransaction txn) {
        if (flushTransaction(txn, false)) {
            finishTransaction(txn);
            return true;
        }
        return false;
    }

    boolean flushTransaction(@NotNull final ReadWriteTransaction txn, final boolean forceCommit) {
        checkIfTransactionCreatedAgainstThis(txn);

        if (!forceCommit && txn.isIdempotent()) {
            return true;
        }

        final ExpiredLoggableCollection expiredLoggables;
        final long initialHighAddress;
        final long resultingHighAddress;
        final boolean isGcTransaction = txn.isGCTransaction();

        boolean wasUpSaved = false;
        final UtilizationProfile up = gc.getUtilizationProfile();
        if (!isGcTransaction && up.isDirty()) {
            up.save(txn);
            wasUpSaved = true;
        }

        synchronized (commitLock) {
            if (isReadOnly()) {
                throw new ReadonlyTransactionException();
            }
            checkIsOperative();
            if (txn.invalidVersion(metaTree.root)) {
                // meta lock not needed 'cause write can only occur in another commit lock
                return false;
            }

            txn.executeBeforeTransactionFlushAction();

            if (wasUpSaved) {
                up.setDirty(false);
            }
            initialHighAddress = log.beginWrite();
            try {
                final MetaTreeImpl.Proto[] tree = new MetaTreeImpl.Proto[1];
                final long updatedHighAddress;
                try {
                    expiredLoggables = txn.doCommit(tree, log);
                } finally {
                    log.flush();
                    updatedHighAddress = log.endWrite();
                }

                final MetaTreeImpl.Proto proto = tree[0];
                metaWriteLock.lock();
                try {
                    resultingHighAddress = updatedHighAddress;
                    txn.setMetaTree(metaTree = MetaTreeImpl.create(this, updatedHighAddress, proto));
                    txn.executeCommitHook();
                } finally {
                    metaWriteLock.unlock();
                }
                // update txn profiler within commitLock
                updateTxnProfiler(txn, initialHighAddress, resultingHighAddress);
            } catch (final Throwable t) {
                final String errorMessage = "Failed to flush transaction. Please close and open environment " +
                        "to trigger environment recovery routine";

                loggerError(errorMessage, t);

                log.switchToReadOnlyMode();
                throwableOnCommit = t;

                throw ExodusException.toExodusException(t, errorMessage);
            }
        }
        gc.fetchExpiredLoggables(expiredLoggables);

        // update statistics
        statistics.getStatisticsItem(BYTES_WRITTEN).setTotal(resultingHighAddress);
        if (isGcTransaction) {
            statistics.getStatisticsItem(BYTES_MOVED_BY_GC).addTotal(resultingHighAddress - initialHighAddress);
        }
        statistics.getStatisticsItem(FLUSHED_TRANSACTIONS).incTotal();

        return true;
    }

    @SuppressWarnings("UnusedReturnValue")
    MetaTreeImpl holdNewestSnapshotBy(@NotNull final TransactionBase txn) {
        return holdNewestSnapshotBy(txn, true);
    }

    MetaTreeImpl holdNewestSnapshotBy(@NotNull final TransactionBase txn, final boolean acquireTxn) {
        if (acquireTxn) {
            acquireTransaction(txn);
        }
        final Runnable beginHook = txn.getBeginHook();
        metaReadLock.lock();
        try {
            if (beginHook != null) {
                beginHook.run();
            }
            return metaTree;
        } finally {
            metaReadLock.unlock();
        }
    }

    public MetaTree getMetaTree() {
        metaReadLock.lock();
        try {
            return metaTree;
        } finally {
            metaReadLock.unlock();
        }
    }

    MetaTreeImpl getMetaTreeInternal() {
        return metaTree;
    }

    // unsafe
    void setMetaTreeInternal(MetaTreeImpl metaTree) {
        this.metaTree = metaTree;
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
    @SuppressWarnings({"AssignmentToMethodParameter"})
    @NotNull
    StoreImpl openStoreImpl(@NotNull final String name,
                            @NotNull StoreConfig config,
                            @NotNull final TransactionBase txn,
                            @Nullable TreeMetaInfo metaInfo) {
        checkIfTransactionCreatedAgainstThis(txn);
        if (config.useExisting) { // this parameter requires to recalculate
            if (metaInfo == null) {
                throw new ExodusException("Can't restore meta information for store " + name);
            } else {
                config = TreeMetaInfo.toConfig(metaInfo);
            }
        }
        final StoreImpl result;
        if (metaInfo == null) {
            if (txn.isReadonly() && ec.getEnvReadonlyEmptyStores()) {
                return createTemporaryEmptyStore(name);
            }
            final int structureId = allocateStructureId();
            metaInfo = TreeMetaInfo.load(this, config.duplicates, config.prefixing, structureId);
            result = createStore(name, metaInfo);
            final ReadWriteTransaction tx = throwIfReadonly(txn, "Can't create a store in read-only transaction");
            tx.getMutableTree(result);
            tx.storeCreated(result);
        } else {
            final boolean hasDuplicates = metaInfo.hasDuplicates();
            if (hasDuplicates != config.duplicates) {
                throw new ExodusException("Attempt to open store '" + name + "' with duplicates = " +
                        config.duplicates + " while it was created with duplicates =" + hasDuplicates);
            }
            if (metaInfo.isKeyPrefixing() != config.prefixing) {
                if (!config.prefixing) {
                    throw new ExodusException("Attempt to open store '" + name +
                            "' with prefixing = false while it was created with prefixing = true");
                }
                // if we're trying to open existing store with prefixing which actually wasn't created as store
                // with prefixing due to lack of the PatriciaTree feature, then open store with existing config
                metaInfo = TreeMetaInfo.load(this, hasDuplicates, false, metaInfo.getStructureId());
            }
            result = createStore(name, metaInfo);
            // XD-774: if the store was just removed in the same txn forget the removal
            if (txn instanceof ReadWriteTransaction) {
                ((ReadWriteTransaction) txn).storeOpened(result);
            }
        }
        return result;
    }

    int getLastStructureId() {
        return structureId.get();
    }

    void registerTransaction(@NotNull final TransactionBase txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        // N.B! due to TransactionImpl.revert(), there can appear a txn which is already in the transaction set
        // any implementation of transaction set should process this well
        txns.add(txn);
    }

    boolean isRegistered(@NotNull final ReadWriteTransaction txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        return txns.contains(txn);
    }

    int activeTransactions() {
        return txns.size();
    }

    void runTransactionSafeTasks() {
        if (throwableOnCommit == null) {
            List<Runnable> tasksToRun = null;
            final long oldestTxnRoot = txns.getOldestTxnRootAddress();
            synchronized (txnSafeTasks) {
                while (true) {
                    if (!txnSafeTasks.isEmpty()) {
                        final RunnableWithTxnRoot r = txnSafeTasks.getFirst();
                        if (r.txnRoot < oldestTxnRoot) {
                            txnSafeTasks.removeFirst();
                            if (tasksToRun == null) {
                                tasksToRun = new ArrayList<>(4);
                            }
                            tasksToRun.add(r.runnable);
                            continue;
                        }
                    }
                    break;
                }
            }
            if (tasksToRun != null) {
                for (final Runnable task : tasksToRun) {
                    task.run();
                }
            }
        }
    }

    void forEachActiveTransaction(@NotNull final TransactionalExecutable executable) {
        txns.forEach(executable);
    }

    protected StoreImpl createTemporaryEmptyStore(String name) {
        return new TemporaryEmptyStore(this, name);
    }

    static boolean isUtilizationProfile(@NotNull final String storeName) {
        return GarbageCollector.isUtilizationProfile(storeName);
    }

    static ReadWriteTransaction throwIfReadonly(@NotNull final Transaction txn, @NotNull final String exceptionMessage) {
        if (txn.isReadonly()) {
            throw new ReadonlyTransactionException(exceptionMessage);
        }
        return (ReadWriteTransaction) txn;
    }

    static void loggerError(@NotNull final String errorMessage) {
        loggerError(errorMessage, null);
    }

    static void loggerError(@NotNull final String errorMessage, @Nullable final Throwable t) {
        if (t == null) {
            logger.error(errorMessage);
        } else {
            logger.error(errorMessage, t);
        }
    }

    static void loggerInfo(@NotNull final String message) {
        if (logger.isInfoEnabled()) {
            logger.info(message);
        }
    }

    static void loggerDebug(@NotNull final String message) {
        loggerDebug(message, null);
    }

    @SuppressWarnings("SameParameterValue")
    static void loggerDebug(@NotNull final String message, @Nullable final Throwable t) {
        if (logger.isDebugEnabled()) {
            if (t == null) {
                logger.debug(message);
            } else {
                logger.debug(message, t);
            }
        }
    }

    private void runAllTransactionSafeTasks() {
        if (throwableOnCommit == null) {
            synchronized (txnSafeTasks) {
                for (final RunnableWithTxnRoot r : txnSafeTasks) {
                    r.runnable.run();
                }
            }
            DeferredIO.getJobProcessor().waitForJobs(100);
        }
    }

    private void checkIfTransactionCreatedAgainstThis(@NotNull final Transaction txn) {
        if (txn.getEnvironment() != this) {
            throw new ExodusException("Transaction is created against another Environment");
        }
    }

    private void checkInactive(boolean exceptionSafe) {
        int txnCount = txns.size();
        if (!exceptionSafe && txnCount > 0) {
            SharedTimer.ensureIdle();
            txnCount = txns.size();
        }
        if (txnCount > 0) {
            final String errorString = "Environment[" + getLocation() + "] is active: " + txnCount + " transaction(s) not finished";
            if (!exceptionSafe) {
                loggerError(errorString);
            } else {
                loggerInfo(errorString);
            }
            if (!exceptionSafe) {
                reportAliveTransactions(false);
            } else if (logger.isDebugEnabled()) {
                reportAliveTransactions(true);
            }
        }
        if (!exceptionSafe) {
            if (txnCount > 0) {
                throw new ExodusException("Finish all transactions before closing database environment");
            }
        }
    }

    private void reportAliveTransactions(final boolean debug) {
        if (transactionTimeout() == 0) {
            String stacksUnavailable = "Transactions stack traces are not available, " +
                    "set '" + EnvironmentConfig.ENV_MONITOR_TXNS_TIMEOUT + " > 0'";
            if (debug) {
                loggerDebug(stacksUnavailable);
            } else {
                loggerError(stacksUnavailable);
            }
        } else {
            forEachActiveTransaction(txn -> {
                final StackTrace trace = ((TransactionBase) txn).getTrace();
                if (debug) {
                    loggerDebug("Alive transaction:\n" + trace);
                } else {
                    loggerError("Alive transaction:\n" + trace);
                }
            });
        }
    }

    private void checkIsOperative() {
        final Throwable t = throwableOnCommit;
        if (t != null) {
            if (t instanceof EnvironmentClosedException) {
                throw new ExodusException("Environment is inoperative", t);
            }
            throw ExodusException.toExodusException(t, "Environment is inoperative");
        }
    }

    private int allocateStructureId() {
        /*
         * <TRICK>
         * Allocates structure id so that 256 doesn't factor it. This ensures that corresponding byte iterable
         * will never end with zero byte, and any such id can be used as a key in meta tree without collision
         * with a string key (store name). String keys (according to StringBinding) do always end with zero byte.
         * </TRICK>
         */
        while (true) {
            final int result = structureId.incrementAndGet();
            if ((result & 0xff) != 0) {
                return result;
            }
        }
    }

    private void invalidateStoreGetCache() {
        final int storeGetCacheSize = ec.getEnvStoreGetCacheSize();
        storeGetCache = storeGetCacheSize == 0 ? null :
                new StoreGetCache(storeGetCacheSize, ec.getEnvStoreGetCacheMinTreeSize(), ec.getEnvStoreGetCacheMaxValueSize());
    }

    private void updateTxnProfiler(TransactionBase txn, long initialHighAddress, long resultingHighAddress) {
        if (txnProfiler != null) {
            final long writtenBytes = resultingHighAddress - initialHighAddress;
            if (txn.isGCTransaction()) {
                txnProfiler.incGcTransaction();
                txnProfiler.addGcMovedBytes(writtenBytes);
            } else if (txn.isReadonly()) {
                txnProfiler.addReadonlyTxn(txn);
            } else {
                txnProfiler.addTxn(txn, writtenBytes);
            }
        }
    }

    private static void applyEnvironmentSettings(@NotNull final String location,
                                                 @NotNull final EnvironmentConfig ec) {
        final File propsFile = new File(location, ENVIRONMENT_PROPERTIES_FILE);
        if (propsFile.exists() && propsFile.isFile()) {
            try {
                try (InputStream propsStream = new FileInputStream(propsFile)) {
                    final Properties envProps = new Properties();
                    envProps.load(propsStream);
                    for (final Map.Entry<Object, Object> entry : envProps.entrySet()) {
                        ec.setSetting(entry.getKey().toString(), entry.getValue());
                    }
                }
            } catch (IOException e) {
                throw ExodusException.toExodusException(e);
            }
        }
    }

    private static void checkStorageType(@NotNull final String location, @NotNull final EnvironmentConfig ec) {
        var provider = ec.getLogDataReaderWriterProvider();
        if (provider.equals(DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER)) {
            final File databaseDir = new File(location);
            if (!ec.isLogAllowRemovable() && IOUtil.isRemovableFile(databaseDir)) {
                throw new StorageTypeNotAllowedException("Database on removable storage is not allowed");
            }
            if (!ec.isLogAllowRemote() && IOUtil.isRemoteFile(databaseDir)) {
                throw new StorageTypeNotAllowedException("Database on remote storage is not allowed");
            }
            if (!ec.isLogAllowRamDisk() && IOUtil.isRamDiskFile(databaseDir)) {
                throw new StorageTypeNotAllowedException("Database on RAM disk is not allowed");
            }
        }
    }

    @Nullable
    private static jetbrains.exodus.env.management.EnvironmentConfig createConfigMBean(@NotNull final EnvironmentImpl e) {
        try {
            return e.ec.getManagementOperationsRestricted() ?
                    new jetbrains.exodus.env.management.EnvironmentConfig(e) :
                    new EnvironmentConfigWithOperations(e);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof InstanceAlreadyExistsException) {
                return null;
            }
            throw ex;
        }
    }

    private static void executeInTransaction(@NotNull final TransactionalExecutable executable,
                                             @NotNull final Transaction txn) {
        try {
            while (true) {
                executable.execute(txn);
                if (txn.isReadonly() || // txn can be read-only if Environment is in read-only mode
                        txn.isFinished() || // txn can be finished if, e.g., it was aborted within executable
                        txn.flush()) {
                    break;
                }
                txn.revert();
            }
        } finally {
            abortIfNotFinished(txn);
        }
    }

    private static <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable,
                                              @NotNull final Transaction txn) {
        try {
            while (true) {
                final T result = computable.compute(txn);
                if (txn.isReadonly() || // txn can be read-only if Environment is in read-only mode
                        txn.isFinished() || // txn can be finished if, e.g., it was aborted within computable
                        txn.flush()) {
                    return result;
                }
                txn.revert();
            }
        } finally {
            abortIfNotFinished(txn);
        }
    }

    private static void abortIfNotFinished(@NotNull final Transaction txn) {
        if (!txn.isFinished()) {
            txn.abort();
        }
    }

    private class EnvironmentSettingsListener implements ConfigSettingChangeListener {

        @Override
        public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
            if (key.equals(EnvironmentConfig.ENV_IS_READONLY)) {
                if (log.getConfig().isReadonlyReaderWriterProvider()) {
                    throw new InvalidSettingException("Can't modify read-only state in run time since DataReaderWriterProvider is read-only");
                }
                if (Boolean.TRUE.equals(value)) {
                    suspendGC();
                    final TransactionBase txn = beginTransaction();
                    try {
                        if (!txn.isReadonly()) {
                            gc.getUtilizationProfile().forceSave(txn);
                            txn.setCommitHook(() -> {
                                EnvironmentConfig.suppressConfigChangeListenersForThread();
                                ec.setEnvIsReadonly(true);
                                EnvironmentConfig.resumeConfigChangeListenersForThread();
                            });
                            ((ReadWriteTransaction) txn).forceFlush();
                        }
                    } finally {
                        txn.abort();
                    }
                    if (!log.isReadOnly()) {
                        log.updateStartUpDbRoot(metaTree.root);
                        flushAndSync();
                    }
                }
            }
        }

        @Override
        public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
            if (key.equals(EnvironmentConfig.ENV_STOREGET_CACHE_SIZE) ||
                    key.equals(EnvironmentConfig.ENV_STOREGET_CACHE_MIN_TREE_SIZE) ||
                    key.equals(EnvironmentConfig.ENV_STOREGET_CACHE_MAX_VALUE_SIZE)) {
                invalidateStoreGetCache();
            } else if (key.equals(EnvironmentConfig.LOG_SYNC_PERIOD)) {
                log.getConfig().setSyncPeriod(ec.getLogSyncPeriod());
            } else if (key.equals(EnvironmentConfig.LOG_DURABLE_WRITE)) {
                log.getConfig().setDurableWrite(ec.getLogDurableWrite());
            } else if (key.equals(EnvironmentConfig.ENV_IS_READONLY) && !isReadOnly()) {
                resumeGC();
            } else if (key.equals(EnvironmentConfig.GC_UTILIZATION_FROM_SCRATCH) && ec.getGcUtilizationFromScratch()) {
                gc.getUtilizationProfile().computeUtilizationFromScratch();
            } else if (key.equals(EnvironmentConfig.GC_UTILIZATION_FROM_FILE)) {
                gc.getUtilizationProfile().loadUtilizationFromFile((String) value);
            } else if (key.equals(EnvironmentConfig.TREE_MAX_PAGE_SIZE)) {
                balancePolicy = null;
            } else if (key.equals(EnvironmentConfig.TREE_DUP_MAX_PAGE_SIZE)) {
                balancePolicy = null;
            } else if (key.equals(EnvironmentConfig.LOG_CACHE_READ_AHEAD_MULTIPLE)) {
                log.getConfig().setCacheReadAheadMultiple(ec.getLogCacheReadAheadMultiple());
            }
        }
    }

    private static class RunnableWithTxnRoot {

        private final Runnable runnable;
        private final long txnRoot;

        private RunnableWithTxnRoot(Runnable runnable, long txnRoot) {
            this.runnable = runnable;
            this.txnRoot = txnRoot;
        }
    }

    private static final class SyncIO {
        private static volatile ScheduledExecutorService syncExecutor;
        private static final ReentrantLock lock = new ReentrantLock();

        private static ScheduledExecutorService getSyncExecutor() {
            if (syncExecutor == null) {
                lock.lock();
                try {
                    if (syncExecutor == null) {
                        syncExecutor = Executors.newScheduledThreadPool(1, new SyncIOThreadFactory());
                    }
                } finally {
                    lock.unlock();
                }
            }

            return syncExecutor;
        }

        public static ScheduledFuture<?> scheduleSyncLoop(final EnvironmentImpl environment) {
            var executor = getSyncExecutor();
            var syncPeriod = environment.ec.getLogSyncPeriod();

            return executor.scheduleWithFixedDelay(() -> {
                try {
                    if (environment.log.needsToBeSynchronized()) {
                        synchronized (environment.commitLock) {
                            if (environment.isOpen()) {
                                if (environment.log.needsToBeSynchronized()) {
                                    environment.flushAndSync();
                                }
                            } else if (environment.syncTask != null) {
                                environment.syncTask.cancel(false);
                            }
                        }
                    }
                } catch (final Throwable t) {
                    logger.error("Error during synchronization of content of log " +
                            environment.log.getLocation(), t);
                    environment.throwableOnCommit = t;
                }
            }, syncPeriod, Math.min(syncPeriod / 10, 1_000), TimeUnit.MILLISECONDS);
        }
    }

    private static final class SyncIOThreadFactory implements ThreadFactory {
        private static final AtomicLong idGen = new AtomicLong();

        @Override
        public Thread newThread(@NotNull Runnable r) {
            var thread = new Thread(r);
            thread.setName("Scheduled Xodus data sync thread #" + idGen.getAndIncrement());
            thread.setUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception in thread" + t, e));
            thread.setDaemon(true);
            return thread;
        }
    }
}
