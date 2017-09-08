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
package jetbrains.exodus.env;

import jetbrains.exodus.ConfigSettingChangeListener;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.gc.GarbageCollector;
import jetbrains.exodus.gc.UtilizationProfile;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.TreeMetaInfo;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.*;

public class EnvironmentImpl implements Environment {

    public static final int META_TREE_ID = 1;

    private static final Logger logger = LoggerFactory.getLogger(EnvironmentImpl.class);

    private static final String ENVIRONMENT_PROPERTIES_FILE = "exodus.properties";

    @NotNull
    private final Log log;
    @NotNull
    private final EnvironmentConfig ec;
    private BTreeBalancePolicy balancePolicy;
    private volatile MetaTree metaTree;
    private final AtomicInteger structureId;
    @NotNull
    private final TransactionSet txns;
    private final LinkedList<RunnableWithTxnRoot> txnSafeTasks;
    @Nullable
    private StoreGetCache storeGetCache;
    private final EnvironmentSettingsListener envSettingsListener;
    private final GarbageCollector gc;
    private final Object commitLock = new Object();
    private final ReentrantTransactionDispatcher txnDispatcher;
    private final ReentrantTransactionDispatcher roTxnDispatcher;
    @NotNull
    private final EnvironmentStatistics statistics;
    @Nullable
    private final jetbrains.exodus.env.management.EnvironmentConfig configMBean;
    @Nullable
    private final jetbrains.exodus.env.management.EnvironmentStatistics statisticsMBean;

    /**
     * Throwable caught during commit after which rollback of highAddress failed.
     * Generally, it should ne null, otherwise environment is inoperative:
     * no transaction can be started or committed in that state. Once environment became inoperative,
     * it will remain inoperative forever.
     */
    private volatile Throwable throwableOnCommit;
    private Throwable throwableOnClose;

    @Nullable
    private final StuckTransactionMonitor stuckTxnMonitor;

    @SuppressWarnings({"ThisEscapedInObjectConstruction"})
    EnvironmentImpl(@NotNull final Log log, @NotNull final EnvironmentConfig ec) {
        this.log = log;
        this.ec = ec;
        applyEnvironmentSettings(log.getLocation(), ec);
        final Pair<MetaTree, Integer> meta = MetaTree.create(this);
        metaTree = meta.getFirst();
        structureId = new AtomicInteger(meta.getSecond());
        txns = new TransactionSet();
        txnSafeTasks = new LinkedList<>();
        invalidateStoreGetCache();
        envSettingsListener = new EnvironmentSettingsListener();
        ec.addChangedSettingsListener(envSettingsListener);

        gc = new GarbageCollector(this);

        txnDispatcher = new ReentrantTransactionDispatcher(ec.getEnvMaxParallelTxns());
        roTxnDispatcher = new ReentrantTransactionDispatcher(ec.getEnvMaxParallelReadonlyTxns());

        statistics = new EnvironmentStatistics(this);
        if (ec.isManagementEnabled()) {
            configMBean = new jetbrains.exodus.env.management.EnvironmentConfig(this);
            // if we don't gather statistics then we should not expose corresponding managed bean
            statisticsMBean = ec.getEnvGatherStatistics() ? new jetbrains.exodus.env.management.EnvironmentStatistics(this) : null;
        } else {
            configMBean = null;
            statisticsMBean = null;
        }

        throwableOnCommit = null;
        throwableOnClose = null;

        stuckTxnMonitor = (transactionTimeout() > 0) ? new StuckTransactionMonitor(this) : null;

        if (logger.isInfoEnabled()) {
            logger.info("Exodus environment created: " + log.getLocation());
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
        return beginTransaction(null, false);
    }

    @Override
    @NotNull
    public TransactionBase beginTransaction(final Runnable beginHook) {
        return beginTransaction(beginHook, false);
    }

    @NotNull
    @Override
    public Transaction beginExclusiveTransaction() {
        return beginTransaction(null, true);
    }

    @NotNull
    @Override
    public Transaction beginExclusiveTransaction(Runnable beginHook) {
        return beginTransaction(beginHook, true);
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
        return new ReadonlyTransaction(this, beginHook);
    }

    @NotNull
    public ReadWriteTransaction beginGCTransaction() {
        if (ec.getEnvIsReadonly()) {
            throw new ReadonlyTransactionException("Can't start GC transaction on read-only Environment");
        }
        return new ReadWriteTransaction(this, metaTree.getClone(), null, ec.getGcUseExclusiveTransaction()) {

            @Override
            boolean isGCTransaction() {
                return true;
            }
        };
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

    @Nullable
    public String getStuckTransactionMonitorMessage() {
        return stuckTxnMonitor == null ? null : stuckTxnMonitor.getErrorMessage();
    }

    @Override
    public void clear() {
        final Thread currentThread = Thread.currentThread();
        if (txnDispatcher.getThreadPermits(currentThread) != 0 || roTxnDispatcher.getThreadPermits(currentThread) != 0) {
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
                final int roPermits = roTxnDispatcher.acquireExclusiveTransaction(currentThread);// wait for and stop all read-only transactions
                try {
                    synchronized (commitLock) {
                        gc.clear();
                        log.clear();
                        invalidateStoreGetCache();
                        throwableOnCommit = null;
                        final Pair<MetaTree, Integer> meta = MetaTree.create(this);
                        metaTree = meta.getFirst();
                        structureId.set(meta.getSecond());
                    }
                } finally {
                    roTxnDispatcher.releaseTransaction(currentThread, roPermits);
                }
            } finally {
                txnDispatcher.releaseTransaction(currentThread, permits);
            }
        } finally {
            resumeGC();
        }
    }

    @SuppressWarnings({"AccessToStaticFieldLockedOnInstance"})
    @Override
    public void close() {
        // if this is already closed do nothing
        synchronized (commitLock) {
            if (!isOpen()) {
                return;
            }
        }
        if (configMBean != null) {
            configMBean.unregister();
        }
        if (statisticsMBean != null) {
            statisticsMBean.unregister();
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
            checkInactive(ec.getEnvCloseForcedly());
            try {
                if (!ec.getEnvIsReadonly() && ec.isGcEnabled()) {
                    executeInTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final Transaction txn) {
                            final UtilizationProfile up = gc.getUtilizationProfile();
                            up.setDirty(true);
                            up.save(txn);
                        }
                    });
                }
                ec.removeChangedSettingsListener(envSettingsListener);
                logCacheHitRate = log.getCacheHitRate();
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
            throwableOnClose = new EnvironmentClosedException();
            throwableOnCommit = throwableOnClose;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Store get cache hit rate: " + ObjectCacheBase.formatHitRate(storeGetCacheHitRate));
            logger.info("Exodus log cache hit rate: " + ObjectCacheBase.formatHitRate(logCacheHitRate));
        }
    }

    @Override
    public boolean isOpen() {
        return throwableOnClose == null;
    }

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
        return metaTree.getAllStoreCount();
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
        gc.wake();
    }

    @Override
    public void suspendGC() {
        gc.suspend();
    }

    @Override
    public void resumeGC() {
        gc.resume();
    }

    public BTreeBalancePolicy getBTreeBalancePolicy() {
        // we don't care of possible race condition here
        if (balancePolicy == null) {
            balancePolicy = new BTreeBalancePolicy(ec.getTreeMaxPageSize());
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
                getLog().flush(true);
            }
        }
    }

    protected StoreImpl createStore(@NotNull final String name, @NotNull final TreeMetaInfo metaInfo) {
        return new StoreImpl(this, name, metaInfo);
    }

    protected void finishTransaction(@NotNull final TransactionBase txn) {
        releaseTransaction(txn);
        txns.remove(txn);
        txn.setIsFinished();
        runTransactionSafeTasks();
    }

    @NotNull
    protected TransactionBase beginTransaction(Runnable beginHook, boolean exclusive) {
        checkIsOperative();
        return ec.getEnvIsReadonly() ?
            new ReadonlyTransaction(this, beginHook) :
            new ReadWriteTransaction(this, beginHook, exclusive);
    }

    long getDiskUsage() {
        return IOUtil.getDirectorySize(new File(getLocation()), LogUtil.LOG_FILE_EXTENSION, false);
    }

    void acquireTransaction(@NotNull final TransactionBase txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        (txn.isReadonly() ? roTxnDispatcher : txnDispatcher).acquireTransaction(txn, this);
    }

    void releaseTransaction(@NotNull final TransactionBase txn) {
        checkIfTransactionCreatedAgainstThis(txn);
        (txn.isReadonly() ? roTxnDispatcher : txnDispatcher).releaseTransaction(txn);
    }

    void downgradeTransaction(@NotNull final TransactionBase txn) {
        (txn.isReadonly() ? roTxnDispatcher : txnDispatcher).downgradeTransaction(txn);
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
     * Tries to load meta tree located at specified rootAddress.
     *
     * @param rootAddress tree root address.
     * @return tree instance or null if the address is not valid.
     */
    @Nullable
    BTree loadMetaTree(final long rootAddress) {
        if (rootAddress < 0 || rootAddress >= log.getHighAddress()) return null;
        return new BTree(log, getBTreeBalancePolicy(), rootAddress, false, META_TREE_ID) {
            @NotNull
            @Override
            public DataIterator getDataIterator(long address) {
                return new DataIterator(log, address);
            }
        };
    }

    boolean flushTransaction(@NotNull final ReadWriteTransaction txn, final boolean forceCommit) {
        checkIfTransactionCreatedAgainstThis(txn);

        if (!forceCommit && txn.isIdempotent()) {
            return true;
        }

        final Iterable<ExpiredLoggableInfo>[] expiredLoggables;
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
            if (ec.getEnvIsReadonly()) {
                throw new ReadonlyTransactionException();
            }
            checkIsOperative();
            if (!txn.checkVersion(metaTree.root)) {
                return false;
            }
            if (wasUpSaved) {
                up.setDirty(false);
            }
            final LogConfig config = log.getConfig();
            config.setFsyncSuppressed(isGcTransaction);
            try {
                initialHighAddress = log.getHighAddress();
                try {
                    expiredLoggables = txn.doCommit();
                    // there is a temptation to postpone I/O in order to reduce number of writes to storage device,
                    // but it's quite difficult to resolve all possible inconsistencies afterwards,
                    // so think twice before removing the following line
                    log.flush();
                    metaTree = txn.getMetaTree();
                    txn.executeCommitHook();
                    resultingHighAddress = log.approveHighAddress();
                } catch (Throwable t) { // pokemon exception handling to decrease try/catch block overhead
                    loggerError("Failed to flush transaction", t);
                    try {
                        log.setHighAddress(initialHighAddress);
                        //log.approveHighAddress();
                    } catch (Throwable th) {
                        throwableOnCommit = t; // inoperative on failing to update high address
                        loggerError("Failed to rollback high address", th);
                        throw ExodusException.toExodusException(th, "Failed to rollback high address");
                    }
                    throw ExodusException.toExodusException(t, "Failed to flush transaction");
                }
            } finally {
                config.setFsyncSuppressed(false);
            }
        }
        gc.fetchExpiredLoggables(new ExpiredLoggableIterable(expiredLoggables));

        // update statistics
        statistics.getStatisticsItem(BYTES_WRITTEN).setTotal(resultingHighAddress);
        if (isGcTransaction) {
            statistics.getStatisticsItem(BYTES_MOVED_BY_GC).addTotal(resultingHighAddress - initialHighAddress);
        }
        statistics.getStatisticsItem(FLUSHED_TRANSACTIONS).incTotal();

        return true;
    }

    MetaTree getMetaTree() {
        return metaTree;
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
            if (ec.getEnvIsReadonly() && ec.getEnvReadonlyEmptyStores()) {
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

    @Nullable
    StoreGetCache getStoreGetCache() {
        return storeGetCache;
    }

    void forEachActiveTransaction(@NotNull final TransactionalExecutable executable) {
        txns.forEach(executable);
    }

    void setHighAddress(final long highAddress) {
        synchronized (commitLock) {
            log.setHighAddress(highAddress);
            final Pair<MetaTree, Integer> meta = MetaTree.create(this);
            metaTree = meta.getFirst();
        }
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
        final int txnCount = txns.size();
        if (txnCount > 0) {
            final String errorString = "Environment[" + getLocation() + "] is active: " + txnCount + " transaction(s) not finished";
            if (!exceptionSafe) {
                loggerError(errorString);
            } else if (logger.isInfoEnabled()) {
                logger.info(errorString);
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
                "set \'" + EnvironmentConfig.ENV_MONITOR_TXNS_TIMEOUT + " > 0\'";
            if (debug) {
                logger.debug(stacksUnavailable);
            } else {
                loggerError(stacksUnavailable);
            }
        } else {
            forEachActiveTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    final Throwable trace = ((TransactionBase) txn).getTrace();
                    if (debug) {
                        logger.debug("Alive transaction: ", trace);
                    } else {
                        loggerError("Alive transaction: ", trace);
                    }
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
        /**
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
        storeGetCache = storeGetCacheSize == 0 ? null : new StoreGetCache(storeGetCacheSize);
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
            if (key.equals(EnvironmentConfig.ENV_IS_READONLY) && Boolean.TRUE.equals(value)) {
                suspendGC();
                final TransactionBase txn = beginTransaction();
                try {
                    if (!txn.isReadonly()) {
                        txn.setCommitHook(new Runnable() {
                            @Override
                            public void run() {
                                EnvironmentConfig.suppressConfigChangeListenersForThread();
                                ec.setEnvIsReadonly(true);
                                EnvironmentConfig.resumeConfigChangeListenersForThread();
                            }
                        });
                        ((ReadWriteTransaction) txn).forceFlush();
                    }
                } finally {
                    txn.abort();
                }
            }
        }

        @Override
        public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
            if (key.equals(EnvironmentConfig.ENV_STOREGET_CACHE_SIZE)) {
                invalidateStoreGetCache();
            } else if (key.equals(EnvironmentConfig.LOG_SYNC_PERIOD)) {
                log.getConfig().setSyncPeriod(ec.getLogSyncPeriod());
            } else if (key.equals(EnvironmentConfig.LOG_DURABLE_WRITE)) {
                log.getConfig().setDurableWrite(ec.getLogDurableWrite());
            } else if (key.equals(EnvironmentConfig.ENV_IS_READONLY) && !ec.getEnvIsReadonly()) {
                resumeGC();
            } else if (key.equals(EnvironmentConfig.GC_UTILIZATION_FROM_SCRATCH) && ec.getGcUtilizationFromScratch()) {
                gc.getUtilizationProfile().computeUtilizationFromScratch();
            } else if (key.equals(EnvironmentConfig.GC_UTILIZATION_FROM_FILE)) {
                gc.getUtilizationProfile().loadUtilizationFromFile((String) value);
            }
        }
    }

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    private static class ExpiredLoggableIterable implements Iterable<ExpiredLoggableInfo> {

        private final Iterable<ExpiredLoggableInfo>[] expiredLoggables;

        private ExpiredLoggableIterable(Iterable<ExpiredLoggableInfo>[] expiredLoggables) {
            this.expiredLoggables = expiredLoggables;
        }

        @Override
        public Iterator<ExpiredLoggableInfo> iterator() {

            return new Iterator<ExpiredLoggableInfo>() {
                private Iterator<ExpiredLoggableInfo> current = expiredLoggables[0].iterator();
                private int index = 0;

                @Override
                public boolean hasNext() {
                    //noinspection LoopConditionNotUpdatedInsideLoop
                    while (!current.hasNext()) {
                        if (++index == expiredLoggables.length) {
                            return false;
                        }
                        current = expiredLoggables[index].iterator();
                    }
                    return true;
                }

                @Override
                public ExpiredLoggableInfo next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more loggables available");
                    }
                    return current.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
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
}