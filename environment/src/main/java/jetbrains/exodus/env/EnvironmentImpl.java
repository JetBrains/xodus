/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.AbstractConfig;
import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache;
import jetbrains.exodus.core.dataStructures.LongObjectCacheBase;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.gc.GarbageCollector;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.Loggable;
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
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class EnvironmentImpl implements Environment {

    public static final int META_TREE_ID = 1;

    private static final Logger logger = LoggerFactory.getLogger(EnvironmentImpl.class);

    private static final String ENVIRONMENT_PROPERTIES_FILE = "exodus.properties";

    @NotNull
    private final Log log;
    @NotNull
    private final EnvironmentConfig ec;
    private BTreeBalancePolicy balancePolicy;
    private MetaTree metaTree;
    private final AtomicInteger structureId;
    @NotNull
    private final TransactionSet txns;
    private final LinkedList<RunnableWithTxnRoot> txnSafeTasks;
    @Nullable
    private StoreGetCache storeGetCache;
    @Nullable
    private SoftReference<LongObjectCacheBase> treeNodesCache;
    private final EnvironmentSettingsListener envSettingsListener;
    private final GarbageCollector gc;
    private final Object commitLock = new Object();
    private final Object metaLock = new Object();
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
        invalidateTreeNodesCache();
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

        if (transactionTimeout() > 0) {
            new StuckTransactionMonitor(this);
        }

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
        return new ReadonlyTransaction(this, beginHook);
    }

    @NotNull
    public TransactionImpl beginGCTransaction() {
        return throwIfReadonly(beginTransaction(null, ec.getGcUseExclusiveTransaction(), true), "Can't start GC transaction on read-only Environment");
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
            txn.abort();
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
            txn.abort();
        }
    }

    @Override
    public void executeTransactionSafeTask(@NotNull final Runnable task) {
        final long newestTxnRoot = getNewestTxnRootAddress();
        if (newestTxnRoot == Long.MIN_VALUE) {
            task.run();
        } else {
            synchronized (txnSafeTasks) {
                txnSafeTasks.addLast(new RunnableWithTxnRoot(task, newestTxnRoot));
            }
        }
    }

    @Override
    public void clear() {
        suspendGC();
        try {
            final Thread currentThread = Thread.currentThread();
            final int permits = txnDispatcher.acquireTransaction(currentThread, true);// wait for and stop all writing transactions
            try {
                final int roPermits = roTxnDispatcher.acquireTransaction(currentThread, true);// wait for and stop all read-only transactions
                try {
                    synchronized (commitLock) {
                        synchronized (metaLock) {
                            log.clear();
                            runAllTransactionSafeTasks();
                            txnSafeTasks.clear();
                            throwableOnCommit = null;
                            final Pair<MetaTree, Integer> meta = MetaTree.create(this);
                            metaTree = meta.getFirst();
                            structureId.set(meta.getSecond());
                        }
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
        if (configMBean != null) {
            configMBean.unregister();
        }
        if (statisticsMBean != null) {
            statisticsMBean.unregister();
        }
        // in order to avoid deadlock, do not finish gc inside lock
        // it is safe to invoke gc.finish() several times
        gc.finish();
        final double logCacheHitRate;
        final double storeGetCacheHitRate;
        final double treeNodesCacheHitRate;
        synchronized (commitLock) {
            if (!isOpen()) {
                throw new IllegalStateException("Already closed, see cause for previous close stack trace", throwableOnClose);
            }
            checkInactive(ec.getEnvCloseForcedly());
            try {
                if (!ec.getEnvIsReadonly()) {
                    gc.saveUtilizationProfile();
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
            final LongObjectCacheBase treeNodesCache = this.treeNodesCache == null ? null : this.treeNodesCache.get();
            if (treeNodesCache == null) {
                treeNodesCacheHitRate = 0;
            } else {
                treeNodesCacheHitRate = treeNodesCache.hitRate();
                treeNodesCache.close();
            }
            throwableOnClose = new Throwable();
            throwableOnCommit = EnvironmentClosedException.INSTANCE;
        }
        runAllTransactionSafeTasks();
        if (logger.isInfoEnabled()) {
            logger.info("Store get cache hit rate: " + ObjectCacheBase.formatHitRate(storeGetCacheHitRate));
            logger.info("Tree nodes cache hit rate: " + ObjectCacheBase.formatHitRate(treeNodesCacheHitRate));
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
    public void truncateStore(@NotNull final String storeName, @NotNull final Transaction transaction) {
        final TransactionImpl txn = throwIfReadonly(transaction, "Can't truncate a store in read-only transaction");
        StoreImpl store = openStore(storeName, StoreConfig.USE_EXISTING, txn, false);
        if (store == null) {
            throw new ExodusException("Attempt to truncate unknown store '" + storeName + '\'');
        }
        txn.storeRemoved(store);
        final TreeMetaInfo metaInfoCloned = store.getMetaInfo().clone(allocateStructureId());
        store = new StoreImpl(this, storeName, metaInfoCloned);
        txn.storeCreated(store);
    }

    @Override
    public void removeStore(@NotNull final String storeName, @NotNull final Transaction transaction) {
        final TransactionImpl txn = throwIfReadonly(transaction, "Can't remove a store in read-only transaction");
        final StoreImpl store = openStore(storeName, StoreConfig.USE_EXISTING, txn, false);
        if (store == null) {
            throw new ExodusException("Attempt to remove unknown store '" + storeName + '\'');
        }
        txn.storeRemoved(store);
    }

    @Override
    @NotNull
    public List<String> getAllStoreNames(@NotNull final Transaction transaction) {
        return ((TransactionBase) transaction).getAllStoreNames();
    }

    public boolean storeExists(@NotNull final String storeName, @NotNull final Transaction transaction) {
        return ((TransactionBase) transaction).getTreeMetaInfo(storeName) != null;
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
    protected TransactionBase beginTransaction(Runnable beginHook, boolean exclusive, boolean cloneMeta) {
        checkIsOperative();
        return ec.getEnvIsReadonly() ?
                new ReadonlyTransaction(this, beginHook) :
                new TransactionImpl(this, beginHook, exclusive, cloneMeta);
    }

    long getDiskUsage() {
        return IOUtil.getDirectorySize(new File(getLocation()), LogUtil.LOG_FILE_EXTENSION, false);
    }

    void acquireTransaction(@NotNull final TransactionBase txn) {
        final ReentrantTransactionDispatcher dispatcher = txn.isReadonly() ? roTxnDispatcher : txnDispatcher;
        dispatcher.acquireTransaction(txn);
    }

    void releaseTransaction(@NotNull final TransactionBase txn) {
        final ReentrantTransactionDispatcher dispatcher = txn.isReadonly() ? roTxnDispatcher : txnDispatcher;
        dispatcher.releaseTransaction(txn);
    }

    boolean shouldTransactionBeExclusive(@NotNull final TransactionImpl txn) {
        final int replayCount = txn.getReplayCount();
        return replayCount > 0 &&
                (ec.getEnvTxnReplayMaxCount() == replayCount ||
                        System.currentTimeMillis() - txn.getCreated() >= ec.getEnvTxnReplayTimeout());
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
        return new BTree(log, getBTreeBalancePolicy(), rootAddress, false, META_TREE_ID);
    }

    /**
     * Flushes Log's data writer exclusively in commit lock. This guarantees that the data writer is in committed state.
     */
    void safeFlush() {
        synchronized (commitLock) {
            getLog().flush(true);
        }
    }

    @SuppressWarnings("OverlyNestedMethod")
    boolean commitTransaction(@NotNull final TransactionImpl txn, final boolean forceCommit) {
        if (flushTransaction(txn, forceCommit)) {
            finishTransaction(txn);
            return true;
        }
        return false;
    }

    boolean flushTransaction(@NotNull final TransactionImpl txn, final boolean forceCommit) {
        if (!forceCommit && txn.isIdempotent()) {
            return true;
        }
        final Iterable<Loggable>[] expiredLoggables;
        final long initialHighAddress;
        final long resultingHighAddress;
        synchronized (commitLock) {
            if (ec.getEnvIsReadonly()) {
                throw new ReadonlyTransactionException();
            }
            checkIsOperative();
            if (!txn.checkVersion(metaTree.root)) {
                // meta lock not needed 'cause write can only occur in another commit lock
                return false;
            }
            initialHighAddress = log.getHighAddress();
            try {
                final MetaTree[] tree = new MetaTree[1];
                expiredLoggables = txn.doCommit(tree);
                synchronized (metaLock) {
                    txn.setMetaTree(metaTree = tree[0]);
                    txn.executeCommitHook();
                }
                resultingHighAddress = log.getHighAddress();
            } catch (Throwable t) { // pokemon exception handling to decrease try/catch block overhead
                logger.error("Failed to flush transaction", t);
                try {
                    log.setHighAddress(initialHighAddress);
                } catch (Throwable th) {
                    throwableOnCommit = t; // inoperative on failing to update high address too
                    logger.error("Failed to rollback high address", th);
                    throw ExodusException.toExodusException(th, "Failed to rollback high address");
                }
                throw ExodusException.toExodusException(t, "Failed to flush transaction");
            }
        }
        gc.fetchExpiredLoggables(new ExpiredLoggableIterable(expiredLoggables));

        // update statistics
        statistics.getStatisticsItem(EnvironmentStatistics.BYTES_WRITTEN).setTotal(resultingHighAddress);
        if (gc.isCleanerThread()) {
            statistics.getStatisticsItem(EnvironmentStatistics.BYTES_MOVED_BY_GC).addTotal(resultingHighAddress - initialHighAddress);
        }
        statistics.getStatisticsItem(EnvironmentStatistics.FLUSHED_TRANSACTIONS).incTotal();

        return true;
    }

    MetaTree holdNewestSnapshotBy(@NotNull final TransactionBase txn) {
        acquireTransaction(txn);
        final Runnable beginHook = txn.getBeginHook();
        synchronized (metaLock) {
            if (beginHook != null) {
                beginHook.run();
            }
            return metaTree;
        }
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
            final TransactionImpl tx = throwIfReadonly(txn, "Can't create a store in read-only transaction");
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
        // N.B! due to TransactionImpl.revert(), there can appear a txn which is already in the transaction set
        // any implementation of transaction set should process this well
        txns.add(txn);
    }

    boolean isRegistered(@NotNull final TransactionImpl txn) {
        return txns.contains(txn);
    }

    int activeTransactions() {
        return txns.size();
    }

    void runTransactionSafeTasks() {
        if (throwableOnCommit == null) {
            List<Runnable> tasksToRun = null;
            final long oldestTxnRoot = getOldestTxnRootAddress();
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
    TransactionBase getOldestTransaction() {
        return txns.getOldestTransaction();
    }

    @Nullable
    TransactionBase getNewestTransaction() {
        return txns.getNewestTransaction();
    }

    @Nullable
    StoreGetCache getStoreGetCache() {
        return storeGetCache;
    }

    @Nullable
    LongObjectCacheBase getTreeNodesCache() {
        final SoftReference<LongObjectCacheBase> cacheRef = treeNodesCache;
        if (cacheRef != null) {
            final LongObjectCacheBase cache = cacheRef.get();
            return cache != null ? cache : invalidateTreeNodesCache();
        }
        return null;
    }

    void forEachActiveTransaction(@NotNull final TransactionalExecutable executable) {
        for (final Transaction txn : txns) {
            executable.execute(txn);
        }
    }

    protected StoreImpl createTemporaryEmptyStore(String name) {
        return new TemporaryEmptyStore(this, name);
    }

    static boolean isUtilizationProfile(@NotNull final String storeName) {
        return GarbageCollector.isUtilizationProfile(storeName);
    }

    static TransactionImpl throwIfReadonly(@NotNull final Transaction txn, @NotNull final String exceptionMessage) {
        if (txn.isReadonly()) {
            throw new ReadonlyTransactionException(exceptionMessage);
        }
        return (TransactionImpl) txn;
    }

    private long getOldestTxnRootAddress() {
        final TransactionBase oldestTxn = getOldestTransaction();
        return oldestTxn == null ? Long.MAX_VALUE : oldestTxn.getRoot();
    }

    private long getNewestTxnRootAddress() {
        final TransactionBase newestTxn = getNewestTransaction();
        return newestTxn == null ? Long.MIN_VALUE : newestTxn.getRoot();
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

    private void checkInactive(boolean exceptionSafe) {
        final int txnCount = txns.size();
        if (txnCount > 0) {
            final String errorString = "Environment[" + getLocation() + "] is active: " + txnCount + " transaction(s) not finished";
            if (!exceptionSafe) {
                logger.error(errorString);
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
                logger.error(stacksUnavailable);
            }
        } else {
            forEachActiveTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    final Throwable trace = ((TransactionBase) txn).getTrace();
                    if (debug) {
                        logger.debug("Alive transaction: ", trace);
                    } else {
                        logger.error("Alive transaction: ", trace);
                    }
                }
            });
        }
    }

    private void checkIsOperative() {
        final Throwable t = throwableOnCommit;
        if (t != null) {
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

    private LongObjectCacheBase invalidateTreeNodesCache() {
        final int treeNodesCacheSize = ec.getTreeNodesCacheSize();
        final LongObjectCacheBase result = treeNodesCacheSize == 0 ? null :
                new ConcurrentLongObjectCache(treeNodesCacheSize, 2);
        treeNodesCache = result == null ? null : new SoftReference<>(result);
        return result;
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
                // txn can be read-only if Environment is in read-only mode
                if (txn.isReadonly() || txn.flush()) {
                    break;
                }
                txn.revert();
            }
        } finally {
            txn.abort();
        }
    }

    private static <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable,
                                              @NotNull final Transaction txn) {
        try {
            while (true) {
                final T result = computable.compute(txn);
                // txn can be read-only if Environment is in read-only mode
                if (txn.isReadonly() || txn.flush()) {
                    return result;
                }
                txn.revert();
            }
        } finally {
            txn.abort();
        }
    }

    private class EnvironmentSettingsListener implements AbstractConfig.ChangedSettingsListener {

        @Override
        public void settingChanged(@NotNull final String settingName) {
            if (settingName.equals(EnvironmentConfig.ENV_STOREGET_CACHE_SIZE)) {
                invalidateStoreGetCache();
            } else if (settingName.equals(EnvironmentConfig.TREE_NODES_CACHE_SIZE)) {
                invalidateTreeNodesCache();
            } else if (settingName.equals(EnvironmentConfig.LOG_SYNC_PERIOD)) {
                log.getConfig().setSyncPeriod(ec.getLogSyncPeriod());
            } else if (settingName.equals(EnvironmentConfig.LOG_DURABLE_WRITE)) {
                log.getConfig().setDurableWrite(ec.getLogDurableWrite());
            } else if (settingName.equals(EnvironmentConfig.ENV_IS_READONLY)) {
                if (ec.getEnvIsReadonly()) {
                    suspendGC();
                } else {
                    resumeGC();
                }
            }
        }
    }

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    private static class ExpiredLoggableIterable implements Iterable<Loggable> {

        private final Iterable<Loggable>[] expiredLoggables;

        private ExpiredLoggableIterable(Iterable<Loggable>[] expiredLoggables) {
            this.expiredLoggables = expiredLoggables;
        }

        @Override
        public Iterator<Loggable> iterator() {

            return new Iterator<Loggable>() {
                private Iterator<Loggable> current = expiredLoggables[0].iterator();
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
                public Loggable next() {
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