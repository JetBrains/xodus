/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.*;
import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Specifies settings of {@linkplain Environment}. Default settings are specified by {@linkplain #DEFAULT} which
 * is immutable. Any newly created {@code EnvironmentConfig} has the same settings as {@linkplain #DEFAULT}.
 *
 * <p>As a rule, the {@code EnvironmentConfig} instance is created along with the {@linkplain Environment} one.
 * E.g., creation of {@linkplain Environment} with some tuned garbage collector settings can look as follows:
 * <pre>
 *     final EnvironmentConfig config = new EnvironmentConfig();
 *     final Environment env = Environments.newInstance(""dbDirectory", config.setGcFileMinAge(3).setGcRunPeriod(60000));
 * </pre>
 *
 * Some setting are mutable at runtime and some are immutable. Immutable at runtime settings can be changed, but they
 * won't take effect on the {@linkplain Environment} instance. Those settings are applicable only during
 * {@linkplain Environment} instance creation.
 *
 * <p>Some settings can be changed only before the database is physically created on storage device. E.g.,
 * {@linkplain #LOG_FILE_SIZE} defines the size of a single {@code Log} file (.xd file) which cannot be changed for
 * existing databases. It makes sense to choose values of such settings at design time and hardcode them.
 *
 * <p>You can define custom processing of changed settings values by
 * {@linkplain #addChangedSettingsListener(ConfigSettingChangeListener)}. Override
 * {@linkplain ConfigSettingChangeListener#beforeSettingChanged(String, Object, Map)} to pre-process mutations of
 * settings and {@linkplain ConfigSettingChangeListener#afterSettingChanged(String, Object, Map)} to post-process them.
 *
 * @see Environment
 * @see Environment#getEnvironmentConfig()
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class EnvironmentConfig extends AbstractConfig {

    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig(ConfigurationStrategy.IGNORE);

    /**
     * Defines absolute value of memory in bytes that can be used by the LogCache. By default, is not set.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE_PERCENTAGE
     */
    public static final String MEMORY_USAGE = "exodus.memoryUsage";

    /**
     * Defines percent of max memory (specified by the "-Xmx" java parameter) that can be used by the LogCache.
     * Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is {@code 50}.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE
     */
    public static final String MEMORY_USAGE_PERCENTAGE = "exodus.memoryUsagePercentage";

    /**
     * If is set to {@code true} forces file system's fsync call after each committed or flushed transaction. By default,
     * is switched off since it creates great performance overhead and can be controlled manually.
     * <p>Mutable at runtime: yes
     */
    public static final String LOG_DURABLE_WRITE = "exodus.log.durableWrite";

    /**
     * Defines the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting cannot be changed
     * for existing databases. Default value is {@code 8192L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_FILE_SIZE = "exodus.log.fileSize";

    /**
     * Defines the number of milliseconds the Log constructor waits for the lock file. Default value is {@code 0L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_LOCK_TIMEOUT = "exodus.log.lockTimeout";

    /**
     * Defines the size in bytes of a single page (byte array) in the LogCache. This number of bytes are read from
     * storage device at a time.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_PAGE_SIZE = "exodus.log.cache.pageSize";

    /**
     * Defines the maximum number of open files that LogCache maintains in order to reduce system calls to open and
     * close files. The more open files is allowed the less system calls are performed in addition to reading files.
     * This value can notably affect performance of database warm-up. Default value is {@code 50}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_OPEN_FILES = "exodus.log.cache.openFilesCount";

    public static final String LOG_CACHE_USE_NIO = "exodus.log.cache.useNIO";

    public static final String LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD = "exodus.log.cache.freePhysicalMemoryThreshold";

    /**
     * If is set to {@code true} the LogCache is shared. Shared cache defined within a class loader caches raw binary
     * data (contents of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
     * By default, the LogCache is shared.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_SHARED = "exodus.log.cache.shared";

    /**
     * If is set to {@code true} the LogCache uses lock-free data structures. Default value is {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_NON_BLOCKING = "exodus.log.cache.nonBlocking";

    /**
     * If is set to {@code true} then the Log constructor fails if the database directory is not clean. Can be useful
     * if an applications expects that the database should always be newly created. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CLEAN_DIRECTORY_EXPECTED = "exodus.log.cleanDirectoryExpected";

    /**
     * If is set to {@code true} then the Log constructor implicitly clears the database is it occurred to be invalid
     * when opened. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CLEAR_INVALID = "exodus.log.clearInvalid";

    /**
     * Sets the period in milliseconds to periodically force file system's fsync call if {@linkplain #LOG_DURABLE_WRITE}
     * is switched off. Default value is {@code 1000L}.
     * <p>Mutable at runtime: yes
     *
     * @see #LOG_DURABLE_WRITE
     */
    public static final String LOG_SYNC_PERIOD = "exodus.log.syncPeriod";

    /**
     * If is set to {@code true} then each complete and immutable {@code Log} file (.xd file) is marked with read-only
     * attribute. Default value is {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_FULL_FILE_READ_ONLY = "exodus.log.fullFileReadonly";

    /**
     * If is set to {@code true} then the {@linkplain Environment} instance is read-only. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENV_IS_READONLY = "exodus.env.isReadonly";

    /**
     * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
     * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a {@linkplain Store},
     * but returns an empty immutable instance instead. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @see #ENV_IS_READONLY
     */
    public static final String ENV_READONLY_EMPTY_STORES = "exodus.env.readonly.emptyStores";

    /**
     * Defines the size of the "store-get" cache. The "store-get" cache can increase performance of
     * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0} what means that
     * the cache is inactive. If the setting is mutated at runtime the cache is invalidated.
     * <p>Mutable at runtime: yes
     */
    public static final String ENV_STOREGET_CACHE_SIZE = "exodus.env.storeGetCacheSize";

    /**
     * If is set to {@code true} then {@linkplain Environment#close()} doest't check if there are unfinished
     * transactions. Otherwise it checks and throws {@linkplain ExodusException} if there are.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @see Environment#close()
     */
    public static final String ENV_CLOSE_FORCEDLY = "exodus.env.closeForcedly";

    /**
     * Defines the number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2000L}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_MAX_COUNT
     * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
     */
    public static final String ENV_TXN_REPLAY_TIMEOUT = "exodus.env.txn.replayTimeout";

    /**
     * Defines the number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_TIMEOUT
     * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
     */
    public static final String ENV_TXN_REPLAY_MAX_COUNT = "exodus.env.txn.replayMaxCount";

    /**
     * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself after
     * {@linkplain Transaction#flush()}. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_TIMEOUT
     * @see #ENV_TXN_REPLAY_MAX_COUNT
     */
    public static final String ENV_TXN_DOWNGRADE_AFTER_FLUSH = "exodus.env.txn.downgradeAfterFlush";

    /**
     * Defines the number of {@linkplain Transaction transactions} that can be started in parallel. By default it is
     * unlimited.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_MAX_PARALLEL_READONLY_TXNS
     */
    public static final String ENV_MAX_PARALLEL_TXNS = "exodus.env.maxParallelTxns";

    /**
     * Defines the number of read-only {@linkplain Transaction transactions} that can be started in parallel. By
     * default it is unlimited.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see Transaction#isReadonly()
     * @see #ENV_MAX_PARALLEL_TXNS
     */
    public static final String ENV_MAX_PARALLEL_READONLY_TXNS = "exodus.env.maxParallelReadonlyTxns";

    /**
     * Defines {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this timeout then
     * it is reported in logs as stuck along with stack trace which it was created with. Default value is {@code 0}
     * which means that no timeout for a {@linkplain Transaction} is defined. In that case, no monitor of stuck
     * transactions is started. Otherwise it is started for each {@linkplain Environment}, though consuming only a
     * single {@linkplain Thread} amongst all environments created within a single class loader.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see #ENV_MONITOR_TXNS_CHECK_FREQ
     */
    public static final String ENV_MONITOR_TXNS_TIMEOUT = "exodus.env.monitorTxns.timeout";

    /**
     * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts and checks
     * {@linkplain Environment}'s transactions with this frequency (period) specified in milliseconds.
     * Default value is {@code 60000L}, one minute.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see #ENV_MONITOR_TXNS_TIMEOUT
     */
    public static final String ENV_MONITOR_TXNS_CHECK_FREQ = "exodus.env.monitorTxns.checkFreq";

    /**
     * If is set to {@code true} then the {@linkplain Environment} gathers statistics. If
     * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX managed bean.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see Environment#getStatistics()
     * @see #MANAGEMENT_ENABLED
     */
    public static final String ENV_GATHER_STATISTICS = "exodus.env.gatherStatistics";

    /**
     * Defines the maximum size of page of B+Tree. Default value is {@code 128}.
     * <p>Mutable at runtime: no
     */
    public static final String TREE_MAX_PAGE_SIZE = "exodus.tree.maxPageSize";

    /**
     * Defines the "tree-nodes" cache size. Default value is {@code 4096}.
     * <p>Mutable at runtime: yes
     */
    public static final String TREE_NODES_CACHE_SIZE = "exodus.tree.nodesCacheSize";

    /**
     * If is set to {@code true} then the database garbage collector is enabled. Default value is {@code true}.
     * Switching GC off makes sense only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_ENABLED = "exodus.gc.enabled";

    /**
     * Defines the number of milliseconds which the database garbage collector is postponed for after the
     * {@linkplain Environment} is created. Default value is {@code 60000}, one minute.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_START_IN = "exodus.gc.startIn";

    /**
     * Defines percent of minimum database utilization. Default value is {@code 70}. That means that only 30 percent
     * of free space in raw data in {@code Log} files (.xd files) is allowed. If database utilization is less than
     * defined, the database garbage collector is triggered.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_MIN_UTILIZATION = "exodus.gc.minUtilization";

    /**
     * If is set to {@code true} the database garbage collector renames files rather than deletes them. Default
     * value is {@code false}. It makes sense to change this setting only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_RENAME_FILES = "exodus.gc.renameFiles";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String GC_USE_EXPIRATION_CHECKER = "exodus.gc.useExpirationChecker";

    /**
     * Defines the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the database garbage
     * collector. The age of the last (the newest, the rightmost) {@code Log} file is {@code 0}, the age of previous
     * file is {@code 1}, etc. Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_MIN_FILE_AGE = "exodus.gc.fileMinAge";

    /**
     * Defines the number of new {@code Log} files (.xd files) that must be created to trigger if necessary (if database
     * utilization is not sufficient) the next background cleaning cycle (single run of the database garbage collector)
     * after the previous cycle finished. Default value is {@code 1}, i.e. GC can start after each newly created
     * {@code Log} file.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_FILES_INTERVAL = "exodus.gc.filesInterval";

    /**
     * Defined the number of milliseconds after which background cleaning cycle (single run of the database garbage
     * collector) can be repeated if the previous one didn't reach required utilization. Default value is
     * {@code 30000}, half a minute.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_RUN_PERIOD = "exodus.gc.runPeriod";

    /**
     * If is set to {@code true} then database utilization will be computed from scratch before the first cleaning
     * cycle (single run of the database garbage collector) will be triggered. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String GC_UTILIZATION_FROM_SCRATCH = "exodus.gc.utilization.fromScratch";

    /**
     * If is set to {@code true} the database garbage collector tries to acquire an exclusive {@linkplain Transaction}
     * for its purposes. In that case, GC transaction never re-plays. In order to not block background cleaner thread
     * forever, acquisition of exclusive GC transaction is performed with a timeout controlled by the
     * {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT} setting. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @see #GC_TRANSACTION_ACQUIRE_TIMEOUT
     * @see Transaction#isExclusive()
     */
    public static final String GC_USE_EXCLUSIVE_TRANSACTION = "exodus.gc.useExclusiveTransaction";

    /**
     * Defines timeout in milliseconds which is used by the database garbage collector to acquire exclusive
     * {@linkplain Transaction} for its purposes if {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}.
     * Default value is {@code 10}.
     * <p>Mutable at runtime: yes
     *
     * @see #GC_USE_EXCLUSIVE_TRANSACTION
     * @see Transaction#isExclusive()
     */
    public static final String GC_TRANSACTION_ACQUIRE_TIMEOUT = "exodus.gc.transactionAcquireTimeout";

    /**
     * Defines the number of milliseconds which deletion of any successfully cleaned {@code Log} file (.xd file)
     * is postponed for. Default value is {@code 5000}, 5 seconds.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_FILES_DELETION_DELAY = "exodus.gc.filesDeletionDelay";

    /**
     * If is set to {@code true} then the {@linkplain Environment} exposes two JMX managed beans. One for
     * {@linkplain Environment#getStatistics() environment statistics} and second for controlling the
     * {@code EnvironmentConfig} settings. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see Environment#getStatistics()
     * @see Environment#getEnvironmentConfig()
     */
    public static final String MANAGEMENT_ENABLED = "exodus.managementEnabled";

    public EnvironmentConfig() {
        this(ConfigurationStrategy.SYSTEM_PROPERTY);
    }

    public EnvironmentConfig(@NotNull final ConfigurationStrategy strategy) {
        //noinspection unchecked
        super(new Pair[]{
                new Pair(MEMORY_USAGE_PERCENTAGE, 50),
                new Pair(LOG_DURABLE_WRITE, false),
                new Pair(LOG_FILE_SIZE, 8192L),
                new Pair(LOG_LOCK_TIMEOUT, 0L),
                new Pair(LOG_CACHE_PAGE_SIZE, 65536),
                new Pair(LOG_CACHE_OPEN_FILES, 500),
                new Pair(LOG_CACHE_USE_NIO, true),
                new Pair(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, 1_000_000_000L),
                new Pair(LOG_CACHE_SHARED, true),
                new Pair(LOG_CACHE_NON_BLOCKING, true),
                new Pair(LOG_CLEAN_DIRECTORY_EXPECTED, false),
                new Pair(LOG_CLEAR_INVALID, false),
                new Pair(LOG_SYNC_PERIOD, 1000L),
                new Pair(LOG_FULL_FILE_READ_ONLY, true),
                new Pair(ENV_IS_READONLY, false),
                new Pair(ENV_READONLY_EMPTY_STORES, false),
                new Pair(ENV_STOREGET_CACHE_SIZE, 0),
                new Pair(ENV_CLOSE_FORCEDLY, false),
                new Pair(ENV_TXN_REPLAY_TIMEOUT, 2000L),
                new Pair(ENV_TXN_REPLAY_MAX_COUNT, 2),
                new Pair(ENV_TXN_DOWNGRADE_AFTER_FLUSH, true),
                new Pair(ENV_MAX_PARALLEL_TXNS, Integer.MAX_VALUE),
                new Pair(ENV_MAX_PARALLEL_READONLY_TXNS, Integer.MAX_VALUE),
                new Pair(ENV_MONITOR_TXNS_TIMEOUT, 0),
                new Pair(ENV_MONITOR_TXNS_CHECK_FREQ, 60000),
                new Pair(ENV_GATHER_STATISTICS, true),
                new Pair(TREE_MAX_PAGE_SIZE, 128),
                new Pair(TREE_NODES_CACHE_SIZE, 4096),
                new Pair(GC_ENABLED, true),
                new Pair(GC_START_IN, 60000),
                new Pair(GC_MIN_UTILIZATION, 70),
                new Pair(GC_RENAME_FILES, false),
                new Pair(GC_USE_EXPIRATION_CHECKER, true),
                new Pair(GC_MIN_FILE_AGE, 2),
                new Pair(GC_FILES_INTERVAL, 1),
                new Pair(GC_RUN_PERIOD, 30000),
                new Pair(GC_UTILIZATION_FROM_SCRATCH, false),
                new Pair(GC_FILES_DELETION_DELAY, 5000),
                new Pair(GC_USE_EXCLUSIVE_TRANSACTION, true),
                new Pair(GC_TRANSACTION_ACQUIRE_TIMEOUT, 10),
                new Pair(MANAGEMENT_ENABLED, true)
        }, strategy);
    }

    @Override
    public EnvironmentConfig setSetting(@NotNull final String key, @NotNull final Object value) {
        return (EnvironmentConfig) super.setSetting(key, value);
    }

    public Long /* NB! do not change to long */ getMemoryUsage() {
        return (Long) getSetting(MEMORY_USAGE);
    }

    public EnvironmentConfig setMemoryUsage(final long maxMemory) {
        return setSetting(MEMORY_USAGE, maxMemory);
    }

    public int getMemoryUsagePercentage() {
        return (Integer) getSetting(MEMORY_USAGE_PERCENTAGE);
    }

    public EnvironmentConfig setMemoryUsagePercentage(final int memoryUsagePercentage) {
        return setSetting(MEMORY_USAGE_PERCENTAGE, memoryUsagePercentage);
    }

    public boolean getLogDurableWrite() {
        return (Boolean) getSetting(LOG_DURABLE_WRITE);
    }

    public EnvironmentConfig setLogDurableWrite(final boolean durableWrite) {
        return setSetting(LOG_DURABLE_WRITE, durableWrite);
    }

    public long getLogFileSize() {
        return (Long) getSetting(LOG_FILE_SIZE);
    }

    public EnvironmentConfig setLogFileSize(final long kilobytes) {
        return setSetting(LOG_FILE_SIZE, kilobytes);
    }

    public long getLogLockTimeout() {
        return (Long) getSetting(LOG_LOCK_TIMEOUT);
    }

    public EnvironmentConfig setLogLockTimeout(final long millis) {
        return setSetting(LOG_LOCK_TIMEOUT, millis);
    }

    public int getLogCachePageSize() {
        return (Integer) getSetting(LOG_CACHE_PAGE_SIZE);
    }

    public EnvironmentConfig setLogCachePageSize(final int bytes) {
        return setSetting(LOG_CACHE_PAGE_SIZE, bytes);
    }

    public int getLogCacheOpenFilesCount() {
        return (Integer) getSetting(LOG_CACHE_OPEN_FILES);
    }

    public EnvironmentConfig setLogCacheOpenFilesCount(final int files) {
        return setSetting(LOG_CACHE_OPEN_FILES, files);
    }

    public boolean getLogCacheUseNio() {
        return (Boolean) getSetting(LOG_CACHE_USE_NIO);
    }

    public EnvironmentConfig setLogCacheUseNio(final boolean useNio) {
        return setSetting(LOG_CACHE_USE_NIO, useNio);
    }

    public long getLogCacheFreePhysicalMemoryThreshold() {
        return (Long) getSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD);
    }

    public EnvironmentConfig getLogCacheFreePhysicalMemoryThreshold(final long freePhysicalMemoryThreshold) {
        return setSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, freePhysicalMemoryThreshold);
    }

    public boolean isLogCacheShared() {
        return (Boolean) getSetting(LOG_CACHE_SHARED);
    }

    public EnvironmentConfig setLogCacheShared(final boolean shared) {
        return setSetting(LOG_CACHE_SHARED, shared);
    }

    public boolean isLogCacheNonBlocking() {
        return (Boolean) getSetting(LOG_CACHE_NON_BLOCKING);
    }

    public EnvironmentConfig setLogCacheNonBlocking(final boolean nonBlocking) {
        return setSetting(LOG_CACHE_NON_BLOCKING, nonBlocking);
    }

    public boolean isLogCleanDirectoryExpected() {
        return (Boolean) getSetting(LOG_CLEAN_DIRECTORY_EXPECTED);
    }

    public EnvironmentConfig setLogCleanDirectoryExpected(final boolean logCleanDirectoryExpected) {
        return setSetting(LOG_CLEAN_DIRECTORY_EXPECTED, logCleanDirectoryExpected);
    }

    public boolean isLogClearInvalid() {
        return (Boolean) getSetting(LOG_CLEAR_INVALID);
    }

    public EnvironmentConfig setLogClearInvalid(final boolean logClearInvalid) {
        return setSetting(LOG_CLEAR_INVALID, logClearInvalid);
    }

    public long getLogSyncPeriod() {
        return (Long) getSetting(LOG_SYNC_PERIOD);
    }

    public EnvironmentConfig setLogSyncPeriod(final long millis) {
        return setSetting(LOG_SYNC_PERIOD, millis);
    }

    public boolean isLogFullFileReadonly() {
        return (Boolean) getSetting(LOG_FULL_FILE_READ_ONLY);
    }

    public EnvironmentConfig setFullFileReadonly(final boolean readonly) {
        return setSetting(LOG_FULL_FILE_READ_ONLY, readonly);
    }

    public boolean getEnvIsReadonly() {
        return (Boolean) getSetting(ENV_IS_READONLY);
    }

    public EnvironmentConfig setEnvIsReadonly(final boolean isReadonly) {
        return setSetting(ENV_IS_READONLY, isReadonly);
    }

    public boolean getEnvReadonlyEmptyStores() {
        return (Boolean) getSetting(ENV_READONLY_EMPTY_STORES);
    }

    public EnvironmentConfig setEnvReadonlyEmptyStores(final boolean readonlyEmptyStores) {
        return setSetting(ENV_READONLY_EMPTY_STORES, readonlyEmptyStores);
    }

    public int getEnvStoreGetCacheSize() {
        return (Integer) getSetting(ENV_STOREGET_CACHE_SIZE);
    }

    public EnvironmentConfig setEnvStoreGetCacheSize(final int storeGetCacheSize) {
        if (storeGetCacheSize < 0) {
            throw new InvalidSettingException("Negative StoreGetCache size");
        }
        return setSetting(ENV_STOREGET_CACHE_SIZE, storeGetCacheSize);
    }

    public boolean getEnvCloseForcedly() {
        return (Boolean) getSetting(ENV_CLOSE_FORCEDLY);
    }

    public EnvironmentConfig setEnvCloseForcedly(final boolean closeForcedly) {
        return setSetting(ENV_CLOSE_FORCEDLY, closeForcedly);
    }

    public long getEnvTxnReplayTimeout() {
        return (Long) getSetting(ENV_TXN_REPLAY_TIMEOUT);
    }

    public EnvironmentConfig setEnvTxnReplayTimeout(final long timeout) {
        if (timeout < 0) {
            throw new InvalidSettingException("Negative transaction replay timeout");
        }
        return setSetting(ENV_TXN_REPLAY_TIMEOUT, timeout);
    }

    public int getEnvTxnReplayMaxCount() {
        return (Integer) getSetting(ENV_TXN_REPLAY_MAX_COUNT);
    }

    public EnvironmentConfig setEnvTxnReplayMaxCount(final int count) {
        if (count < 0) {
            throw new InvalidSettingException("Negative transaction replay count");
        }
        return setSetting(ENV_TXN_REPLAY_MAX_COUNT, count);
    }

    public boolean getEnvTxnDowngradeAfterFlush() {
        return (Boolean) getSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH);
    }

    public EnvironmentConfig setEnvTxnDowngradeAfterFlush(final boolean downgrade) {
        return setSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH, downgrade);
    }

    public int getEnvMaxParallelTxns() {
        return (Integer) getSetting(ENV_MAX_PARALLEL_TXNS);
    }

    public EnvironmentConfig setEnvMaxParallelTxns(final int maxParallelTxns) {
        return setSetting(ENV_MAX_PARALLEL_TXNS, maxParallelTxns);
    }

    public int getEnvMaxParallelReadonlyTxns() {
        return (Integer) getSetting(ENV_MAX_PARALLEL_READONLY_TXNS);
    }

    public EnvironmentConfig setEnvMaxParallelReadonlyTxns(final int maxParallelReadonlyTxns) {
        return setSetting(ENV_MAX_PARALLEL_TXNS, maxParallelReadonlyTxns);
    }

    public int getEnvMonitorTxnsTimeout() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_TIMEOUT);
    }

    public EnvironmentConfig setEnvMonitorTxnsTimeout(final int timeout) {
        if (timeout != 0 && timeout < 1000) {
            throw new InvalidSettingException("Transaction timeout should be greater than a second");
        }
        setSetting(ENV_MONITOR_TXNS_TIMEOUT, timeout);
        if (timeout > 0 && timeout < getEnvMonitorTxnsCheckFreq()) {
            setEnvMonitorTxnsCheckFreq(timeout);
        }
        return this;
    }

    public int getEnvMonitorTxnsCheckFreq() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_CHECK_FREQ);
    }

    public EnvironmentConfig setEnvMonitorTxnsCheckFreq(final int freq) {
        return setSetting(ENV_MONITOR_TXNS_CHECK_FREQ, freq);
    }

    public boolean getEnvGatherStatistics() {
        return (Boolean) getSetting(ENV_GATHER_STATISTICS);
    }

    public EnvironmentConfig setEnvGatherStatistics(final boolean gatherStatistics) {
        return setSetting(ENV_GATHER_STATISTICS, gatherStatistics);
    }

    public int getTreeMaxPageSize() {
        return (Integer) getSetting(TREE_MAX_PAGE_SIZE);
    }

    public EnvironmentConfig setTreeMaxPageSize(final int pageSize) throws InvalidSettingException {
        if (pageSize < 16 || pageSize > 1024) {
            throw new InvalidSettingException("Invalid tree page size: " + pageSize);
        }
        return setSetting(TREE_MAX_PAGE_SIZE, pageSize);
    }

    public int getTreeNodesCacheSize() {
        return (Integer) getSetting(TREE_NODES_CACHE_SIZE);
    }

    public EnvironmentConfig setTreeNodesCacheSize(final int cacheSize) {
        return setSetting(TREE_NODES_CACHE_SIZE, cacheSize);
    }

    public boolean isGcEnabled() {
        return (Boolean) getSetting(GC_ENABLED);
    }

    public EnvironmentConfig setGcEnabled(boolean enabled) {
        return setSetting(GC_ENABLED, enabled);
    }

    public int getGcStartIn() {
        return (Integer) getSetting(GC_START_IN);
    }

    public EnvironmentConfig setGcStartIn(final int startInMillis) {
        if (startInMillis < 0) {
            throw new InvalidSettingException("GC can't be postponed for that number of milliseconds: " + startInMillis);
        }
        return setSetting(GC_START_IN, startInMillis);
    }

    public int getGcMinUtilization() {
        return (Integer) getSetting(GC_MIN_UTILIZATION);
    }

    public EnvironmentConfig setGcMinUtilization(int percent) throws InvalidSettingException {
        if (percent < 1 || percent > 90) {
            throw new InvalidSettingException("Invalid minimum log files utilization: " + percent);
        }
        return setSetting(GC_MIN_UTILIZATION, percent);
    }

    public boolean getGcRenameFiles() {
        return (Boolean) getSetting(GC_RENAME_FILES);
    }

    public EnvironmentConfig setGcRenameFiles(boolean rename) {
        return setSetting(GC_RENAME_FILES, rename);
    }

    public boolean getGcUseExpirationChecker() {
        return (Boolean) getSetting(GC_USE_EXPIRATION_CHECKER);
    }

    public EnvironmentConfig setGcUseExpirationChecker(boolean useExpirationChecker) {
        return setSetting(GC_USE_EXPIRATION_CHECKER, useExpirationChecker);
    }

    public int getGcFileMinAge() {
        return (Integer) getSetting(GC_MIN_FILE_AGE);
    }

    public EnvironmentConfig setGcFileMinAge(int minAge) throws InvalidSettingException {
        if (minAge < 1) {
            throw new InvalidSettingException("Invalid file minimum age: " + minAge);
        }
        return setSetting(GC_MIN_FILE_AGE, minAge);
    }

    public int getGcFilesInterval() {
        return (Integer) getSetting(GC_FILES_INTERVAL);
    }

    public EnvironmentConfig setGcFilesInterval(final int files) throws InvalidSettingException {
        if (files < 1) {
            throw new InvalidSettingException("Invalid number of files: " + files);
        }
        return setSetting(GC_FILES_INTERVAL, files);
    }

    public int getGcRunPeriod() {
        return (Integer) getSetting(GC_RUN_PERIOD);
    }

    public EnvironmentConfig setGcRunPeriod(final int runPeriod) {
        if (runPeriod < 0) {
            throw new InvalidSettingException("Invalid GC run period: " + runPeriod);
        }
        return setSetting(GC_RUN_PERIOD, runPeriod);
    }

    public boolean getGcUtilizationFromScratch() {
        return (Boolean) getSetting(GC_UTILIZATION_FROM_SCRATCH);
    }

    public EnvironmentConfig setGcUtilizationFromScratch(final boolean fromScratch) {
        return setSetting(GC_UTILIZATION_FROM_SCRATCH, fromScratch);
    }

    public boolean getGcUseExclusiveTransaction() {
        return (Boolean) getSetting(GC_USE_EXCLUSIVE_TRANSACTION);
    }

    public EnvironmentConfig setGcUseExclusiveTransaction(final boolean useExclusiveTransaction) {
        return setSetting(GC_USE_EXCLUSIVE_TRANSACTION, useExclusiveTransaction);
    }

    public int getGcTransactionAcquireTimeout() {
        return (Integer) getSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT);
    }

    public EnvironmentConfig setGcTransactionAcquireTimeout(final int txnAcquireTimeout) {
        return setSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT, txnAcquireTimeout);
    }

    public int getGcFilesDeletionDelay() {
        return (Integer) getSetting(GC_FILES_DELETION_DELAY);
    }

    public EnvironmentConfig setGcFilesDeletionDelay(final int delay) {
        if (delay < 0) {
            throw new InvalidSettingException("Invalid GC files deletion delay: " + delay);
        }
        return setSetting(GC_FILES_DELETION_DELAY, delay);
    }

    public boolean isManagementEnabled() {
        return (Boolean) getSetting(MANAGEMENT_ENABLED);
    }

    public EnvironmentConfig setManagementEnabled(final boolean managementEnabled) {
        return setSetting(MANAGEMENT_ENABLED, managementEnabled);
    }
}
