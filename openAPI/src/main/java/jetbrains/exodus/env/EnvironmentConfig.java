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
import jetbrains.exodus.ConfigurationStrategy;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("UnusedDeclaration")
public final class EnvironmentConfig extends AbstractConfig {

    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig(ConfigurationStrategy.IGNORE);

    public static final String MEMORY_USAGE = "exodus.memoryUsage";

    public static final String MEMORY_USAGE_PERCENTAGE = "exodus.memoryUsagePercentage";

    public static final String LOG_DURABLE_WRITE = "exodus.log.durableWrite";

    public static final String LOG_FILE_SIZE = "exodus.log.fileSize"; // in Kb

    public static final String LOG_LOCK_TIMEOUT = "exodus.log.lockTimeout"; // in milliseconds

    public static final String LOG_CACHE_PAGE_SIZE = "exodus.log.cache.pageSize"; // in bytes

    public static final String LOG_CACHE_OPEN_FILES = "exodus.log.cache.openFilesCount";

    public static final String LOG_CACHE_SHARED = "exodus.log.cache.shared";

    public static final String LOG_CACHE_NON_BLOCKING = "exodus.log.cache.nonBlocking";

    public static final String LOG_CLEAN_DIRECTORY_EXPECTED = "exodus.log.cleanDirectoryExpected";

    public static final String LOG_CLEAR_INVALID = "exodus.log.clearInvalid";

    public static final String LOG_SYNC_PERIOD = "exodus.log.syncPeriod"; // in milliseconds

    public static final String ENV_IS_READONLY = "exodus.env.isReadonly";

    /**
     * If this setting is set to {@code true} and exodus.env.isReadonly is also {@code true},
     * env.openStore() doesn't try to create a store, but returns an empty immutable instance instead
     */
    public static final String ENV_READONLY_EMPTY_STORES = "exodus.env.readonly.emptyStores";

    public static final String ENV_STOREGET_CACHE_SIZE = "exodus.env.storeGetCacheSize";

    public static final String ENV_CLOSE_FORCEDLY = "exodus.env.closeForcedly";

    public static final String ENV_TXN_REPLAY_TIMEOUT = "exodus.env.txn.replayTimeout"; // in milliseconds

    public static final String ENV_TXN_REPLAY_MAX_COUNT = "exodus.env.txn.replayMaxCount";

    public static final String ENV_MAX_PARALLEL_TXNS = "exodus.env.maxParallelTxns";

    public static final String ENV_MAX_PARALLEL_READONLY_TXNS = "exodus.env.maxParallelReadonlyTxns";

    public static final String ENV_MONITOR_TXNS_TIMEOUT = "exodus.env.monitorTxns.timeout"; // in milliseconds

    public static final String ENV_MONITOR_TXNS_CHECK_FREQ = "exodus.env.monitorTxns.checkFreq"; // in milliseconds

    public static final String ENV_GATHER_STATISTICS = "exodus.env.gatherStatistics";

    public static final String TREE_MAX_PAGE_SIZE = "exodus.tree.maxPageSize";

    public static final String TREE_NODES_CACHE_SIZE = "exodus.tree.nodesCacheSize";

    public static final String GC_ENABLED = "exodus.gc.enabled";

    public static final String GC_START_IN = "exodus.gc.startIn"; // in milliseconds

    public static final String GC_MIN_UTILIZATION = "exodus.gc.minUtilization";

    public static final String GC_RENAME_FILES = "exodus.gc.renameFiles";

    public static final String GC_USE_EXPIRATION_CHECKER = "exodus.gc.useExpirationChecker";

    /**
     * Minimum age of a file to consider it for cleaning.
     */
    public static final String GC_MIN_FILE_AGE = "exodus.gc.fileMinAge";

    /**
     * Cleaner checks log utilization and runs if necessary after this many new files are created in the log.
     */
    public static final String GC_FILES_INTERVAL = "exodus.gc.filesInterval";

    /**
     * If a single cleaner run didn't reach target utilization then next run will happen in this number of milliseconds
     */
    public static final String GC_RUN_PERIOD = "exodus.gc.runPeriod";

    public static final String GC_UTILIZATION_FROM_SCRATCH = "exodus.gc.utilization.fromScratch";

    public static final String GC_USE_EXCLUSIVE_TRANSACTION = "exodus.gc.useExclusiveTransaction";

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
                new Pair(LOG_CACHE_OPEN_FILES, 50),
                new Pair(LOG_CACHE_SHARED, true),
                new Pair(LOG_CACHE_NON_BLOCKING, true),
                new Pair(LOG_CLEAN_DIRECTORY_EXPECTED, false),
                new Pair(LOG_CLEAR_INVALID, false),
                new Pair(LOG_SYNC_PERIOD, 1000L),
                new Pair(ENV_IS_READONLY, false),
                new Pair(ENV_READONLY_EMPTY_STORES, false),
                new Pair(ENV_STOREGET_CACHE_SIZE, 0),
                new Pair(ENV_CLOSE_FORCEDLY, false),
                new Pair(ENV_TXN_REPLAY_TIMEOUT, 1000L),
                new Pair(ENV_TXN_REPLAY_MAX_COUNT, 2),
                new Pair(ENV_MAX_PARALLEL_TXNS, Integer.MAX_VALUE),
                new Pair(ENV_MAX_PARALLEL_READONLY_TXNS, Integer.MAX_VALUE),
                new Pair(ENV_MONITOR_TXNS_CHECK_FREQ, 60000),
                new Pair(ENV_GATHER_STATISTICS, true),
                new Pair(ENV_MONITOR_TXNS_TIMEOUT, 0),
                new Pair(TREE_MAX_PAGE_SIZE, 128),
                new Pair(TREE_NODES_CACHE_SIZE, 4096),
                new Pair(GC_ENABLED, true),
                new Pair(GC_START_IN, 60000),
                new Pair(GC_MIN_UTILIZATION, 75),
                new Pair(GC_RENAME_FILES, false),
                new Pair(GC_USE_EXPIRATION_CHECKER, true),
                new Pair(GC_MIN_FILE_AGE, 2),
                new Pair(GC_FILES_INTERVAL, 1),
                new Pair(GC_RUN_PERIOD, 30000),
                new Pair(GC_UTILIZATION_FROM_SCRATCH, false),
                new Pair(GC_USE_EXCLUSIVE_TRANSACTION, true),
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

    public boolean isManagementEnabled() {
        return (Boolean) getSetting(MANAGEMENT_ENABLED);
    }

    public EnvironmentConfig setManagementEnabled(final boolean managementEnabled) {
        return setSetting(MANAGEMENT_ENABLED, managementEnabled);
    }
}
