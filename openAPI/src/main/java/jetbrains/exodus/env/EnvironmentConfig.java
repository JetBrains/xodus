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

    public static final String ENV_MONITOR_TXNS_TIMEOUT = "exodus.env.monitorTxns.timeout"; // in milliseconds

    public static final String ENV_MONITOR_TXNS_CHECK_FREQ = "exodus.env.monitorTxns.checkFreq"; // in milliseconds

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

    public static final String GC_UTILIZATION_FROM_SCRATCH = "exodus.gc.utilization.fromScratch";

    public static final String MANAGEMENT_ENABLED = "exodus.managementEnabled";

    public EnvironmentConfig() {
        this(ConfigurationStrategy.SYSTEM_PROPERTY);
    }

    public EnvironmentConfig(@NotNull final ConfigurationStrategy strategy) {
        //noinspection unchecked
        super(new Pair[]{
                new Pair(MEMORY_USAGE_PERCENTAGE, 60),
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
                new Pair(ENV_MONITOR_TXNS_CHECK_FREQ, 60000),
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
                new Pair(GC_UTILIZATION_FROM_SCRATCH, false),
                new Pair(MANAGEMENT_ENABLED, true)
        }, strategy);
    }

    public Long /* NB! do not change to long */ getMemoryUsage() {
        return (Long) getSetting(MEMORY_USAGE);
    }

    public void setMemoryUsage(final long maxMemory) {
        setSetting(MEMORY_USAGE, maxMemory);
    }

    public int getMemoryUsagePercentage() {
        return (Integer) getSetting(MEMORY_USAGE_PERCENTAGE);
    }

    public void setMemoryUsagePercentage(int memoryUsagePercentage) {
        setSetting(MEMORY_USAGE_PERCENTAGE, memoryUsagePercentage);
    }

    public boolean getLogDurableWrite() {
        return (Boolean) getSetting(LOG_DURABLE_WRITE);
    }

    public void setLogDurableWrite(boolean durableWrite) {
        setSetting(LOG_DURABLE_WRITE, durableWrite);
    }

    public long getLogFileSize() {
        return (Long) getSetting(LOG_FILE_SIZE);
    }

    public void setLogFileSize(long kilobytes) {
        setSetting(LOG_FILE_SIZE, kilobytes);
    }

    public long getLogLockTimeout() {
        return (Long) getSetting(LOG_LOCK_TIMEOUT);
    }

    public void setLogLockTimeout(long millis) {
        setSetting(LOG_LOCK_TIMEOUT, millis);
    }

    public int getLogCachePageSize() {
        return (Integer) getSetting(LOG_CACHE_PAGE_SIZE);
    }

    public void setLogCachePageSize(int bytes) {
        setSetting(LOG_CACHE_PAGE_SIZE, bytes);
    }

    public int getLogCacheOpenFilesCount() {
        return (Integer) getSetting(LOG_CACHE_OPEN_FILES);
    }

    public void setLogCacheOpenFilesCount(int files) {
        setSetting(LOG_CACHE_OPEN_FILES, files);
    }

    public boolean isLogCacheShared() {
        return (Boolean) getSetting(LOG_CACHE_SHARED);
    }

    public void setLogCacheShared(boolean shared) {
        setSetting(LOG_CACHE_SHARED, shared);
    }

    public boolean isLogCacheNonBlocking() {
        return (Boolean) getSetting(LOG_CACHE_NON_BLOCKING);
    }

    public void setLogCacheNonBlocking(boolean nonBlocking) {
        setSetting(LOG_CACHE_NON_BLOCKING, nonBlocking);
    }

    public boolean isLogCleanDirectoryExpected() {
        return (Boolean) getSetting(LOG_CLEAN_DIRECTORY_EXPECTED);
    }

    public void setLogCleanDirectoryExpected(boolean logCleanDirectoryExpected) {
        setSetting(LOG_CLEAN_DIRECTORY_EXPECTED, logCleanDirectoryExpected);
    }

    public boolean isLogClearInvalid() {
        return (Boolean) getSetting(LOG_CLEAR_INVALID);
    }

    public void setLogClearInvalid(boolean logClearInvalid) {
        setSetting(LOG_CLEAR_INVALID, logClearInvalid);
    }

    public long getLogSyncPeriod() {
        return (Long) getSetting(LOG_SYNC_PERIOD);
    }

    public void setLogSyncPeriod(long millis) {
        setSetting(LOG_SYNC_PERIOD, millis);
    }

    public boolean getEnvIsReadonly() {
        return (Boolean) getSetting(ENV_IS_READONLY);
    }

    public void setEnvIsReadonly(final boolean isReadonly) {
        setSetting(ENV_IS_READONLY, isReadonly);
    }

    public boolean getEnvReadonlyEmptyStores() {
        return (Boolean) getSetting(ENV_READONLY_EMPTY_STORES);
    }

    public void setEnvReadonlyEmptyStores(final boolean readonlyEmptyStores) {
        setSetting(ENV_READONLY_EMPTY_STORES, readonlyEmptyStores);
    }

    public int getEnvStoreGetCacheSize() {
        return (Integer) getSetting(ENV_STOREGET_CACHE_SIZE);
    }

    public void setEnvStoreGetCacheSize(final int storeGetCacheSize) {
        if (storeGetCacheSize < 0) {
            throw new InvalidSettingException("Negative StoreGetCache size");
        }
        setSetting(ENV_STOREGET_CACHE_SIZE, storeGetCacheSize);
    }

    public boolean getEnvCloseForcedly() {
        return (Boolean) getSetting(ENV_CLOSE_FORCEDLY);
    }

    public void setEnvCloseForcedly(boolean closeForcedly) {
        setSetting(ENV_CLOSE_FORCEDLY, closeForcedly);
    }

    public int getEnvMonitorTxnsTimeout() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_TIMEOUT);
    }

    public void setEnvMonitorTxnsTimeout(final int timeout) {
        if (timeout != 0 && timeout < 1000) {
            throw new InvalidSettingException("Transaction timeout should be greater than a second");
        }
        setSetting(ENV_MONITOR_TXNS_TIMEOUT, timeout);
        if (timeout > 0 && timeout < getEnvMonitorTxnsCheckFreq()) {
            setEnvMonitorTxnsCheckFreq(timeout);
        }
    }

    public int getEnvMonitorTxnsCheckFreq() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_CHECK_FREQ);
    }

    public void setEnvMonitorTxnsCheckFreq(final int freq) {
        setSetting(ENV_MONITOR_TXNS_CHECK_FREQ, freq);
    }

    public int getTreeMaxPageSize() {
        return (Integer) getSetting(TREE_MAX_PAGE_SIZE);
    }

    public void setTreeMaxPageSize(final int pageSize) throws InvalidSettingException {
        if (pageSize < 16 || pageSize > 1024) {
            throw new InvalidSettingException("Invalid tree page size: " + pageSize);
        }
        setSetting(TREE_MAX_PAGE_SIZE, pageSize);
    }

    public int getTreeNodesCacheSize() {
        return (Integer) getSetting(TREE_NODES_CACHE_SIZE);
    }

    public void setTreeNodesCacheSize(final int cacheSize) {
        setSetting(TREE_NODES_CACHE_SIZE, cacheSize);
    }

    public boolean isGcEnabled() {
        return (Boolean) getSetting(GC_ENABLED);
    }

    public void setGcEnabled(boolean enabled) {
        setSetting(GC_ENABLED, enabled);
    }

    public int getGcStartIn() {
        return (Integer) getSetting(GC_START_IN);
    }

    public void setGcStartIn(final int startInMillis) {
        if (startInMillis < 0) {
            throw new InvalidSettingException("GC can't be postponed for that number of milliseconds: " + startInMillis);
        }
        setSetting(GC_START_IN, startInMillis);
    }

    public int getGcMinUtilization() {
        return (Integer) getSetting(GC_MIN_UTILIZATION);
    }

    public void setGcMinUtilization(int percent) throws InvalidSettingException {
        if (percent < 1 || percent > 90) {
            throw new InvalidSettingException("Invalid minimum log files utilization: " + percent);
        }
        setSetting(GC_MIN_UTILIZATION, percent);
    }

    public boolean getGcRenameFiles() {
        return (Boolean) getSetting(GC_RENAME_FILES);
    }

    public void setGcRenameFiles(boolean rename) {
        setSetting(GC_RENAME_FILES, rename);
    }

    public boolean getGcUseExpirationChecker() {
        return (Boolean) getSetting(GC_USE_EXPIRATION_CHECKER);
    }

    public void setGcUseExpirationChecker(boolean useExpirationChecker) {
        setSetting(GC_USE_EXPIRATION_CHECKER, useExpirationChecker);
    }

    public int getGcFileMinAge() {
        return (Integer) getSetting(GC_MIN_FILE_AGE);
    }

    public void setGcFileMinAge(int minAge) throws InvalidSettingException {
        if (minAge < 1) {
            throw new InvalidSettingException("Invalid file minimum age: " + minAge);
        }
        setSetting(GC_MIN_FILE_AGE, minAge);
    }

    public int getGcFilesInterval() {
        return (Integer) getSetting(GC_FILES_INTERVAL);
    }

    public void setGcFilesInterval(int files) throws InvalidSettingException {
        if (files < 1) {
            throw new InvalidSettingException("Invalid number of files: " + files);
        }
        setSetting(GC_FILES_INTERVAL, files);
    }

    public boolean getGcUtilizationFromScratch() {
        return (Boolean) getSetting(GC_UTILIZATION_FROM_SCRATCH);
    }

    public void setGcUtilizationFromScratch(boolean fromScratch) {
        setSetting(GC_UTILIZATION_FROM_SCRATCH, fromScratch);
    }

    public boolean isManagementEnabled() {
        return (Boolean) getSetting(MANAGEMENT_ENABLED);
    }

    public void setManagementEnabled(final boolean managementEnabled) {
        setSetting(MANAGEMENT_ENABLED, managementEnabled);
    }
}
