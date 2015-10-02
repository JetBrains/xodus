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
package jetbrains.exodus.env.management;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.TransactionBase;
import jetbrains.exodus.env.TransactionImpl;
import jetbrains.exodus.management.MBeanBase;
import org.jetbrains.annotations.NotNull;

public class EnvironmentConfig extends MBeanBase implements EnvironmentConfigMBean {

    @NotNull
    private final EnvironmentImpl env;
    @NotNull
    private final jetbrains.exodus.env.EnvironmentConfig config;

    public EnvironmentConfig(@NotNull final EnvironmentImpl env) {
        super(getObjectName(env));
        this.env = env;
        config = env.getEnvironmentConfig();
    }

    @Override
    public long getMemoryUsage() {
        return config.getMemoryUsage();
    }

    @Override
    public int getMemoryUsagePercent() {
        return config.getMemoryUsagePercentage();
    }

    @Override
    public boolean getLogDurableWrite() {
        return config.getLogDurableWrite();
    }

    @Override
    public void setLogDurableWrite(boolean durableWrite) {
        config.setLogDurableWrite(durableWrite);
    }

    @Override
    public long getLogFileSize() {
        return config.getLogFileSize();
    }

    @Override
    public long getLogLockTimeout() {
        return config.getLogLockTimeout();
    }

    @Override
    public int getLogCachePageSize() {
        return config.getLogCachePageSize();
    }

    @Override
    public int getLogCacheOpenFilesCount() {
        return config.getLogCacheOpenFilesCount();
    }

    @Override
    public boolean isLogCacheShared() {
        return config.isLogCacheShared();
    }

    @Override
    public boolean isLogCacheNonBlocking() {
        return config.isLogCacheNonBlocking();
    }

    @Override
    public boolean isLogCleanDirectoryExpected() {
        return config.isLogCleanDirectoryExpected();
    }

    @Override
    public boolean isLogClearInvalid() {
        return config.isLogClearInvalid();
    }

    @Override
    public long getLogSyncPeriod() {
        return config.getLogSyncPeriod();
    }

    @Override
    public void setLogSyncPeriod(long millis) {
        config.setLogSyncPeriod(millis);
    }

    @Override
    public boolean getEnvIsReadonly() {
        return config.getEnvIsReadonly();
    }

    @Override
    public void setEnvIsReadonly(boolean isReadonly) {
        if (config.getEnvIsReadonly() == isReadonly) {
            return;
        }
        if (isReadonly) {
            env.suspendGC();
            final TransactionBase txn = env.beginTransaction();
            try {
                if (!txn.isReadonly()) {
                    txn.setCommitHook(new Runnable() {
                        @Override
                        public void run() {
                            config.setEnvIsReadonly(true);
                        }
                    });
                    ((TransactionImpl) txn).forceFlush();
                }
            } finally {
                txn.abort();
            }
        } else {
            env.resumeGC();
            config.setEnvIsReadonly(false);
        }
    }

    @Override
    public int getEnvStoreGetCacheSize() {
        return config.getEnvStoreGetCacheSize();
    }

    @Override
    public void setEnvStoreGetCacheSize(int storeGetCacheSize) {
        config.setEnvStoreGetCacheSize(storeGetCacheSize);
    }

    @Override
    public boolean getEnvCloseForcedly() {
        return config.getEnvCloseForcedly();
    }

    @Override
    public void setEnvCloseForcedly(boolean closeForcedly) {
        config.setEnvCloseForcedly(closeForcedly);
    }

    @Override
    public long getEnvTxnReplayTimeout() {
        return config.getEnvTxnReplayTimeout();
    }

    @Override
    public void setEnvTxnReplayTimeout(final long txnReplayTimeout) {
        config.setEnvTxnReplayTimeout(txnReplayTimeout);
    }

    @Override
    public int getEnvTxnReplayMaxCount() {
        return config.getEnvTxnReplayMaxCount();
    }

    @Override
    public void setEnvTxnReplayMaxCount(final int txnReplayMaxCount) {
        config.setEnvTxnReplayMaxCount(txnReplayMaxCount);
    }

    @Override
    public int getEnvMonitorTxnsTimeout() {
        return config.getEnvMonitorTxnsTimeout();
    }

    @Override
    public int getEnvMonitorTxnsCheckFreq() {
        return config.getEnvMonitorTxnsCheckFreq();
    }

    @Override
    public boolean getEnvGatherStatistics() {
        return config.getEnvGatherStatistics();
    }

    @Override
    public int getTreeMaxPageSize() {
        return config.getTreeMaxPageSize();
    }

    @Override
    public int getTreeNodesCacheSize() {
        return config.getTreeNodesCacheSize();
    }

    @Override
    public void setTreeNodesCacheSize(int cacheSize) {
        config.setTreeNodesCacheSize(cacheSize);
    }

    @Override
    public boolean isGcEnabled() {
        return config.isGcEnabled();
    }

    @Override
    public void setGcEnabled(boolean enabled) {
        config.setGcEnabled(enabled);
    }

    @Override
    public int getGcStartIn() {
        return config.getGcStartIn();
    }

    @Override
    public int getGcMinUtilization() {
        return config.getGcMinUtilization();
    }

    @Override
    public void setGcMinUtilization(int percent) {
        config.setGcMinUtilization(percent);
    }

    @Override
    public boolean getGcRenameFiles() {
        return config.getGcRenameFiles();
    }

    @Override
    public void setGcRenameFiles(boolean rename) {
        config.setGcRenameFiles(rename);
    }

    @Override
    public boolean getGcUseExpirationChecker() {
        return config.getGcUseExpirationChecker();
    }

    @Override
    public int getGcFileMinAge() {
        return config.getGcFileMinAge();
    }

    @Override
    public void setGcFileMinAge(int minAge) {
        config.setGcFileMinAge(minAge);
    }

    @Override
    public int getGcFilesInterval() {
        return config.getGcFilesInterval();
    }

    @Override
    public void setGcFilesInterval(int files) {
        config.setGcFilesInterval(files);
    }

    @Override
    public int getGcRunPeriod() {
        return config.getGcRunPeriod();
    }

    @Override
    public void setGcRunPeriod(int runPeriod) {
        config.setGcRunPeriod(runPeriod);
    }

    @Override
    public boolean getGcUtilizationFromScratch() {
        return config.getGcUtilizationFromScratch();
    }

    @Override
    public void close() {
        env.close();
        super.close();
    }

    public static String getObjectName(@NotNull final Environment env) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(env.getLocation());
    }
}
