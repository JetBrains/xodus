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

public interface EnvironmentConfigMBean {

    String OBJECT_NAME_PREFIX = "jetbrains.exodus.env: type=EnvironmentConfig";

    long getMemoryUsage();

    int getMemoryUsagePercent();

    boolean getLogDurableWrite();

    void setLogDurableWrite(boolean durableWrite);

    long getLogFileSize();

    long getLogLockTimeout();

    int getLogCachePageSize();

    int getLogCacheOpenFilesCount();

    boolean isLogCacheShared();

    boolean isLogCacheNonBlocking();

    boolean isLogCleanDirectoryExpected();

    boolean isLogClearInvalid();

    long getLogSyncPeriod();

    void setLogSyncPeriod(long millis);

    boolean getEnvIsReadonly();

    void setEnvIsReadonly(boolean isReadonly);

    int getEnvStoreGetCacheSize();

    void setEnvStoreGetCacheSize(int storeGetCacheSize);

    boolean getEnvCloseForcedly();

    void setEnvCloseForcedly(boolean closeForcedly);

    long getEnvTxnReplayTimeout();

    void setEnvTxnReplayTimeout(final long txnReplayTimeout);

    int getEnvTxnReplayMaxCount();

    void setEnvTxnReplayMaxCount(final int txnReplayMaxCount);

    int getEnvMonitorTxnsTimeout();

    int getEnvMonitorTxnsCheckFreq();

    boolean getEnvGatherStatistics();

    int getTreeMaxPageSize();

    int getTreeNodesCacheSize();

    void setTreeNodesCacheSize(int cacheSize);

    boolean isGcEnabled();

    void setGcEnabled(boolean enabled);

    int getGcStartIn();

    int getGcMinUtilization();

    void setGcMinUtilization(int percent);

    boolean getGcRenameFiles();

    void setGcRenameFiles(boolean rename);

    boolean getGcUseExpirationChecker();

    int getGcFileMinAge();

    void setGcFileMinAge(int minAge);

    int getGcFilesInterval();

    void setGcFilesInterval(int files);

    int getGcRunPeriod();

    void setGcRunPeriod(int runPeriod);

    boolean getGcUtilizationFromScratch();

    void close();
}
