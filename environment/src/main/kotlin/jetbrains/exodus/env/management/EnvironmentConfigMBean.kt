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
package jetbrains.exodus.env.management

const val CONFIG_OBJECT_NAME_PREFIX = "jetbrains.exodus.env: type=EnvironmentConfig"

interface EnvironmentConfigMBean {
    fun getMemoryUsage(): Long
    fun getMemoryUsagePercent(): Int
    fun getLogDurableWrite(): Boolean
    fun setLogDurableWrite(durableWrite: Boolean)
    fun getLogFileSize(): Long
    fun getLogLockTimeout(): Long
    fun getLogCachePageSize(): Int
    fun getLogCacheOpenFilesCount(): Int
    fun getLogCacheFreePhysicalMemoryThreshold(): Long
    fun isLogCacheShared(): Boolean
    fun isLogCacheNonBlocking(): Boolean
    fun getLogCacheGenerationCount(): Int
    fun getLogCacheReadAheadMultiple(): Int
    fun setLogCacheReadAheadMultiple(readAheadMultiple: Int)
    fun isLogCleanDirectoryExpected(): Boolean
    fun isLogClearInvalid(): Boolean
    fun getLogSyncPeriod(): Long
    fun setLogSyncPeriod(syncPeriod: Long)
    fun isLogFullFileReadonly(): Boolean
    fun isLogAllowRemovable(): Boolean
    fun isLogAllowRemote(): Boolean
    fun isLogAllowRamDisk(): Boolean
    fun isEnvIsReadonly(): Boolean
    fun setEnvIsReadonly(readOnly: Boolean)
    fun isEnvFailFastInReadonly(): Boolean
    fun setEnvFailFastInReadonly(failFast: Boolean)
    fun isEnvReadonlyEmptyStores(): Boolean
    fun setEnvReadonlyEmptyStores(readOnly: Boolean)
    fun getEnvStoreGetCacheSize(): Int
    fun setEnvStoreGetCacheSize(cacheSize: Int)
    fun getEnvStoreGetCacheMinTreeSize(): Int
    fun setEnvStoreGetCacheMinTreeSize(minTreeSize: Int)
    fun getEnvStoreGetCacheMaxValueSize(): Int
    fun setEnvStoreGetCacheMaxValueSize(maxValueSize: Int)
    fun isEnvCloseForcedly(): Boolean
    fun setEnvCloseForcedly(closeForcedly: Boolean)
    fun getEnvTxnReplayTimeout(): Long
    fun setEnvTxnReplayTimeout(txnReplayTimeout: Long)
    fun getEnvTxnReplayMaxCount(): Int
    fun setEnvTxnReplayMaxCount(txnReplayMaxCount: Int)
    fun isEnvTxnDowngradeAfterFlush(): Boolean
    fun setEnvTxnDowngradeAfterFlush(downgrade: Boolean)
    fun isEnvTxnSingleThreadWrites(): Boolean
    fun setEnvTxnSingleThreadWrites(singleThreadWrites: Boolean)
    fun isEnvTxnTraceFinish(): Boolean
    fun setEnvTxnTraceFinish(traceFinish: Boolean)
    fun getEnvMaxParallelTxns(): Int
    fun getEnvMonitorTxnsTimeout(): Int
    fun getEnvMonitorTxnsCheckFreq(): Int
    fun isEnvGatherStatistics(): Boolean
    fun getTreeMaxPageSize(): Int
    fun setTreeMaxPageSize(treeMaxPageSize: Int)
    fun isGcEnabled(): Boolean
    fun setGcEnabled(enabled: Boolean)
    fun isGcSuspended(): Boolean
    fun getGcStartIn(): Int
    fun getGcMinUtilization(): Int
    fun setGcMinUtilization(percent: Int)
    fun isGcRenameFiles(): Boolean
    fun setGcRenameFiles(rename: Boolean)
    fun getGcFileMinAge(): Int
    fun setGcFileMinAge(minAge: Int)
    fun getGcRunPeriod(): Int
    fun setGcRunPeriod(runPeriod: Int)
    fun isGcUtilizationFromScratch(): Boolean
    fun setGcUtilizationFromScratch(fromScratch: Boolean)
    fun getGcUtilizationFromFile(): String?
    fun setGcUtilizationFromFile(file: String?)
    fun isGcUseExclusiveTransaction(): Boolean
    fun setGcUseExclusiveTransaction(useExclusiveTransaction: Boolean)
    fun getGcTransactionAcquireTimeout(): Int
    fun setGcTransactionAcquireTimeout(timeout: Int)
    fun getGcTransactionTimeout(): Int
    fun setGcTransactionTimeout(timeout: Int)
    fun getGcFilesDeletionDelay(): Int
    fun setGcFilesDeletionDelay(delay: Int)
    fun getGcRunEvery(): Int
    fun setGcRunEvery(seconds: Int)
    fun close()
    fun gc()
}
