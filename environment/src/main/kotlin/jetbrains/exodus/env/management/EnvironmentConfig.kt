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

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.management.MBeanBase

open class EnvironmentConfig(@JvmField protected val env: EnvironmentImpl) : MBeanBase(getObjectName(env)),
    EnvironmentConfigMBean {
    private val config = env.environmentConfig
    override fun getMemoryUsage(): Long = config.memoryUsage
    override fun getMemoryUsagePercent(): Int = config.memoryUsagePercentage
    override fun getLogDurableWrite(): Boolean = config.logDurableWrite
    override fun setLogDurableWrite(durableWrite: Boolean) {
        config.logDurableWrite = durableWrite
    }

    override fun getLogFileSize(): Long = config.logFileSize
    override fun getLogLockTimeout(): Long = config.logLockTimeout
    override fun getLogCachePageSize(): Int = config.logCachePageSize
    override fun getLogCacheOpenFilesCount(): Int = config.logCacheOpenFilesCount
    override fun getLogCacheFreePhysicalMemoryThreshold(): Long = config.logCacheFreePhysicalMemoryThreshold
    override fun isLogCacheShared(): Boolean = config.isLogCacheShared
    override fun isLogCacheNonBlocking(): Boolean = config.isLogCacheNonBlocking
    override fun getLogCacheGenerationCount(): Int = config.logCacheGenerationCount
    override fun getLogCacheReadAheadMultiple(): Int = config.logCacheReadAheadMultiple
    override fun setLogCacheReadAheadMultiple(readAheadMultiple: Int) {
        config.logCacheReadAheadMultiple = readAheadMultiple
    }

    override fun isLogCleanDirectoryExpected(): Boolean = config.isLogCleanDirectoryExpected
    override fun isLogClearInvalid(): Boolean = config.isLogClearInvalid
    override fun getLogSyncPeriod(): Long = config.logSyncPeriod
    override fun setLogSyncPeriod(syncPeriod: Long) {
        config.logSyncPeriod = syncPeriod
    }

    override fun isLogFullFileReadonly(): Boolean = config.isLogFullFileReadonly
    override fun isLogAllowRemovable(): Boolean = config.isLogAllowRemovable
    override fun isLogAllowRemote(): Boolean = config.isLogAllowRemote
    override fun isLogAllowRamDisk(): Boolean = config.isLogAllowRamDisk
    override fun isEnvIsReadonly(): Boolean = config.envIsReadonly
    override fun setEnvIsReadonly(readOnly: Boolean) {
        config.envIsReadonly = readOnly
    }

    override fun isEnvFailFastInReadonly(): Boolean = config.envFailFastInReadonly
    override fun setEnvFailFastInReadonly(failFast: Boolean) {
        config.envFailFastInReadonly = failFast
    }

    override fun isEnvReadonlyEmptyStores(): Boolean = config.envReadonlyEmptyStores
    override fun setEnvReadonlyEmptyStores(readOnly: Boolean) {
        config.envReadonlyEmptyStores = readOnly
    }

    override fun getEnvStoreGetCacheSize(): Int = config.envStoreGetCacheSize
    override fun setEnvStoreGetCacheSize(cacheSize: Int) {
        config.envStoreGetCacheSize = cacheSize
    }

    override fun getEnvStoreGetCacheMinTreeSize(): Int = config.envStoreGetCacheMinTreeSize
    override fun setEnvStoreGetCacheMinTreeSize(minTreeSize: Int) {
        config.envStoreGetCacheMinTreeSize = minTreeSize
    }

    override fun getEnvStoreGetCacheMaxValueSize(): Int = config.envStoreGetCacheMaxValueSize
    override fun setEnvStoreGetCacheMaxValueSize(maxValueSize: Int) {
        config.envStoreGetCacheMaxValueSize = maxValueSize
    }

    override fun isEnvCloseForcedly(): Boolean = config.envCloseForcedly
    override fun setEnvCloseForcedly(closeForcedly: Boolean) {
        config.envCloseForcedly = closeForcedly
    }

    override fun getEnvTxnReplayTimeout(): Long = config.envTxnReplayTimeout
    override fun setEnvTxnReplayTimeout(txnReplayTimeout: Long) {
        config.envTxnReplayTimeout = txnReplayTimeout
    }

    override fun getEnvTxnReplayMaxCount(): Int = config.envTxnReplayMaxCount
    override fun setEnvTxnReplayMaxCount(txnReplayMaxCount: Int) {
        config.envTxnReplayMaxCount = txnReplayMaxCount
    }

    override fun isEnvTxnDowngradeAfterFlush(): Boolean = config.envTxnDowngradeAfterFlush
    override fun setEnvTxnDowngradeAfterFlush(downgrade: Boolean) {
        config.envTxnDowngradeAfterFlush = downgrade
    }

    override fun isEnvTxnSingleThreadWrites(): Boolean = config.envTxnSingleThreadWrites
    override fun setEnvTxnSingleThreadWrites(singleThreadWrites: Boolean) {
        config.envTxnSingleThreadWrites = singleThreadWrites
    }

    override fun isEnvTxnTraceFinish(): Boolean = config.isEnvTxnTraceFinish
    override fun setEnvTxnTraceFinish(traceFinish: Boolean) {
        config.isEnvTxnTraceFinish = traceFinish
    }

    override fun getEnvMaxParallelTxns(): Int = config.envMaxParallelTxns

    override fun getEnvMonitorTxnsTimeout(): Int = config.envMonitorTxnsTimeout

    override fun getEnvMonitorTxnsCheckFreq(): Int = config.envMonitorTxnsCheckFreq

    override fun isEnvGatherStatistics(): Boolean = config.envGatherStatistics

    override fun getTreeMaxPageSize(): Int = config.treeMaxPageSize

    override fun setTreeMaxPageSize(treeMaxPageSize: Int) {
        config.treeMaxPageSize = treeMaxPageSize
    }

    override fun isGcEnabled(): Boolean = config.isGcEnabled

    override fun setGcEnabled(enabled: Boolean) {
        config.isGcEnabled = enabled
    }

    override fun isGcSuspended(): Boolean = env.gc.isSuspended

    override fun getGcStartIn(): Int = config.gcStartIn

    override fun getGcMinUtilization(): Int = config.gcMinUtilization

    override fun setGcMinUtilization(percent: Int) {
        config.gcMinUtilization = percent
    }

    override fun isGcRenameFiles(): Boolean = config.gcRenameFiles

    override fun setGcRenameFiles(rename: Boolean) {
        config.gcRenameFiles = rename
    }

    override fun getGcFileMinAge(): Int = config.gcFileMinAge

    override fun setGcFileMinAge(minAge: Int) {
        config.gcFileMinAge = minAge
    }

    override fun getGcRunPeriod(): Int = config.gcRunPeriod
    override fun setGcRunPeriod(runPeriod: Int) {
        config.gcRunPeriod = runPeriod
    }

    override fun isGcUtilizationFromScratch(): Boolean = config.gcUtilizationFromScratch
    override fun setGcUtilizationFromScratch(fromScratch: Boolean) {
        config.gcUtilizationFromScratch = fromScratch
    }

    override fun getGcUtilizationFromFile(): String? = config.gcUtilizationFromFile
    override fun setGcUtilizationFromFile(file: String?) {
        config.gcUtilizationFromFile = file
    }

    override fun isGcUseExclusiveTransaction(): Boolean = config.gcUseExclusiveTransaction
    override fun setGcUseExclusiveTransaction(useExclusiveTransaction: Boolean) {
        config.gcUseExclusiveTransaction = useExclusiveTransaction
    }

    override fun getGcTransactionAcquireTimeout(): Int = config.gcTransactionAcquireTimeout
    override fun setGcTransactionAcquireTimeout(timeout: Int) {
        config.gcTransactionAcquireTimeout = timeout
    }

    override fun getGcTransactionTimeout(): Int = config.gcTransactionTimeout
    override fun setGcTransactionTimeout(timeout: Int) {
        config.gcTransactionTimeout = timeout
    }

    override fun getGcFilesDeletionDelay(): Int = config.gcFilesDeletionDelay
    override fun setGcFilesDeletionDelay(delay: Int) {
        config.gcFilesDeletionDelay = delay
    }

    override fun getGcRunEvery(): Int = config.gcRunEvery
    override fun setGcRunEvery(seconds: Int) {
        config.gcRunEvery = seconds
    }


    override fun gc() {
        env.gc()
    }

    companion object {
        internal fun getObjectName(env: Environment) =
            "$CONFIG_OBJECT_NAME_PREFIX, location=${escapeLocation(env.location)}"
    }
}
