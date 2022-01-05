/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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

open class EnvironmentConfig(protected val env: EnvironmentImpl) : MBeanBase(getObjectName(env)), EnvironmentConfigMBean {

    private val config = env.environmentConfig

    override val memoryUsage: Long
        get() = config.memoryUsage

    override val memoryUsagePercent: Int
        get() = config.memoryUsagePercentage

    override var logDurableWrite: Boolean
        get() = config.logDurableWrite
        set(durableWrite) {
            config.logDurableWrite = durableWrite
        }

    override val logFileSize: Long
        get() = config.logFileSize

    override val logLockTimeout: Long
        get() = config.logLockTimeout

    override val logCachePageSize: Int
        get() = config.logCachePageSize

    override val logCacheOpenFilesCount: Int
        get() = config.logCacheOpenFilesCount

    override val logCacheUseNio: Boolean
        get() = config.logCacheUseNio

    override val logCacheFreePhysicalMemoryThreshold: Long
        get() = config.logCacheFreePhysicalMemoryThreshold

    override val isLogCacheShared: Boolean
        get() = config.isLogCacheShared

    override val isLogCacheNonBlocking: Boolean
        get() = config.isLogCacheNonBlocking

    override val logCacheGenerationCount: Int
        get() = config.logCacheGenerationCount

    override var logCacheReadAheadMultiple: Int
        get() = config.logCacheReadAheadMultiple
        set(readAheadMultiple) {
            config.logCacheReadAheadMultiple = readAheadMultiple
        }

    override val isLogCleanDirectoryExpected: Boolean
        get() = config.isLogCleanDirectoryExpected

    override val isLogClearInvalid: Boolean
        get() = config.isLogClearInvalid

    override var logSyncPeriod: Long
        get() = config.logSyncPeriod
        set(millis) {
            config.logSyncPeriod = millis
        }

    override val isLogFullFileReadonly: Boolean
        get() = config.isLogFullFileReadonly

    override val isLogAllowRemovable: Boolean
        get() = config.isLogAllowRemovable

    override val isLogAllowRemote: Boolean
        get() = config.isLogAllowRemote

    override val isLogAllowRamDisk: Boolean
        get() = config.isLogAllowRamDisk

    override var envIsReadonly: Boolean
        get() = config.envIsReadonly
        set(isReadonly) {
            config.envIsReadonly = isReadonly
        }

    override var envFailFastInReadonly: Boolean
        get() = config.envFailFastInReadonly
        set(failFast) {
            config.envFailFastInReadonly = failFast
        }

    override var envReadonlyEmptyStores: Boolean
        get() = config.envReadonlyEmptyStores
        set(readonlyEmptyStores) {
            config.envReadonlyEmptyStores = readonlyEmptyStores
        }

    override var envStoreGetCacheSize: Int
        get() = config.envStoreGetCacheSize
        set(storeGetCacheSize) {
            config.envStoreGetCacheSize = storeGetCacheSize
        }

    override var envStoreGetCacheMinTreeSize: Int
        get() = config.envStoreGetCacheMinTreeSize
        set(minTreeSize) {
            config.envStoreGetCacheMinTreeSize = minTreeSize
        }

    override var envStoreGetCacheMaxValueSize: Int
        get() = config.envStoreGetCacheMaxValueSize
        set(maxValueSize) {
            config.envStoreGetCacheMaxValueSize = maxValueSize
        }

    override var envCloseForcedly: Boolean
        get() = config.envCloseForcedly
        set(closeForcedly) {
            config.envCloseForcedly = closeForcedly
        }

    override var envTxnReplayTimeout: Long
        get() = config.envTxnReplayTimeout
        set(txnReplayTimeout) {
            config.envTxnReplayTimeout = txnReplayTimeout
        }

    override var envTxnReplayMaxCount: Int
        get() = config.envTxnReplayMaxCount
        set(txnReplayMaxCount) {
            config.envTxnReplayMaxCount = txnReplayMaxCount
        }

    override var envTxnDowngradeAfterFlush: Boolean
        get() = config.envTxnDowngradeAfterFlush
        set(downgrade) {
            config.envTxnDowngradeAfterFlush = downgrade
        }

    override var envTxnSingleThreadWrites: Boolean
        get() = config.envTxnSingleThreadWrites
        set(singleThreadWrites) {
            config.envTxnSingleThreadWrites = singleThreadWrites
        }

    override var envTxnTraceFinish: Boolean
        get() = config.isEnvTxnTraceFinish
        set(traceFinish) {
            config.isEnvTxnTraceFinish = traceFinish
        }

    override val envMaxParallelTxns: Int
        get() = config.envMaxParallelTxns

    override val envMonitorTxnsTimeout: Int
        get() = config.envMonitorTxnsTimeout

    override val envMonitorTxnsCheckFreq: Int
        get() = config.envMonitorTxnsCheckFreq

    override val envGatherStatistics: Boolean
        get() = config.envGatherStatistics

    override var treeMaxPageSize: Int
        get() = config.treeMaxPageSize
        set(treeMaxPageSize) {
            config.treeMaxPageSize = treeMaxPageSize
        }

    override var isGcEnabled: Boolean
        get() = config.isGcEnabled
        set(enabled) {
            config.isGcEnabled = enabled
        }

    override val isGcSuspended: Boolean
        get() = env.gc.isSuspended

    override val gcStartIn: Int
        get() = config.gcStartIn

    override var gcMinUtilization: Int
        get() = config.gcMinUtilization
        set(percent) {
            config.gcMinUtilization = percent
        }

    override var gcRenameFiles: Boolean
        get() = config.gcRenameFiles
        set(rename) {
            config.gcRenameFiles = rename
        }

    override var gcFileMinAge: Int
        get() = config.gcFileMinAge
        set(minAge) {
            config.gcFileMinAge = minAge
        }

    override var gcFilesInterval: Int
        get() = config.gcFilesInterval
        set(files) {
            config.gcFilesInterval = files
        }

    override var gcRunPeriod: Int
        get() = config.gcRunPeriod
        set(runPeriod) {
            config.gcRunPeriod = runPeriod
        }

    override var gcUtilizationFromScratch: Boolean
        get() = config.gcUtilizationFromScratch
        set(fromScratch) {
            config.gcUtilizationFromScratch = fromScratch
        }

    override var gcUtilizationFromFile: String?
        get() = config.gcUtilizationFromFile
        set(file) {
            config.gcUtilizationFromFile = file
        }

    override var gcUseExclusiveTransaction: Boolean
        get() = config.gcUseExclusiveTransaction
        set(useExclusiveTransaction) {
            config.gcUseExclusiveTransaction = useExclusiveTransaction
        }

    override var gcTransactionAcquireTimeout: Int
        get() = config.gcTransactionAcquireTimeout
        set(timeout) {
            config.gcTransactionAcquireTimeout = timeout
        }

    override var gcTransactionTimeout: Int
        get() = config.gcTransactionTimeout
        set(timeout) {
            config.gcTransactionTimeout = timeout
        }

    override var gcFilesDeletionDelay: Int
        get() = config.gcFilesDeletionDelay
        set(delay) {
            config.gcFilesDeletionDelay = delay
        }

    override var gcRunEvery: Int
        get() = config.gcRunEvery
        set(seconds) {
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
