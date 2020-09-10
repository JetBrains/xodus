/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.env.management

const val CONFIG_OBJECT_NAME_PREFIX = "jetbrains.exodus.env: type=EnvironmentConfig"

interface EnvironmentConfigMBean {
    val memoryUsage: Long
    val memoryUsagePercent: Int
    var logDurableWrite: Boolean
    val logFileSize: Long
    val logLockTimeout: Long
    val logCachePageSize: Int
    val logCacheOpenFilesCount: Int
    val logCacheUseNio: Boolean
    val logCacheFreePhysicalMemoryThreshold: Long
    val isLogCacheShared: Boolean
    val isLogCacheNonBlocking: Boolean
    val logCacheGenerationCount: Int
    var logCacheReadAheadMultiple: Int
    val isLogCleanDirectoryExpected: Boolean
    val isLogClearInvalid: Boolean
    var logSyncPeriod: Long
    val isLogFullFileReadonly: Boolean
    var envIsReadonly: Boolean
    var envFailFastInReadonly: Boolean
    var envReadonlyEmptyStores: Boolean
    var envStoreGetCacheSize: Int
    var envStoreGetCacheMinTreeSize: Int
    var envStoreGetCacheMaxValueSize: Int
    var envCloseForcedly: Boolean
    var envTxnReplayTimeout: Long
    var envTxnReplayMaxCount: Int
    var envTxnDowngradeAfterFlush: Boolean
    var envTxnSingleThreadWrites: Boolean
    val envMaxParallelTxns: Int
    val envMaxParallelReadonlyTxns: Int
    val envMonitorTxnsTimeout: Int
    val envMonitorTxnsCheckFreq: Int
    val envGatherStatistics: Boolean
    var treeMaxPageSize: Int
    var isGcEnabled: Boolean
    val isGcSuspended: Boolean
    val gcStartIn: Int
    var gcMinUtilization: Int
    var gcRenameFiles: Boolean
    var gcFileMinAge: Int
    var gcFilesInterval: Int
    var gcRunPeriod: Int
    var gcUtilizationFromScratch: Boolean
    var gcUtilizationFromFile: String?
    var gcUseExclusiveTransaction: Boolean
    var gcTransactionAcquireTimeout: Int
    var gcTransactionTimeout: Int
    var gcFilesDeletionDelay: Int
    var gcRunEvery: Int

    fun close()
    fun gc()
}
