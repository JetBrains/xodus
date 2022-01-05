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
import jetbrains.exodus.env.EnvironmentStatistics
import jetbrains.exodus.management.MBeanBase

class EnvironmentStatistics(private val env: EnvironmentImpl) : MBeanBase(getObjectName(env)), EnvironmentStatisticsMBean {

    private val statistics = env.statistics

    override val bytesWritten: Long
        get() = getTotal(EnvironmentStatistics.Type.BYTES_WRITTEN)

    override val bytesWrittenPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.BYTES_WRITTEN)

    override val bytesRead: Long
        get() = getTotal(EnvironmentStatistics.Type.BYTES_READ)

    override val bytesReadPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.BYTES_READ)

    override val bytesMovedByGC: Long
        get() = getTotal(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC)

    override val bytesMovedByGCPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC)

    override val logCacheHitRate: Float
        get() = env.log.cacheHitRate

    override val numberOfTransactions: Long
        get() = getTotal(EnvironmentStatistics.Type.TRANSACTIONS)

    override val numberOfTransactionsPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.TRANSACTIONS)

    override val numberOfReadonlyTransactions: Long
        get() = getTotal(EnvironmentStatistics.Type.READONLY_TRANSACTIONS)

    override val numberOfReadonlyTransactionsPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.READONLY_TRANSACTIONS)

    override val numberOfGCTransactions: Long
        get() = getTotal(EnvironmentStatistics.Type.GC_TRANSACTIONS)

    override val numberOfGCTransactionsPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.GC_TRANSACTIONS)

    override val activeTransactions: Int
        get() = getTotal(EnvironmentStatistics.Type.ACTIVE_TRANSACTIONS).toInt()

    override val numberOfFlushedTransactions: Long
        get() = getTotal(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS)

    override val numberOfFlushedTransactionsPerSecond: Double
        get() = getMean(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS)

    override val transactionsDuration: Long
        get() = getTotal(EnvironmentStatistics.Type.TRANSACTIONS_DURATION)

    override val readonlyTransactionsDuration: Long
        get() = getTotal(EnvironmentStatistics.Type.READONLY_TRANSACTIONS_DURATION)

    override val gcTransactionsDuration: Long
        get() = getTotal(EnvironmentStatistics.Type.GC_TRANSACTIONS_DURATION)

    override val diskUsage: Long
        get() = getTotal(EnvironmentStatistics.Type.DISK_USAGE)

    override val utilizationPercent: Int
        get() = getTotal(EnvironmentStatistics.Type.UTILIZATION_PERCENT).toInt()

    override val storeGetCacheHitRate: Float
        get() = env.storeGetCacheHitRate

    override val stuckTransactionCount: Int
        get() = env.stuckTransactionCount

    private fun getTotal(statisticsName: EnvironmentStatistics.Type): Long {
        return statistics.getStatisticsItem(statisticsName).total
    }

    private fun getMean(statisticsName: EnvironmentStatistics.Type): Double {
        return statistics.getStatisticsItem(statisticsName).mean
    }

    companion object {
        internal fun getObjectName(env: Environment) =
                "$STATISTICS_OBJECT_NAME_PREFIX, location=${escapeLocation(env.location)}"
    }

}
