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
import jetbrains.exodus.env.EnvironmentStatistics
import jetbrains.exodus.management.MBeanBase

class EnvironmentStatistics(private val env: EnvironmentImpl) : MBeanBase(getObjectName(env)),
    EnvironmentStatisticsMBean {

    private fun getStatistics() = env.statistics

    override fun getBytesWritten(): Long = getTotal(EnvironmentStatistics.Type.BYTES_WRITTEN)

    override fun getBytesWrittenPerSecond(): Double = getMean(EnvironmentStatistics.Type.BYTES_WRITTEN)

    override fun getBytesRead(): Long = getTotal(EnvironmentStatistics.Type.BYTES_READ)

    override fun getBytesReadPerSecond(): Double = getMean(EnvironmentStatistics.Type.BYTES_READ)

    override fun getBytesMovedByGC(): Long = getTotal(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC)

    override fun getBytesMovedByGCPerSecond(): Double = getMean(EnvironmentStatistics.Type.BYTES_MOVED_BY_GC)

    override fun getLogCacheHitRate(): Float = env.log.getCacheHitRate()

    override fun getNumberOfTransactions(): Long = getTotal(EnvironmentStatistics.Type.TRANSACTIONS)

    override fun getNumberOfTransactionsPerSecond(): Double = getMean(EnvironmentStatistics.Type.TRANSACTIONS)

    override fun getNumberOfReadonlyTransactions(): Long = getTotal(EnvironmentStatistics.Type.READONLY_TRANSACTIONS)

    override fun getNumberOfReadonlyTransactionsPerSecond(): Double =
        getMean(EnvironmentStatistics.Type.READONLY_TRANSACTIONS)

    override fun getNumberOfGCTransactions(): Long = getTotal(EnvironmentStatistics.Type.GC_TRANSACTIONS)

    override fun getNumberOfGCTransactionsPerSecond(): Double = getMean(EnvironmentStatistics.Type.GC_TRANSACTIONS)

    override fun getActiveTransactions(): Int = getTotal(EnvironmentStatistics.Type.ACTIVE_TRANSACTIONS).toInt()

    override fun getNumberOfFlushedTransactions(): Long = getTotal(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS)

    override fun getNumberOfFlushedTransactionsPerSecond(): Double =
        getMean(EnvironmentStatistics.Type.FLUSHED_TRANSACTIONS)

    override fun getTransactionsDuration(): Long = getTotal(EnvironmentStatistics.Type.TRANSACTIONS_DURATION)

    override fun getReadonlyTransactionsDuration(): Long =
        getTotal(EnvironmentStatistics.Type.READONLY_TRANSACTIONS_DURATION)

    override fun getGcTransactionsDuration(): Long = getTotal(EnvironmentStatistics.Type.GC_TRANSACTIONS_DURATION)

    override fun getDiskUsage(): Long = getTotal(EnvironmentStatistics.Type.DISK_USAGE)

    override fun getUtilizationPercent(): Int = getTotal(EnvironmentStatistics.Type.UTILIZATION_PERCENT).toInt()

    override fun getStoreGetCacheHitRate(): Float = env.getStoreGetCacheHitRate()

    override fun getStuckTransactionCount(): Int = env.getStuckTransactionCount()

    private fun getTotal(statisticsName: EnvironmentStatistics.Type): Long {
        return getStatistics().getStatisticsItem(statisticsName).total
    }

    private fun getMean(statisticsName: EnvironmentStatistics.Type): Double {
        return getStatistics().getStatisticsItem(statisticsName).mean
    }

    companion object {
        internal fun getObjectName(env: Environment) =
            "$STATISTICS_OBJECT_NAME_PREFIX, location=${escapeLocation(env.location)}"
    }

}
