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
package jetbrains.exodus.env

import jetbrains.exodus.management.Statistics
import jetbrains.exodus.management.StatisticsItem

class EnvironmentStatistics internal constructor(private val env: EnvironmentImpl) :
    Statistics<EnvironmentStatistics.Type?>(
        Type.values()
    ) {
    enum class Type(val id: String) {
        BYTES_WRITTEN("Bytes written"),
        BYTES_READ("Bytes read"),
        BYTES_MOVED_BY_GC("Bytes moved by GC"),
        TRANSACTIONS("Transactions"),
        READONLY_TRANSACTIONS("Read-only transactions"),
        GC_TRANSACTIONS("GC transactions"),
        ACTIVE_TRANSACTIONS("Active transactions"),
        FLUSHED_TRANSACTIONS("Flushed transactions"),
        TRANSACTIONS_DURATION("Transactions duration"),
        READONLY_TRANSACTIONS_DURATION("Read-only transactions duration"),
        GC_TRANSACTIONS_DURATION("GC transactions duration"),
        DISK_USAGE("Disk usage"),
        UTILIZATION_PERCENT("Utilization percent")
    }

    init {
        createAllStatisticsItems()
        getStatisticsItem(Type.BYTES_WRITTEN).total = env.log.getHighAddress()
        env.log.addReadBytesListener { _: ByteArray, count: Int ->
            getStatisticsItem(Type.BYTES_READ).addTotal(
                count.toLong()
            )
        }
    }

    override fun createStatisticsItem(key: Type): StatisticsItem {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        return if (!env.environmentConfig.envGatherStatistics) {
            StatisticsItem(this)
        } else super.createStatisticsItem(key)
    }

    override fun getStatisticsItem(statisticsName: String): StatisticsItem {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        return if (!env.environmentConfig.envGatherStatistics) {
            StatisticsItem(this)
        } else super.getStatisticsItem(statisticsName)
    }

    override fun createNewBuiltInItem(key: Type): StatisticsItem {
        return when (key) {
            Type.ACTIVE_TRANSACTIONS -> ActiveTransactionsStatisticsItem(this)
            Type.DISK_USAGE -> DiskUsageStatisticsItem(this)
            Type.UTILIZATION_PERCENT -> UtilizationPercentStatisticsItem(this)
            else -> super.createNewBuiltInItem(key)
        }
    }

    private class ActiveTransactionsStatisticsItem(statistics: EnvironmentStatistics) :
        StatisticsItem(statistics) {
        override fun getAutoUpdatedTotal(): Long? {
            val statistics = statistics as EnvironmentStatistics?
            return statistics?.env?.activeTransactions()?.toLong()
        }
    }

    private class DiskUsageStatisticsItem(statistics: EnvironmentStatistics) :
        StatisticsItem(statistics) {
        private var lastAutoUpdateTime: Long = 0
        override fun getAutoUpdatedTotal(): Long? {
            val statistics = statistics as EnvironmentStatistics?
            if (statistics != null) {
                val currentTime = System.currentTimeMillis()
                if (currentTime - lastAutoUpdateTime > DISK_USAGE_FREQ) {
                    lastAutoUpdateTime = currentTime
                    return statistics.env.diskUsage
                }
            }
            return null
        }
    }

    private class UtilizationPercentStatisticsItem(statistics: EnvironmentStatistics) :
        StatisticsItem(statistics) {
        override fun getAutoUpdatedTotal(): Long? {
            val statistics = statistics as EnvironmentStatistics?
            return statistics?.env?.gc?.utilizationProfile?.totalUtilizationPercent()?.toLong()
        }
    }

    companion object {
        private const val DISK_USAGE_FREQ = 10000 // calculate disk usage not more often than each 10 seconds
    }
}
