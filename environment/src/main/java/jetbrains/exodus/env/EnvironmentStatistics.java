/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.LongObjectCacheBase;
import jetbrains.exodus.log.ReadBytesListener;
import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.management.StatisticsItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EnvironmentStatistics extends Statistics {

    public static final String BYTES_WRITTEN = "Bytes written";
    public static final String BYTES_READ = "Bytes read";
    public static final String BYTES_MOVED_BY_GC = "Bytes moved by GC";
    public static final String TRANSACTIONS = "Transactions";
    public static final String READONLY_TRANSACTIONS = "Read-only transactions";
    public static final String ACTIVE_TRANSACTIONS = "Active transactions";
    public static final String FLUSHED_TRANSACTIONS = "Flushed transactions";
    public static final String DISK_USAGE = "Disk usage";
    public static final String UTILIZATION_PERCENT = "Utilization percent";
    public static final String LOG_CACHE_HIT_RATE = "Log cache hit rate";
    public static final String STORE_GET_CACHE_HIT_RATE = "StoreGet cache hit rate";
    public static final String TREE_NODES_CACHE_HIT_RATE = "Tree nodes cache hit rate";

    private static final int DISK_USAGE_FREQ = 10000; // calculate disk usage not more often than each 10 seconds

    @NotNull
    private final EnvironmentImpl env;

    EnvironmentStatistics(@NotNull final EnvironmentImpl env) {
        this.env = env;
        getStatisticsItem(BYTES_WRITTEN).setTotal(env.getLog().getHighAddress());
        getStatisticsItem(BYTES_READ);
        getStatisticsItem(BYTES_MOVED_BY_GC);
        getStatisticsItem(TRANSACTIONS);
        getStatisticsItem(READONLY_TRANSACTIONS);
        getStatisticsItem(ACTIVE_TRANSACTIONS);
        getStatisticsItem(FLUSHED_TRANSACTIONS);
        getStatisticsItem(DISK_USAGE);
        getStatisticsItem(UTILIZATION_PERCENT);
        getStatisticsItem(LOG_CACHE_HIT_RATE);
        getStatisticsItem(STORE_GET_CACHE_HIT_RATE);
        getStatisticsItem(TREE_NODES_CACHE_HIT_RATE);
        env.getLog().addReadBytesListener(new ReadBytesListener() {
            @Override
            public void bytesRead(final byte[] bytes, final int count) {
                getStatisticsItem(BYTES_READ).addTotal(count);
            }
        });
    }

    @NotNull
    @Override
    public StatisticsItem getStatisticsItem(@NotNull final String statisticsName) {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        if (!env.getEnvironmentConfig().getEnvGatherStatistics()) {
            return new StatisticsItem(this);
        }
        return super.getStatisticsItem(statisticsName);
    }

    @NotNull
    @Override
    protected StatisticsItem createNewItem(@NotNull final String statisticsName) {
        if (ACTIVE_TRANSACTIONS.equals(statisticsName)) {
            return new ActiveTransactionsStatisticsItem(this);
        }
        if (DISK_USAGE.equals(statisticsName)) {
            return new DiskUsageStatisticsItem(this);
        }
        if (UTILIZATION_PERCENT.equals(statisticsName)) {
            return new UtilizationPercentStatisticsItem(this);
        }
        if (LOG_CACHE_HIT_RATE.equals(statisticsName)) {
            return new LogCacheHitRateStatisticsItem(this);
        }
        if (STORE_GET_CACHE_HIT_RATE.equals(statisticsName)) {
            return new StoreGetCacheHitRateStatisticsItem(this);
        }
        if (TREE_NODES_CACHE_HIT_RATE.equals(statisticsName)) {
            return new TreeNodesCacheHitRateStatisticsItem(this);
        }
        return super.createNewItem(statisticsName);
    }

    private static class ActiveTransactionsStatisticsItem extends StatisticsItem {

        ActiveTransactionsStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            return statistics == null ? null : (long) (statistics.env.activeTransactions());
        }
    }

    private static class DiskUsageStatisticsItem extends StatisticsItem {

        private long lastAutoUpdateTime;

        DiskUsageStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
            lastAutoUpdateTime = 0;
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            if (statistics != null) {
                final long currentTime = System.currentTimeMillis();
                if (currentTime - lastAutoUpdateTime > DISK_USAGE_FREQ) {
                    lastAutoUpdateTime = currentTime;
                    return statistics.env.getDiskUsage();
                }
            }
            return null;
        }
    }

    private static class UtilizationPercentStatisticsItem extends StatisticsItem {

        UtilizationPercentStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            return statistics == null ? null : (long) (statistics.env.getGC().getUtilizationProfile().totalUtilizationPercent());
        }
    }

    private static class LogCacheHitRateStatisticsItem extends StatisticsItem {

        LogCacheHitRateStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
        }

        @Override
        public double getMean() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            return statistics == null ? 0 : statistics.env.getLog().getCacheHitRate();
        }
    }

    private static class StoreGetCacheHitRateStatisticsItem extends StatisticsItem {

        StoreGetCacheHitRateStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
        }

        @Override
        public double getMean() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            if (statistics == null) {
                return 0;
            }
            @Nullable final StoreGetCache storeGetCache = statistics.env.getStoreGetCache();
            return storeGetCache == null ? 0 : storeGetCache.hitRate();
        }
    }

    private static class TreeNodesCacheHitRateStatisticsItem extends StatisticsItem {

        TreeNodesCacheHitRateStatisticsItem(@NotNull final EnvironmentStatistics statistics) {
            super(statistics);
        }

        @Override
        public double getMean() {
            final EnvironmentStatistics statistics = (EnvironmentStatistics) getStatistics();
            if (statistics == null) {
                return 0;
            }
            final @Nullable LongObjectCacheBase treeNodesCache = statistics.env.getTreeNodesCache();
            return treeNodesCache == null ? 0 : treeNodesCache.hitRate();
        }
    }
}
