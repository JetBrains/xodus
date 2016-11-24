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
package jetbrains.exodus.env.management;

import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.management.MBeanBase;
import jetbrains.exodus.management.Statistics;
import org.jetbrains.annotations.NotNull;

public class EnvironmentStatistics extends MBeanBase implements EnvironmentStatisticsMBean {

    @NotNull
    private final Environment env;
    @NotNull
    private final Statistics statistics;

    public EnvironmentStatistics(@NotNull final Environment env) {
        super(getObjectName(env));
        this.env = env;
        statistics = env.getStatistics();
    }

    public static String getObjectName(@NotNull final Environment env) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(env.getLocation());
    }

    @Override
    public long getBytesWritten() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.BYTES_WRITTEN);
    }

    @Override
    public double getBytesWrittenPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.BYTES_WRITTEN);
    }

    @Override
    public long getBytesRead() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.BYTES_READ);
    }

    @Override
    public double getBytesReadPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.BYTES_READ);
    }

    @Override
    public long getBytesMovedByGC() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.BYTES_MOVED_BY_GC);
    }

    @Override
    public double getBytesMovedByGCPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.BYTES_MOVED_BY_GC);
    }

    @Override
    public String getLogCacheHitRate() {
        return ObjectCacheBase.formatHitRate((float) getMean(jetbrains.exodus.env.EnvironmentStatistics.LOG_CACHE_HIT_RATE));
    }

    @Override
    public long getNumberOfTransactions() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.TRANSACTIONS);
    }

    @Override
    public double getNumberOfTransactionsPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.TRANSACTIONS);
    }

    @Override
    public long getNumberOfReadonlyTransactions() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.READONLY_TRANSACTIONS);
    }

    @Override
    public double getNumberOfReadonlyTransactionsPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.READONLY_TRANSACTIONS);
    }

    @Override
    public int getActiveTransactions() {
        return (int) getTotal(jetbrains.exodus.env.EnvironmentStatistics.ACTIVE_TRANSACTIONS);
    }

    @Override
    public long getNumberOfFlushedTransactions() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.FLUSHED_TRANSACTIONS);
    }

    @Override
    public double getNumberOfFlushedTransactionsPerSecond() {
        return getMean(jetbrains.exodus.env.EnvironmentStatistics.FLUSHED_TRANSACTIONS);
    }

    @Override
    public long getDiskUsage() {
        return getTotal(jetbrains.exodus.env.EnvironmentStatistics.DISK_USAGE);
    }

    @Override
    public int getUtilizationPercent() {
        return (int) getTotal(jetbrains.exodus.env.EnvironmentStatistics.UTILIZATION_PERCENT);
    }

    @Override
    public String getStoreGetCacheHitRate() {
        return ObjectCacheBase.formatHitRate((float) getMean(jetbrains.exodus.env.EnvironmentStatistics.STORE_GET_CACHE_HIT_RATE));
    }

    @Override
    public String getTreeNodesCacheHitRate() {
        return ObjectCacheBase.formatHitRate((float) getMean(jetbrains.exodus.env.EnvironmentStatistics.TREE_NODES_CACHE_HIT_RATE));
    }

    private long getTotal(@NotNull final String statisticsName) {
        return statistics.getStatisticsItem(statisticsName).getTotal();
    }

    private double getMean(@NotNull final String statisticsName) {
        return statistics.getStatisticsItem(statisticsName).getMean();
    }
}
