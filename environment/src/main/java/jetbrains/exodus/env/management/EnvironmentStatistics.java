/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.EnvironmentStatistics.Type;
import jetbrains.exodus.management.MBeanBase;
import jetbrains.exodus.management.Statistics;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.*;

public class EnvironmentStatistics extends MBeanBase implements EnvironmentStatisticsMBean {

    @NotNull
    private final EnvironmentImpl env;
    @NotNull
    private final Statistics<Type> statistics;

    @SuppressWarnings("unchecked")
    public EnvironmentStatistics(@NotNull final EnvironmentImpl env) {
        super(getObjectName(env));
        this.env = env;
        statistics = env.getStatistics();
    }

    static String getObjectName(@NotNull final Environment env) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(env.getLocation());
    }

    @Override
    public long getBytesWritten() {
        return getTotal(BYTES_WRITTEN);
    }

    @Override
    public double getBytesWrittenPerSecond() {
        return getMean(BYTES_WRITTEN);
    }

    @Override
    public long getBytesRead() {
        return getTotal(BYTES_READ);
    }

    @Override
    public double getBytesReadPerSecond() {
        return getMean(BYTES_READ);
    }

    @Override
    public long getBytesMovedByGC() {
        return getTotal(BYTES_MOVED_BY_GC);
    }

    @Override
    public double getBytesMovedByGCPerSecond() {
        return getMean(BYTES_MOVED_BY_GC);
    }

    @Override
    public String getLogCacheHitRate() {
        return ObjectCacheBase.formatHitRate((float) getMean(LOG_CACHE_HIT_RATE));
    }

    @Override
    public long getNumberOfTransactions() {
        return getTotal(TRANSACTIONS);
    }

    @Override
    public double getNumberOfTransactionsPerSecond() {
        return getMean(TRANSACTIONS);
    }

    @Override
    public long getNumberOfReadonlyTransactions() {
        return getTotal(READONLY_TRANSACTIONS);
    }

    @Override
    public double getNumberOfReadonlyTransactionsPerSecond() {
        return getMean(READONLY_TRANSACTIONS);
    }

    @Override
    public int getActiveTransactions() {
        return (int) getTotal(ACTIVE_TRANSACTIONS);
    }

    @Override
    public long getNumberOfFlushedTransactions() {
        return getTotal(FLUSHED_TRANSACTIONS);
    }

    @Override
    public double getNumberOfFlushedTransactionsPerSecond() {
        return getMean(FLUSHED_TRANSACTIONS);
    }

    @Override
    public long getDiskUsage() {
        return getTotal(DISK_USAGE);
    }

    @Override
    public int getUtilizationPercent() {
        return (int) getTotal(UTILIZATION_PERCENT);
    }

    @Override
    public String getStoreGetCacheHitRate() {
        return ObjectCacheBase.formatHitRate((float) getMean(STORE_GET_CACHE_HIT_RATE));
    }

    @Override
    public String getStuckTransactionMonitorMessage() {
        return env.getStuckTransactionMonitorMessage();
    }

    private long getTotal(@NotNull final Type statisticsName) {
        return statistics.getStatisticsItem(statisticsName).getTotal();
    }

    private double getMean(@NotNull final Type statisticsName) {
        return statistics.getStatisticsItem(statisticsName).getMean();
    }
}
