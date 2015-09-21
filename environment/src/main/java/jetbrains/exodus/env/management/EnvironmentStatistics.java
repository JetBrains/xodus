/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.management.MBeanBase;
import jetbrains.exodus.management.Statistics;
import org.jetbrains.annotations.NotNull;

public class EnvironmentStatistics extends MBeanBase implements EnvironmentStatisticsMBean {

    @NotNull
    private final Statistics statistics;

    public EnvironmentStatistics(@NotNull final Environment env) {
        super(getObjectName(env));
        statistics = env.getStatistics();
    }

    public static String getObjectName(@NotNull final Environment env) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(env.getLocation());
    }

    @Override
    public long getBytesWritten() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_WRITTEN).getTotal();
    }

    @Override
    public double getBytesWrittenPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_WRITTEN).getMean();
    }

    @Override
    public long getBytesRead() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_READ).getTotal();
    }

    @Override
    public double getBytesReadPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_READ).getMean();
    }

    @Override
    public long getBytesMovedByGC() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_MOVED_BY_GC).getTotal();
    }

    @Override
    public double getBytesMovedByGCPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.BYTES_MOVED_BY_GC).getMean();
    }

    @Override
    public long getNumberOfTransactions() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.TRANSACTIONS).getTotal();
    }

    @Override
    public double getNumberOfTransactionsPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.TRANSACTIONS).getMean();
    }

    @Override
    public long getNumberOfReadonlyTransactions() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.READONLY_TRANSACTIONS).getTotal();
    }

    @Override
    public double getNumberOfReadonlyTransactionsPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.READONLY_TRANSACTIONS).getMean();
    }

    @Override
    public int getActiveTransaction() {
        return (int) statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.ACTIVE_TRANSACTIONS).getTotal();
    }

    @Override
    public long getNumberOfFlushedTransactions() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.FLUSHED_TRANSACTIONS).getTotal();
    }

    @Override
    public double getNumberOfFlushedTransactionsPerSecond() {
        return statistics.getStatisticsItem(jetbrains.exodus.env.EnvironmentStatistics.FLUSHED_TRANSACTIONS).getMean();
    }
}
