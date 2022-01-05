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
package jetbrains.exodus.env;

import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.management.StatisticsItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.BYTES_READ;
import static jetbrains.exodus.env.EnvironmentStatistics.Type.BYTES_WRITTEN;

public class EnvironmentStatistics extends Statistics<EnvironmentStatistics.Type> {

    public enum Type {
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
        UTILIZATION_PERCENT("Utilization percent");

        public final String id;

        Type(String id) {
            this.id = id;
        }
    }

    private static final int DISK_USAGE_FREQ = 10000; // calculate disk usage not more often than each 10 seconds

    @NotNull
    private final EnvironmentImpl env;

    EnvironmentStatistics(@NotNull final EnvironmentImpl env) {
        super(Type.values());
        this.env = env;

        createAllStatisticsItems();
        getStatisticsItem(BYTES_WRITTEN).setTotal(env.getLog().getHighAddress());

        env.getLog().addReadBytesListener((bytes, count) -> getStatisticsItem(BYTES_READ).addTotal(count));
    }

    @Override
    protected StatisticsItem createStatisticsItem(@NotNull Type key) {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        if (!env.getEnvironmentConfig().getEnvGatherStatistics()) {
            return new StatisticsItem(this);
        }
        return super.createStatisticsItem(key);
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
    protected StatisticsItem createNewBuiltInItem(@NotNull final Type key) {
        switch (key) {
            case ACTIVE_TRANSACTIONS:
                return new ActiveTransactionsStatisticsItem(this);
            case DISK_USAGE:
                return new DiskUsageStatisticsItem(this);
            case UTILIZATION_PERCENT:
                return new UtilizationPercentStatisticsItem(this);
            default:
                return super.createNewBuiltInItem(key);
        }
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
}
