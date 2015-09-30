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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.management.StatisticsItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PersistentEntityStoreStatistics extends Statistics {

    public static final String BLOBS_DISK_USAGE = "Blobs disk usage";
    public static final String CACHING_JOBS = "Caching jobs";

    @NotNull
    private final PersistentEntityStoreImpl store;

    PersistentEntityStoreStatistics(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
        getStatisticsItem(BLOBS_DISK_USAGE);
        getStatisticsItem(CACHING_JOBS);
    }

    @NotNull
    @Override
    public StatisticsItem getStatisticsItem(@NotNull final String statisticsName) {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        if (!store.getConfig().getGatherStatistics()) {
            return new StatisticsItem(this, statisticsName);
        }
        return super.getStatisticsItem(statisticsName);
    }

    @NotNull
    @Override
    protected StatisticsItem createNewItem(@NotNull final String statisticsName) {
        if (BLOBS_DISK_USAGE.equals(statisticsName)) {
            return new BlobsDiskUsageStatisticsItem(this, statisticsName);
        }
        if (CACHING_JOBS.equals(statisticsName)) {
            return new CachingJobsStatisticsItem(this, statisticsName);
        }
        return super.createNewItem(statisticsName);
    }

    private static class BlobsDiskUsageStatisticsItem extends StatisticsItem {

        public BlobsDiskUsageStatisticsItem(@NotNull final PersistentEntityStoreStatistics statistics, @NotNull final String name) {
            super(statistics, name);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final PersistentEntityStoreStatistics statistics = (PersistentEntityStoreStatistics) getStatistics();
            return statistics == null ? null : statistics.store.getBlobVault().size();
        }
    }

    private static class CachingJobsStatisticsItem extends StatisticsItem {

        public CachingJobsStatisticsItem(@NotNull final PersistentEntityStoreStatistics statistics, @NotNull final String name) {
            super(statistics, name);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final PersistentEntityStoreStatistics statistics = (PersistentEntityStoreStatistics) getStatistics();
            return statistics == null ? null : (long) (statistics.store.getAsyncProcessor().pendingJobs());
        }
    }
}
