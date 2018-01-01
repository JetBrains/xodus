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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.management.StatisticsItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PersistentEntityStoreStatistics extends Statistics<PersistentEntityStoreStatistics.Type> {

    public enum Type {
        BLOBS_DISK_USAGE("Blobs disk usage"),
        CACHING_JOBS("Caching jobs");

        public final String id;

        Type(String id) {
            this.id = id;
        }
    }

    @NotNull
    private final PersistentEntityStoreImpl store;

    PersistentEntityStoreStatistics(@NotNull final PersistentEntityStoreImpl store) {
        super(Type.values());
        this.store = store;

        createAllStatisticsItems();
    }

    @Override
    protected StatisticsItem createStatisticsItem(@NotNull Type key) {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        if (!store.getConfig().getGatherStatistics()) {
            return new StatisticsItem(this);
        }
        return super.createStatisticsItem(key);
    }

    @NotNull
    @Override
    public StatisticsItem getStatisticsItem(@NotNull final String statisticsName) {
        // if don't gather statistics just return the new item and don't register it as periodic task in SharedTimer
        if (!store.getConfig().getGatherStatistics()) {
            return new StatisticsItem(this);
        }
        return super.getStatisticsItem(statisticsName);
    }

    @NotNull
    @Override
    protected StatisticsItem createNewBuiltInItem(@NotNull final Type key) {
        switch (key) {
            case BLOBS_DISK_USAGE:
                return new BlobsDiskUsageStatisticsItem(this);
            case CACHING_JOBS:
                new CachingJobsStatisticsItem(this);

            default:
                return super.createNewBuiltInItem(key);
        }
    }

    private static class BlobsDiskUsageStatisticsItem extends StatisticsItem {

        public BlobsDiskUsageStatisticsItem(@NotNull final PersistentEntityStoreStatistics statistics) {
            super(statistics);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final PersistentEntityStoreStatistics statistics = (PersistentEntityStoreStatistics) getStatistics();
            return statistics == null ? null : statistics.store.getBlobVault().size();
        }
    }

    private static class CachingJobsStatisticsItem extends StatisticsItem {

        public CachingJobsStatisticsItem(@NotNull final PersistentEntityStoreStatistics statistics) {
            super(statistics);
        }

        @Nullable
        @Override
        protected Long getAutoUpdatedTotal() {
            final PersistentEntityStoreStatistics statistics = (PersistentEntityStoreStatistics) getStatistics();
            return statistics == null ? null : (long) (statistics.store.getAsyncProcessor().pendingJobs());
        }
    }
}
