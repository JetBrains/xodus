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
package jetbrains.exodus.entitystore.management;

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.management.MBeanBase;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.entitystore.PersistentEntityStoreStatistics.Type.BLOBS_DISK_USAGE;
import static jetbrains.exodus.entitystore.PersistentEntityStoreStatistics.Type.CACHING_JOBS;

public class EntityStoreStatistics extends MBeanBase implements EntityStoreStatisticsMBean {

    @NotNull
    private final PersistentEntityStoreImpl store;

    public EntityStoreStatistics(@NotNull final PersistentEntityStoreImpl store) {
        super(getObjectName(store));
        this.store = store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public long getBlobsDiskUsage() {
        return store.getStatistics().getStatisticsItem(BLOBS_DISK_USAGE).getTotal();
    }

    @SuppressWarnings("unchecked")
    @Override
    public long getNumberOfCachingJobs() {
        return store.getStatistics().getStatisticsItem(CACHING_JOBS).getTotal();
    }

    @Override
    public long getTotalCachingJobsEnqueued() {
        return store.getEntityIterableCache().getStats().getTotalJobsEnqueued();
    }

    @Override
    public long getTotalCachingJobsStarted() {
        return store.getEntityIterableCache().getStats().getTotalJobsStarted();
    }

    @Override
    public long getTotalCachingJobsInterrupted() {
        return store.getEntityIterableCache().getStats().getTotalJobsInterrupted();
    }

    @Override
    public long getTotalCachingJobsNotStarted() {
        return store.getEntityIterableCache().getStats().getTotalJobsNotStarted();
    }

    @Override
    public long getTotalCachingCountJobs() {
        return store.getEntityIterableCache().getStats().getTotalCountJobs();
    }

    @Override
    public long getTotalEntityIterableCacheHits() {
        return store.getEntityIterableCache().getStats().getTotalHits();
    }

    @Override
    public long getTotalEntityIterableCacheMisses() {
        return store.getEntityIterableCache().getStats().getTotalMisses();
    }

    @Override
    public float getEntityIterableCacheHitRate() {
        return store.getEntityIterableCache().hitRate();
    }

    @Override
    public float getBlobStringsCacheHitRate() {
        return store.getBlobVault().getStringContentCacheHitRate();
    }

    public static String getObjectName(@NotNull final PersistentEntityStoreImpl store) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(store.getLocation());
    }
}
