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
package jetbrains.exodus.entitystore.management;

public interface EntityStoreStatisticsMBean {

    String OBJECT_NAME_PREFIX = "jetbrains.exodus.entitystore: type=EntityStoreStatistics";

    long getBlobsDiskUsage();

    long getNumberOfCachingJobs();

    long getTotalCachingJobsEnqueued();

    long getTotalCachingJobsNotQueued();

    long getTotalCachingJobsStarted();

    long getTotalCachingJobsInterrupted();

    long getTotalCachingJobsNotStarted();

    long getTotalCachingCountJobsEnqueued();

    long getTotalEntityIterableCacheHits();

    long getTotalEntityIterableCacheMisses();

    long getTotalEntityIterableCacheCountHits();

    long getTotalEntityIterableCacheCountMisses();

    float getEntityIterableCacheHitRate();

    float getEntityIterableCacheCountHitRate();

    int getEntityIterableCacheCount();

    float getBlobStringsCacheHitRate();
}
