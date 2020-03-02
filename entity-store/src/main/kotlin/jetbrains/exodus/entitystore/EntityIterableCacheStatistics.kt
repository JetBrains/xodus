/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore

class EntityIterableCacheStatistics {

    var totalJobsEnqueued = 0L
        private set
    var totalJobsMerged = 0L
        private set
    var totalJobsStarted = 0L
        private set
    var totalJobsInterrupted = 0L
        private set
    var totalJobsNotStarted = 0L
        private set
    var totalCountJobsEnqueued = 0L
        private set
    var totalHits = 0L
        private set
    var totalMisses = 0L
        private set
    var totalCountHits = 0L
        private set
    var totalCountMisses = 0L
        private set

    fun incTotalJobsEnqueued() = ++totalJobsEnqueued

    fun incTotalJobsMerged() = ++totalJobsMerged

    fun incTotalJobsStarted() = ++totalJobsStarted

    fun incTotalJobsInterrupted() = ++totalJobsInterrupted

    fun incTotalJobsNotStarted() = ++totalJobsNotStarted

    fun incTotalCountJobsEnqueued() = ++totalCountJobsEnqueued

    fun incTotalHits() = ++totalHits

    fun incTotalMisses() = ++totalMisses

    fun incTotalCountHits() = ++totalCountHits

    fun incTotalCountMisses() = ++totalCountMisses
}