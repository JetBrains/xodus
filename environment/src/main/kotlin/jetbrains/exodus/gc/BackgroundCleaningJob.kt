/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.gc

import jetbrains.exodus.log.Log
import java.util.*

internal class BackgroundCleaningJob(gc: GarbageCollector) : GcJob(gc) {

    private val beforeGcActions = ArrayList<Runnable>()

    override fun getName() = "Background cleaner"

    fun addBeforeGcAction(action: Runnable) {
        beforeGcActions += action
    }

    override fun doJob() {
        val gc = this.getGc() ?: return
        val cleaner = gc.cleaner
        if (!cleaner.isCurrentThread()) {
            val processor = cleaner.getJobProcessor()
            GarbageCollector.loggingInfo { "Re-queueing BackgroundCleaningJob[${gc.environment.location}] to thread[${cleaner.threadId}]" }
            reQueue(processor)
            return
        }

        // is invoked too early?
        val minTimeToInvokeCleaner = gc.getStartTime()
        val currentTime = System.currentTimeMillis()
        if (minTimeToInvokeCleaner > currentTime) {
            wakeAt(gc, minTimeToInvokeCleaner)
            return
        }

        val env = gc.environment
        val ec = env.environmentConfig
        val gcRunPeriod = ec.gcRunPeriod

        // is invoked too often?
        if (gcRunPeriod > 0 && gc.lastInvocationTime + gcRunPeriod > currentTime) {
            wakeAt(gc, gc.lastInvocationTime + gcRunPeriod)
            return
        }

        val log = env.log
        // are there enough files in the log?
        if (gc.getMinFileAge() < log.getNumberOfFiles()) {
            if (!canContinue()) {
                wakeAt(gc, System.currentTimeMillis() + gcRunPeriod)
                return
            }
            cleaner.isCleaning = true
            try {
                doCleanLog(log, gc)
                if (gc.isTooMuchFreeSpace()) {
                    if (gcRunPeriod > 0) {
                        wakeAt(gc, System.currentTimeMillis() + gcRunPeriod)
                    }
                }
            } finally {
                gc.lastInvocationTime = System.currentTimeMillis()
                cleaner.isCleaning = false
            }
        }
    }

    private fun doCleanLog(log: Log, gc: GarbageCollector) {

        GarbageCollector.loggingInfo { "Executing before GC actions for ${log.getLocation()}" }
        try {
            beforeGcActions.forEach { it.run() }
        } catch (t: Throwable) {
            GarbageCollector.loggingError(t) { "Failed to execute before GC actions for ${log.getLocation()}" }
        }

        val up = gc.utilizationProfile

        GarbageCollector.loggingInfo {
            "Starting background cleaner loop for ${log.getLocation()}, free space: ${up.totalFreeSpacePercent()}%"
        }

        val env = gc.environment
        val highFile = log.getHighFileAddress()
        val loopStart = System.currentTimeMillis()
        val gcRunPeriod = env.environmentConfig.gcRunPeriod

        try {
            do {
                val fragmentedFiles = up.getFilesSortedByUtilization(highFile)
                if (!fragmentedFiles.hasNext()) {
                    break
                }
                if (!cleanFiles(gc, fragmentedFiles)) {
                    Thread.yield()
                }
            } while (canContinue() && loopStart + gcRunPeriod > System.currentTimeMillis())
        } finally {
            up.estimateTotalBytes()
            up.isDirty = true
            GarbageCollector.loggingInfo { "Finished background cleaner loop for ${log.getLocation()}, free space: ${up.totalFreeSpacePercent()}%" }
        }
    }

    /**
     * We need this synchronized method in order to provide correctness of  [BackgroundCleaner.suspend].
     */
    @Synchronized
    private fun cleanFiles(gc: GarbageCollector, fragmentedFiles: Iterator<Long>) = gc.cleanFiles(fragmentedFiles)

    private fun canContinue(): Boolean {
        val gc = this.getGc() ?: return false
        val cleaner = gc.cleaner
        if (cleaner.isSuspended() || cleaner.isFinished()) {
            return false
        }
        val ec = gc.environment.environmentConfig
        return ec.isGcEnabled && !gc.environment.isReadOnly && gc.isTooMuchFreeSpace()
    }

    private fun wakeAt(gc: GarbageCollector, time: Long) {
        GarbageCollector.loggingDebug {
            val location = gc.environment.location
            "Queueing BackgroundCleaningJob[$location] to wake up at [${Date(time)}]"
        }
        gc.wakeAt(time)
    }
}
