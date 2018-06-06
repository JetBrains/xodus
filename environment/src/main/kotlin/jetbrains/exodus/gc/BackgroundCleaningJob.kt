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
package jetbrains.exodus.gc

import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.log.Log

internal class BackgroundCleaningJob(private var gc: GarbageCollector?) : Job() {

    override fun getName() = "Background cleaner"

    override fun getGroup() = gc?.environment?.location ?: "<finished>"

    /**
     * Cancels job so that it never will be executed again.
     */
    fun cancel() {
        gc = null
        processor = null
    }

    override fun execute() {
        val gc = this.gc ?: return
        val cleaner = gc.cleaner
        if (!cleaner.isCurrentThread) {
            return
        }
        try {
            if (canContinue()) {
                val minTimeToInvokeCleaner = gc.startTime
                if (minTimeToInvokeCleaner > System.currentTimeMillis()) {
                    gc.wakeAt(minTimeToInvokeCleaner)
                    return
                }
                val env = gc.environment
                val ec = env.environmentConfig
                val log = env.log
                if (gc.minFileAge < log.numberOfFiles) {
                    cleaner.isCleaning = true
                    try {
                        doCleanLog(log, gc)
                        if (gc.isTooMuchFreeSpace) {
                            val gcRunPeriod = ec.gcRunPeriod
                            if (gcRunPeriod > 0) {
                                gc.wakeAt(System.currentTimeMillis() + gcRunPeriod)
                            }
                        }
                    } finally {
                        cleaner.isCleaning = false
                    }
                }
            }
        } finally {
            gc.deletePendingFiles()
        }
    }

    private fun doCleanLog(log: Log, gc: GarbageCollector) {
        val up = gc.utilizationProfile

        GarbageCollector.loggingInfo { "Starting background cleaner loop for ${log.location}, free space: ${up.totalFreeSpacePercent()}%" }

        val env = gc.environment
        val highFile = log.highFileAddress
        val loopStart = System.currentTimeMillis()
        val gcRunPeriod = env.environmentConfig.gcRunPeriod

        try {
            do {
                val fragmentedFiles = up.getFilesSortedByUtilization(highFile)
                if (!fragmentedFiles.hasNext()) {
                    return
                }
                if (cleanFiles(gc, fragmentedFiles)) {
                    break
                }
                Thread.yield()
            } while (canContinue() && loopStart + gcRunPeriod > System.currentTimeMillis())
            gc.setUseRegularTxn(true)
            try {
                while (canContinue() && loopStart + gcRunPeriod > System.currentTimeMillis()) {
                    val fragmentedFiles = up.getFilesSortedByUtilization(highFile)
                    if (!fragmentedFiles.hasNext() || !cleanFiles(gc, fragmentedFiles)) {
                        break
                    }
                }
            } finally {
                gc.setUseRegularTxn(false)
            }
        } finally {
            gc.resetNewFiles()
            up.estimateTotalBytes()
            up.isDirty = true
            GarbageCollector.loggingInfo { "Finished background cleaner loop for ${log.location}, free space: ${up.totalFreeSpacePercent()}%" }
        }
    }

    /**
     * We need this synchronized method in order to provide correctness of  [BackgroundCleaner.suspend].
     */
    @Synchronized
    private fun cleanFiles(gc: GarbageCollector, fragmentedFiles: Iterator<Long>) = gc.cleanFiles(fragmentedFiles)

    private fun canContinue(): Boolean {
        val gc = this.gc ?: return false
        val cleaner = gc.cleaner
        if (cleaner.isSuspended || cleaner.isFinished) {
            return false
        }
        val ec = gc.environment.environmentConfig
        return ec.isGcEnabled && !ec.envIsReadonly && gc.isTooMuchFreeSpace
    }
}
