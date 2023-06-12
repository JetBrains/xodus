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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.execution.*
import jetbrains.exodus.kotlin.synchronized
import mu.KLogging

internal class BackgroundCleaner(private val gc: GarbageCollector) {

    private val backgroundCleaningJob = BackgroundCleaningJob(gc)
    private var processor: JobProcessorAdapter

    @JvmField
    internal var threadId: Long = 0

    @Volatile
    private var isSuspended: Boolean = false

    @Volatile
    @JvmField
    var isCleaning: Boolean = false

    init {
        processor = setJobProcessor(
            if (gc.environment.environmentConfig.isLogCacheShared) {
                DelegatingJobProcessor(ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared background cleaner"))
            } else {
                ThreadJobProcessor("Exodus background cleaner for " + gc.environment.location)
            }
        )
    }

    fun isSuspended() = isSuspended

    fun setJobProcessor(processor: JobProcessorAdapter): JobProcessorAdapter {
        if (processor is ThreadJobProcessor) {
            threadId = processor.id
        } else if (processor is DelegatingJobProcessor<*>) {
            val delegate = processor.delegate
            if (delegate is ThreadJobProcessor) {
                threadId = delegate.id
            } else {
                throw ExodusException("Unexpected job processor: $processor")
            }
        }
        if (processor.exceptionHandler == null) {
            processor.exceptionHandler = JobProcessorExceptionHandler { _, _, t -> logger.error(t.message, t) }
        }
        processor.start()
        this.processor = processor
        return processor
    }

    fun getJobProcessor() = processor

    fun addBeforeGcAction(action: Runnable) = backgroundCleaningJob.addBeforeGcAction(action)

    fun isFinished() = processor.isFinished

    fun isCurrentThread(): Boolean = threadId == Thread.currentThread().threadId()

    fun finish() {
        (processor.currentJob as? GcJob)?.cancel()
        backgroundCleaningJob.cancel()
        processor.waitForLatchJob(object : LatchJob() {
            override fun execute() {
                try {
                    gc.deletePendingFiles()
                } finally {
                    release()
                }
            }
        }, 100)
        processor.finish()
    }

    fun deletePendingFiles() {
        backgroundCleaningJob.synchronized {
            if (!isSuspended) {
                gc.doDeletePendingFiles()
            }
        }
    }

    fun suspend() = backgroundCleaningJob.synchronized {
        if (!isSuspended) {
            isSuspended = true
        }
    }

    fun resume() = backgroundCleaningJob.synchronized {
        isSuspended = false
        queueCleaningJob()
    }

    fun queueCleaningJob() {
        if (gc.environment.environmentConfig.isGcEnabled) {
            backgroundCleaningJob.renew(gc)
            processor.queue(backgroundCleaningJob)
        }
    }

    fun queueCleaningJobAt(millis: Long) {
        if (gc.environment.environmentConfig.isGcEnabled) {
            backgroundCleaningJob.renew(gc)
            processor.queueAt(backgroundCleaningJob, millis)
        }
    }

    fun cleanEntireLog() {
        processor.waitForLatchJob(CleanEntireLogJob(gc), 0)
    }

    fun checkThread() {
        if (!isCurrentThread()) {
            throw ExodusException("Background cleaner thread expected as current one")
        }
    }

    companion object : KLogging()
}
