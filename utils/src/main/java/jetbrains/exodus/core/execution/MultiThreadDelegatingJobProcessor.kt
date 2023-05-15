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
package jetbrains.exodus.core.execution

import jetbrains.exodus.core.dataStructures.*
import jetbrains.exodus.core.execution.SharedTimer.ExpirablePeriodicTask
import jetbrains.exodus.core.execution.SharedTimer.registerPeriodicTaskIn
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.function.Consumer

abstract class MultiThreadDelegatingJobProcessor protected constructor(
    name: String,
    @JvmField val threadCount: Int,
    jobTimeout: Long = 0
) : JobProcessorAdapter() {
    private val jobProcessors: AtomicReferenceArray<ThreadJobProcessor>

    init {
        if (jobTimeout > 0L) {
            registerPeriodicTaskIn(WatchDog(jobTimeout), jobTimeout)
        }
        jobProcessors = AtomicReferenceArray(threadCount)
        for (i in 0 until threadCount) {
            jobProcessors[i] = ThreadJobProcessorPool.getOrCreateJobProcessor(name + i)
        }
    }

    override var exceptionHandler: JobProcessorExceptionHandler?
        get() = super.exceptionHandler
        set(handler) {
            super.setExceptionHandler(handler)
            for (i in 0 until jobProcessors.length()) {
                jobProcessors[i].setExceptionHandler(handler)
            }
        }

    @Suppress("unused")
    fun forEachSubProcessor(action: Consumer<ThreadJobProcessor?>) {
        for (i in 0 until jobProcessors.length()) {
            action.accept(jobProcessors[i])
        }
    }

    @Suppress("unused")
    fun currentJobs(): Array<Job?> {
        val jobs = arrayOfNulls<Job>(
            threadCount
        )
        for (i in 0 until jobProcessors.length()) {
            jobs[i] = jobProcessors[i].currentJob
        }
        return jobs
    }

    val isDispatcherThread: Boolean
        get() {
            for (i in 0 until jobProcessors.length()) {
                if (jobProcessors[i].isCurrentThread) {
                    return true
                }
            }
            return false
        }

    override fun pushAt(job: Job, millis: Long): Job? {
        throw UnsupportedOperationException(UNSUPPORTED_TIMED_JOBS_MESSAGE)
    }

    override fun waitForTimedJobs(spinTimeout: Long) {
        for (i in 0 until jobProcessors.length()) {
            jobProcessors[i].waitForTimedJobs(spinTimeout)
        }
    }

    override fun waitForJobs(spinTimeout: Long) {
        for (i in 0 until jobProcessors.length()) {
            jobProcessors[i].waitForJobs(spinTimeout)
        }
    }

    @Throws(InterruptedException::class)
    override fun suspend() {
        for (i in 0 until jobProcessors.length()) {
            jobProcessors[i].suspend()
        }
    }

    override fun resume() {
        for (i in 0 until jobProcessors.length()) {
            jobProcessors[i].resume()
        }
    }

    override fun queueLowestTimed(job: Job): Boolean {
        throw UnsupportedOperationException()
    }

    override fun queueLowest(job: Job): Boolean {
        throw UnsupportedOperationException()
    }

    override val currentJob: Job?
        get() = null
    override val currentJobStartedAt: Long
        get() = 0

    override fun pendingTimedJobs(): Int {
        return 0
    }

    override val pendingJobs: Iterable<Job?>
        get() = emptyList<Job>()

    override fun start() {
        if (!started.getAndSet(true)) {
            finished.set(false)
            for (i in 0 until jobProcessors.length()) {
                val jobProcessor = jobProcessors[i]
                jobProcessor.start()
            }
        }
    }

    override fun finish() {
        if (started.get() && !finished.getAndSet(true)) {
            for (i in 0 until jobProcessors.length()) {
                val processor = jobProcessors[i]
                // wait for each processor to execute current job (to prevent us from shutting down
                // while our job is being executed right now)
                processor.waitForLatchJob(object : LatchJob() {
                    override fun execute() {
                        release()
                    }
                }, 100)
            }
            started.set(false)
        }
    }

    override fun pendingJobs(): Int {
        var jobs = 0
        for (i in 0 until jobProcessors.length()) {
            val processor = jobProcessors[i]
            jobs += processor.pendingJobs()
        }
        return jobs
    }

    override fun push(job: Job, priority: Priority?): Boolean {
        if (isFinished) {
            return false
        }
        if (job.processor == null) {
            job.processor = this
        }
        val hc = job.hashCode()
        // if you change the way of computing processorNumber then make sure you've changed
        // EntityIterableAsyncInstantiation.hashCode() correspondingly
        val processorNumber = ((hc and 0xffff) + (hc ushr 16)) % jobProcessors.length()
        return job.queue(jobProcessors[processorNumber], priority)
    }

    private inner class WatchDog(private val jobTimeout: Long) : ExpirablePeriodicTask {
        override fun isExpired(): Boolean {
            return isFinished
        }

        override fun run() {
            val currentTime = System.currentTimeMillis()
            for (i in 0 until jobProcessors.length()) {
                val processor = jobProcessors[i]
                val currentJob = processor.currentJob
                if (currentJob != null && currentJob.startedAt + jobTimeout < currentTime) {
                    val newProcessor = ThreadJobProcessorPool.getOrCreateJobProcessor(processor.name + '+')
                    jobProcessors[i] = newProcessor
                    newProcessor.setExceptionHandler(exceptionHandler)
                    processor.moveTo(newProcessor)
                    processor.queueFinish()
                }
            }
        }
    }

    companion object {
        private const val UNSUPPORTED_TIMED_JOBS_MESSAGE =
            "Timed jobs are not supported by MultiThreadDelegatingJobProcessor"
    }
}
