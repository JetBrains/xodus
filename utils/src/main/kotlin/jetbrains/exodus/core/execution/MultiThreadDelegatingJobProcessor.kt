/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.core.execution

import jetbrains.exodus.core.dataStructures.Priority

abstract class MultiThreadDelegatingJobProcessor
@JvmOverloads protected constructor(name: String, threadCount: Int, jobTimeout: Long = 0L) : JobProcessorAdapter() {

    protected val jobProcessors: Array<ThreadJobProcessor> =
            (0 until threadCount).map { i -> ThreadJobProcessorPool.getOrCreateJobProcessor(name + i) }.toTypedArray()

    init {
        if (jobTimeout > 0L) {
            SharedTimer.registerPeriodicTaskIn(WatchDog(jobTimeout), jobTimeout)
        }
    }

    val currentJobs: Array<Job?> get() = jobProcessors.map { processor -> processor.currentJob }.toTypedArray()

    val isDispatcherThread: Boolean get() = jobProcessors.any { it.isCurrentThread }

    val threadCount: Int get() = jobProcessors.size

    fun forEachSubProcessor(action: (ThreadJobProcessor) -> Unit) = jobProcessors.forEach { processor -> action(processor) }

    override fun setExceptionHandler(handler: JobProcessorExceptionHandler?) {
        super.setExceptionHandler(handler)
        jobProcessors.forEach { processor -> processor.exceptionHandler = handler }
    }

    override fun pushAt(job: Job, millis: Long): Job = throw UnsupportedOperationException(UNSUPPORTED_TIMED_JOBS_MESSAGE)

    override fun waitForJobs(spinTimeout: Long) = jobProcessors.forEach { processor -> processor.waitForJobs(spinTimeout) }

    override fun waitForTimedJobs(spinTimeout: Long) = jobProcessors.forEach { processor -> processor.waitForTimedJobs(spinTimeout) }

    override fun suspend() =
            jobProcessors.forEach { processor -> processor.suspend() }

    override fun resume() =
            jobProcessors.forEach { processor -> processor.resume() }

    override fun queueLowest(job: Job): Boolean = throw UnsupportedOperationException()

    override fun queueLowestTimed(job: Job): Boolean = throw UnsupportedOperationException()

    override fun getCurrentJob(): Job? = null

    override fun getCurrentJobStartedAt(): Long = 0L

    override fun getPendingJobs(): Iterable<Job> = emptyList()

    override fun pendingTimedJobs(): Int = 0

    override fun start() {
        if (!started.getAndSet(true)) {
            finished.set(false)
            for (jobProcessor in jobProcessors) {
                jobProcessor.start()
            }
        }
    }

    override fun finish() {
        if (started.get() && !finished.getAndSet(true)) {
            for (processor in jobProcessors) {
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

    override fun pendingJobs(): Int = jobProcessors.fold(0) { jobs, processor -> jobs + processor.pendingJobs() }

    override fun push(job: Job, priority: Priority): Boolean {
        if (isFinished) return false
        if (job.processor == null) {
            job.processor = this
        }
        val hc = job.hashCode()
        val processorNumber = ((hc and 0xffff) + hc.ushr(16)) % jobProcessors.size
        return job.queue(jobProcessors[processorNumber], priority)
    }

    private inner class WatchDog(private val jobTimeout: Long) : SharedTimer.ExpirablePeriodicTask {

        override val isExpired: Boolean
            get() = isFinished

        override fun run() {
            val currentTime = System.currentTimeMillis()
            for (i in jobProcessors.indices) {
                val processor = jobProcessors[i]
                val currentJob = processor.currentJob
                if (currentJob != null && currentJob.startedAt + jobTimeout < currentTime) {
                    val newProcessor = ThreadJobProcessorPool.getOrCreateJobProcessor(processor.name + '+')
                    jobProcessors[i] = newProcessor
                    newProcessor.exceptionHandler = exceptionHandler
                    processor.moveTo(newProcessor)
                    processor.queueFinish()
                }
            }
        }
    }

    companion object {

        private const val UNSUPPORTED_TIMED_JOBS_MESSAGE = "Timed jobs are not supported by MultiThreadDelegatingJobProcessor"
        private const val UNSUPPORTED_SUSPEND_MESSAGE = "Suspend operation is not supported by MultiThreadDelegatingJobProcessor"
        private const val UNSUPPORTED_RESUME_MESSAGE = "Resume operation is not supported by MultiThreadDelegatingJobProcessor"
    }
}
