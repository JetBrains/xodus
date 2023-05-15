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
import jetbrains.exodus.core.execution.JobHandler.Companion.invokeHandlers
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

abstract class JobProcessorAdapter protected constructor() : JobProcessor {
    protected val started: AtomicBoolean
    protected val finished: AtomicBoolean
    override var exceptionHandler: JobProcessorExceptionHandler? = null
    protected var jobStartingHandlers: Array<JobHandler>
    protected var jobFinishedHandlers: Array<JobHandler>
    private val suspendSemaphore: Semaphore
    private val suspendLatchJob = SuspendLatchJob()
    private val resumeLatchJob = ResumeLatchJob()
    private val waitJob = WaitJob()
    private val timedWaitJob = WaitJob()
    override var isSuspended = false
        private set

    init {
        started = AtomicBoolean(false)
        finished = AtomicBoolean(true)
        suspendSemaphore = Semaphore(1, true)
    }

    override fun isFinished(): Boolean {
        return finished.get()
    }

    /**
     * Queues a job for execution with the normal (default) priority.
     *
     * @param job job to execute.
     * @return true if the job was actually queued, else if it was merged with an equal job queued earlier
     * and still not executed.
     */
    override fun queue(job: Job): Boolean {
        return push(job, Priority.Companion.normal)
    }

    /**
     * Queues a job for execution with specified priority.
     *
     * @param job      job to execute.
     * @param priority priority if the job in the job queue.
     * @return true if the job was actually queued, else if it was merged with an equal job queued earlier
     * and still not executed.
     */
    override fun queue(job: Job, priority: Priority?): Boolean {
        return push(job, priority)
    }

    /**
     * Queues a job for execution at specified time.
     *
     * @param job    the job.
     * @param millis time to execute the job.
     */
    override fun queueAt(job: Job, millis: Long): Job? {
        return pushAt(job, millis)
    }

    /**
     * Queues a job for execution in specified time.
     *
     * @param job    the job.
     * @param millis execute the job in this time.
     */
    override fun queueIn(job: Job, millis: Long): Job? {
        return pushAt(job, System.currentTimeMillis() + millis)
    }

    override fun finish() {
        waitJob.release()
        timedWaitJob.release()
    }

    override fun waitForJobs(spinTimeout: Long) {
        waitForJobs(waitJob, false, spinTimeout)
    }

    override fun waitForTimedJobs(spinTimeout: Long) {
        waitForJobs(timedWaitJob, true, spinTimeout)
    }

    @Throws(InterruptedException::class)
    override fun suspend() {
        synchronized(suspendLatchJob) {
            if (!isSuspended) {
                if (suspendSemaphore.tryAcquire(0, TimeUnit.SECONDS)) {
                    if (waitForLatchJob(suspendLatchJob, 100)) {
                        suspendLatchJob.release()
                    }
                } else {
                    throw IllegalStateException("Can't acquire suspend semaphore!")
                }
            }
        }
    }

    override fun resume() {
        synchronized(suspendLatchJob) {
            if (isSuspended) {
                suspendSemaphore.release()
                if (waitForLatchJob(resumeLatchJob, 100)) {
                    resumeLatchJob.release()
                }
            }
        }
    }

    override fun beforeProcessingJob(job: Job) {}
    override fun afterProcessingJob(job: Job) {}
    protected abstract fun queueLowest(job: Job): Boolean
    protected abstract fun queueLowestTimed(job: Job): Boolean
    @JvmOverloads
    fun waitForLatchJob(
        latchJob: LatchJob,
        spinTimeout: Long,
        priority: Priority? = Priority.Companion.highest
    ): Boolean {
        if (!acquireLatchJob(latchJob, spinTimeout)) {
            return false
        }
        // latchJob is queued without processor to ensure delegate will execute it
        if (queue(latchJob, priority)) {
            if (acquireLatchJob(latchJob, spinTimeout)) {
                return true
            }
        }
        return false
    }

    abstract fun push(job: Job, priority: Priority?): Boolean
    abstract fun pushAt(job: Job, millis: Long): Job?
    protected fun processorStarted() {}
    protected fun processorFinished() {}
    protected open fun executeJob(job: Job?) {
        if (job != null) {
            val processor = job.processor
            if (processor != null && !processor.isFinished) {
                val exceptionHandler = processor.exceptionHandler
                try {
                    processor.beforeProcessingJob(job)
                    invokeHandlers(jobStartingHandlers, job)
                    try {
                        job.run(exceptionHandler, Thread.currentThread())
                    } finally {
                        invokeHandlers(jobFinishedHandlers, job)
                    }
                    processor.afterProcessingJob(job)
                } catch (t: Throwable) {
                    handleThrowable(job, exceptionHandler, t)
                }
            }
        }
    }

    fun handleThrowable(job: Job?, exceptionHandler: JobProcessorExceptionHandler?, t: Throwable) {
        if (exceptionHandler != null) {
            try {
                exceptionHandler.handle(this, job, t)
            } catch (tt: Throwable) {
                t.printStackTrace()
            }
        } else {
            t.printStackTrace()
        }
    }

    private fun waitForJobs(waitJob: LatchJob, timed: Boolean, spinTimeout: Long) {
        synchronized(waitJob) {
            try {
                if (!acquireLatchJob(waitJob, spinTimeout)) {
                    return
                }
                // latchJob is queued without processor to ensure delegate will execute it
                if (if (timed) queueLowestTimed(waitJob) else queueLowest(waitJob)) {
                    acquireLatchJob(waitJob, spinTimeout)
                }
            } finally {
                waitJob.release()
            }
        }
    }

    private fun acquireLatchJob(waitJob: LatchJob, spinTimeout: Long): Boolean {
        while (!isFinished) {
            try {
                if (waitJob.acquire(spinTimeout)) {
                    return true
                }
            } catch (ignore: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        return false
    }

    private class WaitJob : LatchJob() {
        override fun execute() {
            release()
        }
    }

    private inner class SuspendLatchJob : LatchJob() {
        @Throws(InterruptedException::class)
        override fun execute() {
            isSuspended = true
            release()
            suspendSemaphore.acquire()
            suspendSemaphore.release()
        }

        override fun isEqualTo(job: Job): Boolean {
            return true
        }

        override fun hashCode(): Int {
            return 239
        }
    }

    private inner class ResumeLatchJob : LatchJob() {
        override fun execute() {
            isSuspended = false
            release()
        }

        override fun isEqualTo(job: Job): Boolean {
            return true
        }

        override fun hashCode(): Int {
            return 239
        }
    }
}
