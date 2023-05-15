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

class DelegatingJobProcessor<T : JobProcessorAdapter?>(val delegate: T) : JobProcessorAdapter() {

    override fun start() {
        if (!started.getAndSet(true)) {
            delegate!!.start()
            finished.set(false)
            val startJob: Job = object : Job() {
                override fun execute() {
                    processorStarted()
                }
            }
            startJob.processor = this
            delegate.queue(startJob, Priority.Companion.highest)
        }
    }

    override fun finish() {
        if (started.get() && !finished.getAndSet(true)) {
            delegate!!.waitForLatchJob(object : LatchJob() {
                override fun execute() {
                    try {
                        processorFinished()
                    } catch (t: Throwable) {
                        val exceptionHandler = getExceptionHandler()
                        exceptionHandler?.handle(this@DelegatingJobProcessor, this, t)
                    } finally {
                        release()
                    }
                }
            }, 100)
            setExceptionHandler(null)
            started.set(false)
        }
    }

    override fun pendingJobs(): Int {
        // this value is not relevant, but we can't afford its graceful evaluation
        return delegate!!.pendingJobs()
    }

    override fun waitForJobs(spinTimeout: Long) {
        delegate!!.waitForJobs(spinTimeout)
    }

    override fun waitForTimedJobs(spinTimeout: Long) {
        delegate!!.waitForTimedJobs(spinTimeout)
    }

    override val currentJob: Job?
        get() = delegate.getCurrentJob()
    override val currentJobStartedAt: Long
        get() = delegate.getCurrentJobStartedAt()
    override val pendingJobs: Iterable<Job?>
        get() = delegate.getPendingJobs()

    override fun pendingTimedJobs(): Int {
        // this value is not relevant, but we can't afford its graceful evaluation
        return delegate!!.pendingTimedJobs()
    }

    override fun toString(): String {
        return "delegating -> $delegate"
    }

    override fun queueLowest(job: Job): Boolean {
        throw UnsupportedOperationException()
    }

    override fun queueLowestTimed(job: Job): Boolean {
        throw UnsupportedOperationException()
    }

    override fun push(job: Job, priority: Priority?): Boolean {
        if (isFinished) return false
        if (job.processor == null) {
            job.processor = this
        }
        return delegate!!.push(job, priority)
    }

    override fun pushAt(job: Job, millis: Long): Job? {
        if (isFinished) return null
        if (job.processor == null) {
            job.processor = this
        }
        return delegate!!.pushAt(job, millis)
    }
}
