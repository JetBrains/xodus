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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

abstract class JobProcessorQueueAdapter protected constructor() : JobProcessorAdapter() {
    private val queue: PriorityQueue<Priority?, Job?>
    private val timeQueue: PriorityQueue<Long, Job?>

    @Volatile
    private var outdatedJobsCount = 0

    @Volatile
    override var currentJob: Job? = null
        private set

    @Volatile
    override var currentJobStartedAt: Long = 0
        private set
    protected val awake: Semaphore

    init {
        queue = createQueue()
        timeQueue = createQueue()
        awake = Semaphore(0)
    }

    override fun queueLowest(job: Job): Boolean {
        if (isFinished) return false
        if (job.processor == null) {
            job.processor = this
        }
        queue.lock().use { ignored ->
            val pair = queue.floorPair()
            val priority: Priority = if (pair == null) Priority.Companion.highest else pair.getFirst()
            if (queue.push(priority, job) != null) {
                return false
            }
        }
        awake.release()
        return true
    }

    override fun push(job: Job, priority: Priority?): Boolean {
        if (isFinished) return false
        if (job.processor == null) {
            job.processor = this
        }
        queue.lock().use { ignored ->
            if (queue.push(priority, job) != null) {
                return false
            }
        }
        awake.release()
        return true
    }

    override fun pushAt(job: Job, millis: Long): Job? {
        if (isFinished) {
            return null
        }
        if (job.processor == null) {
            job.processor = this
        }
        var oldJob: Job?
        val pair: Pair<Long?, Job?>?
        val priority = Long.MAX_VALUE - millis
        timeQueue.lock().use { ignored ->
            oldJob = timeQueue.push(priority, job)
            pair = timeQueue.peekPair()
        }
        if (pair != null && pair.getFirst() != priority) {
            return oldJob
        }
        awake.release()
        return oldJob
    }

    override fun queueLowestTimed(job: Job): Boolean {
        if (isFinished) {
            return false
        }
        if (job.processor == null) {
            job.processor = this
        }
        timeQueue.lock().use { ignored ->
            val pair = timeQueue.floorPair()
            val priority = if (pair == null) Long.MAX_VALUE - System.currentTimeMillis() else pair.getFirst()
            if (timeQueue.push(priority, job) != null) {
                return false
            }
        }
        awake.release()
        return true
    }

    override fun pendingJobs(): Int {
        return queue.size() + if (currentJob == null) 0 else 1
    }

    override fun pendingTimedJobs(): Int {
        return timeQueue.size() + outdatedJobsCount
    }

    override val pendingJobs: Iterable<Job?>
        get() = queue

    protected fun doJobs() {
        val jobsQueued: Boolean
        jobsQueued = try {
            waitForJobs()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return
        }
        try {
            if (!isFinished) {
                if (jobsQueued) {
                    val job: Job?
                    queue.lock().use { ignored -> job = queue.pop() }
                    doExecuteJob(job)
                } else {
                    doTimedJobs()
                }
            }
        } catch (t: Throwable) {
            handleThrowable(null, getExceptionHandler(), t)
        }
    }

    protected fun clearQueues() {
        queue.clear()
        timeQueue.clear()
    }

    protected fun doTimedJobs() {
        val outdatedJobs: MutableCollection<Job?> = ArrayList()
        val currentTimePriority = Long.MAX_VALUE - System.currentTimeMillis()
        var count: Int
        timeQueue.lock().use { ignored ->
            var pair = timeQueue.peekPair()
            while (pair != null && pair.getFirst() >= currentTimePriority) {
                outdatedJobs.add(timeQueue.pop())
                pair = timeQueue.peekPair()
            }
            count = outdatedJobs.size
        }
        // outdatedJobsCount is updated in single thread, so we won't bother with synchronization on its update
        outdatedJobsCount = count
        for (job in outdatedJobs) {
            executeImmediateJobsIfAny()
            if (isFinished) {
                break
            }
            doExecuteJob(job)
            outdatedJobsCount = --count
        }
    }

    fun moveTo(copy: JobProcessorQueueAdapter) {
        PriorityQueue.Companion.moveQueue<Long, Job?>(timeQueue, copy.timeQueue)
        val queueSize: Int = PriorityQueue.Companion.moveQueue<Priority?, Job?>(
            queue, copy.queue
        )
        if (queueSize > 0) {
            copy.awake.release(queueSize)
        }
    }

    private fun executeImmediateJobsIfAny() {
        while (!isFinished && executeImmediateJobIfAny() != null);
    }

    /**
     * @return executed job or null if it was nothing to execute.
     */
    private fun executeImmediateJobIfAny(): Job? {
        var urgentImmediateJob: Job? = null
        queue.lock().use { ignored ->
            val peekPair = queue.peekPair()
            if (peekPair != null && peekPair.getFirst() === Priority.Companion.highest) {
                urgentImmediateJob = queue.pop()
            }
        }
        if (urgentImmediateJob != null) {
            doExecuteJob(urgentImmediateJob)
        }
        return urgentImmediateJob
    }

    // returns true if a job was queued within a timeout
    @Throws(InterruptedException::class)
    protected fun waitForJobs(): Boolean {
        val peekPair: Pair<Long?, Job?>?
        timeQueue.lock().use { ignored -> peekPair = timeQueue.peekPair() }
        if (peekPair == null) {
            awake.acquire()
            return true
        }
        val timeout = Long.MAX_VALUE - peekPair.getFirst() - System.currentTimeMillis()
        return if (timeout < 0) {
            false
        } else awake.tryAcquire(timeout, TimeUnit.MILLISECONDS)
    }

    private fun doExecuteJob(job: Job?) {
        currentJob = job
        currentJobStartedAt = System.currentTimeMillis()
        try {
            executeJob(job)
        } finally {
            currentJob = null
            currentJobStartedAt = 0L
        }
    }

    companion object {
        const val CONCURRENT_QUEUE_PROPERTY = "jetbrains.exodus.core.execution.concurrentQueue"
        private fun createQueue(): PriorityQueue<*, *> {
            return if (java.lang.Boolean.getBoolean(CONCURRENT_QUEUE_PROPERTY)) ConcurrentStablePriorityQueue<Any?, Any?>() else StablePriorityQueue<Any?, Any?>()
        }
    }
}
