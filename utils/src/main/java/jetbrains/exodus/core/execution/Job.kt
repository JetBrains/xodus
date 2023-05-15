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
import jetbrains.exodus.core.execution.JobHandler.Companion.append
import jetbrains.exodus.core.execution.JobHandler.Companion.invokeHandlers
import java.util.*

abstract class Job {
    var processor: JobProcessor?
    private var wasQueued = false
    private var jobStartingHandlers: Array<JobHandler>?
    private var jobFinishedHandlers: Array<JobHandler>?
    var executingThread: Thread? = null
        private set
    var startedAt: Long = 0
        private set

    @Volatile
    var isCompleted = false
        private set

    protected constructor() {
        processor = null
        wasQueued = false
        jobStartingHandlers = null
        jobFinishedHandlers = null
        executingThread = null
        startedAt = 0L
    }

    protected constructor(processor: JobProcessor, priority: Priority = Priority.Companion.normal) {
        this.processor = processor
        jobStartingHandlers = null
        jobFinishedHandlers = null
        queue(priority)
    }

    fun queue(priority: Priority?): Boolean {
        return queue(processor, priority)
    }

    fun queue(processor: JobProcessor?, priority: Priority?): Boolean {
        return processor!!.queue(this, priority).also { wasQueued = it }
    }

    fun wasQueued(): Boolean {
        return wasQueued
    }

    open val name: String?
        get() {
            val name = javaClass.simpleName
            return if (name.isEmpty()) "<anonymous>" else name
        }
    open val group: String?
        get() = javaClass.name

    override fun toString(): String {
        val result = StringBuilder(100)
        result.append(group)
        result.append(": ")
        result.append(name)
        if (startedAt > 0L) {
            result.append(", started at: ")
            result.append(
                Date(startedAt).toString()
                    .substring(4) // skip day of the week
            )
        }
        return result.toString()
    }

    /**
     * Registers a handler to be invoked before the job is executed.
     * All handlers are executed in the same processor as the [.execute] method.
     *
     * @param handler an instance of [JobHandler] to be invoked before the job is started.
     */
    fun registerJobStartingHandler(handler: JobHandler) {
        jobStartingHandlers = append(jobStartingHandlers, handler)
    }

    /**
     * Registers a handler to be invoked after the job has executed.
     * All handlers are executed in the same processor as the [.execute] method and independently
     * of exceptions it may throw during execution.
     *
     * @param handler an instance of [JobHandler] to be invoked after the job has finished.
     */
    fun registerJobFinishedHandler(handler: JobHandler) {
        jobFinishedHandlers = append(jobFinishedHandlers, handler)
    }

    fun run(
        handler: JobProcessorExceptionHandler?,
        executingThread: Thread
    ) {
        this.executingThread = executingThread
        startedAt = System.currentTimeMillis()
        var exception: Throwable? = null
        invokeHandlers(jobStartingHandlers, this)
        try {
            execute()
        } catch (t: Throwable) {
            exception = t
        } finally {
            invokeHandlers(jobFinishedHandlers, this)
            isCompleted = true
        }
        if (exception != null && handler != null) {
            handler.handle(processor, this, exception)
        }
    }

    override fun equals(job: Any?): Boolean {
        return if (job === this) true else job != null && javaClass == job.javaClass && isEqualTo(job as Job)
        /* this.getClass() returns some class derived from Job, hence
if getClass() == job.getClass() then 'job' is instanceOf Job too */
    }

    open fun isEqualTo(job: Job): Boolean {
        return super.equals(job)
    }

    @Throws(Throwable::class)
    abstract fun execute()
}
