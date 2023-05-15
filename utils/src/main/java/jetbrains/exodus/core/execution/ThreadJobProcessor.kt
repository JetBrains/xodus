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

class ThreadJobProcessor @JvmOverloads constructor(
    private val name: String,
    private val creator: ThreadCreator? = null
) : JobProcessorQueueAdapter() {
    private val body: Runnable
    private var thread: Thread? = null
    private val classLoader: ClassLoader

    init {
        classLoader = javaClass.classLoader
        body = Runnable { this@ThreadJobProcessor.run() }
        createProcessorThread()
    }

    /**
     * Starts processor thread.
     */
    @Synchronized
    override fun start() {
        if (!started.getAndSet(true)) {
            finished.set(false)
            thread!!.start()
        }
    }

    /**
     * Signals that the processor to finish and waits until it finishes.
     */
    override fun finish() {
        if (started.get() && !finished.getAndSet(true)) {
            waitUntilFinished()
            super.finish()
            // recreate thread (don't start) for processor reuse
            createProcessorThread()
            clearQueues()
            started.set(false)
        }
    }

    fun queueFinish(): Boolean {
        return object : Job(this, Priority.Companion.lowest) {
            override fun execute() {
                finished.set(true)
            }
        }.wasQueued()
    }

    fun waitUntilFinished() {
        awake.release()
        try {
            thread!!.join()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            // ignore
        }
    }

    override fun toString(): String {
        return thread.toString()
    }

    fun getName(): String {
        return thread!!.name
    }

    val id: Long
        get() = thread!!.id
    val isCurrentThread: Boolean
        get() = thread === Thread.currentThread()

    fun run() {
        processorStarted()
        while (!isFinished) {
            doJobs()
        }
        processorFinished()
    }

    private fun createProcessorThread() {
        thread = if (creator == null) Thread(body, name) else creator.createThread(body, name)
        thread!!.contextClassLoader = classLoader
        thread!!.isDaemon = true
    }

    override fun executeJob(job: Job?) {
        if (job != null) {
            val processor = job.processor
            if (processor != null && !processor.isFinished) {
                val exceptionHandler = processor.exceptionHandler
                try {
                    processor.beforeProcessingJob(job)
                    invokeHandlers(jobStartingHandlers, job)
                    try {
                        thread!!.contextClassLoader = job.javaClass.classLoader
                        job.run(exceptionHandler, thread!!)
                    } finally {
                        thread!!.contextClassLoader = classLoader
                        invokeHandlers(jobFinishedHandlers, job)
                    }
                    processor.afterProcessingJob(job)
                } catch (t: Throwable) {
                    handleThrowable(job, exceptionHandler, t)
                }
            }
        }
    }

    interface ThreadCreator {
        fun createThread(body: Runnable?, name: String?): Thread?
    }
}
