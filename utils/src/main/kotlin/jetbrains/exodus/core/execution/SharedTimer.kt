/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.decorators.QueueDecorator
import jetbrains.exodus.core.dataStructures.hash.HashSet
import mu.KLogging

/**
 * Shared timer runs registered periodic tasks (each second) in lock-free manner.
 */
object SharedTimer : KLogging() {

    private val PERIOD = 1000 // in milliseconds
    private val registeredTasks: MutableSet<ExpirablePeriodicTask>
    private val processor: JobProcessor

    init {
        registeredTasks = HashSet()
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared timer thread")
        processor.queueIn(Ticker(), PERIOD.toLong())
    }

    @JvmStatic
    fun registerNonExpirablePeriodicTask(task: () -> Unit) {
        processor.queue(object : Job() {
            override fun execute() {
                registeredTasks.add(object : ExpirablePeriodicTask {
                    override val isExpired: Boolean get() = false
                    override fun run() {
                        task()
                    }
                })
            }
        })
    }

    @JvmStatic
    fun registerPeriodicTask(task: ExpirablePeriodicTask) {
        processor.queue(object : Job() {
            override fun execute() {
                registeredTasks.add(task)
            }
        })
    }

    @JvmStatic
    fun registerPeriodicTaskIn(task: ExpirablePeriodicTask, millis: Long) {
        processor.queueIn(object : Job() {
            override fun execute() {
                registeredTasks.add(task)
            }
        }, millis)
    }

    @JvmStatic
    fun unregisterPeriodicTask(task: ExpirablePeriodicTask) {
        processor.queue(object : Job() {
            override fun execute() {
                registeredTasks.remove(task)
            }
        })
    }

    @JvmStatic
    fun ensureIdle() {
        processor.waitForJobs(1)
    }

    interface ExpirablePeriodicTask : Runnable {

        val isExpired: Boolean
    }

    private class Ticker : Job() {

        override fun execute() {
            val nextTick = System.currentTimeMillis() + PERIOD
            try {
                val expiredTasks = QueueDecorator<ExpirablePeriodicTask>()
                registeredTasks.forEach { task ->
                    if (task.isExpired) {
                        expiredTasks.add(task)
                    } else {
                        try {
                            task.run()
                        } catch (t: Throwable) {
                            logger.error("Periodic task failure", t)
                        }
                    }
                }
                for (expiredTask in expiredTasks) {
                    registeredTasks.remove(expiredTask)
                }
            } finally {
                processor.queueAt(this, nextTick)
            }
        }
    }
}