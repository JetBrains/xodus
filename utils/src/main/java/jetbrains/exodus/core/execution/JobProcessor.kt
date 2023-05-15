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

interface JobProcessor {
    fun start()
    fun finish()
    val isFinished: Boolean
    @JvmField
    var exceptionHandler: JobProcessorExceptionHandler?
    fun queue(job: Job): Boolean
    fun queue(job: Job, priority: Priority?): Boolean
    fun queueAt(job: Job, millis: Long): Job?
    fun queueIn(job: Job, millis: Long): Job?
    fun pendingJobs(): Int
    fun pendingTimedJobs(): Int
    fun waitForJobs(spinTimeout: Long)
    fun waitForTimedJobs(spinTimeout: Long)

    @Throws(InterruptedException::class)
    fun suspend()
    fun resume()
    val isSuspended: Boolean
    fun beforeProcessingJob(job: Job)
    fun afterProcessingJob(job: Job)
    val currentJob: Job?
    val currentJobStartedAt: Long
    val pendingJobs: Iterable<Job?>
}
