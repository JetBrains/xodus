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

abstract class DecoratorJob : Job {
    private var decorated: Job

    protected constructor() {
        decorated = this
    }

    protected constructor(
        processor: JobProcessor,
        decorated: Job
    ) : this(processor, Priority.Companion.normal, decorated)

    protected constructor(
        processor: JobProcessor,
        priority: Priority,
        decorated: Job
    ) : super() {
        this.decorated = decorated
        setProcessor(processor)
        queue(priority)
    }

    /**
     * Factory method to decorate a job for execution with normal priority.
     *
     * @param decorated source job.
     * @return decorator job.
     */
    abstract fun newDecoratorJob(decorated: Job?): DecoratorJob?

    /**
     * Factory method to decorate a job for execution with specified priority.
     *
     * @param decorated source job.
     * @param priority  priority which to execute the job with.
     * @return decorator job.
     */
    abstract fun newDecoratorJob(decorated: Job?, priority: Priority?): DecoratorJob?
    fun getDecorated(): Job {
        return decorated
    }

    fun setDecorated(decorated: Job) {
        this.decorated = decorated
        processor = decorated.processor
    }

    @Throws(Throwable::class)
    protected fun executeDecorated() {
        decorated.execute()
    }

    override val name: String?
        get() = decorated.name
    override val group: String?
        get() = decorated.group

    override fun hashCode(): Int {
        return decorated.hashCode()
    }

    override fun isEqualTo(job: Job): Boolean {
        return decorated == (job as DecoratorJob).decorated
    }
}
