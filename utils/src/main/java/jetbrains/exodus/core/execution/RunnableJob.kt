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

class RunnableJob : Job {
    private val runnable: Runnable

    constructor(runnable: Runnable) {
        this.runnable = runnable
    }

    constructor(processor: JobProcessor, runnable: Runnable) : this(processor, Priority.Companion.normal, runnable)
    constructor(processor: JobProcessor, priority: Priority, runnable: Runnable) {
        setProcessor(processor)
        this.runnable = runnable
        queue(priority)
    }

    override fun execute() {
        runnable.run()
    }

    override val name: String?
        get() {
            val name = runnable.javaClass.simpleName
            return if (name.isEmpty()) "Anonymous runnable job" else "Runnable job $name"
        }
    override val group: String?
        get() = runnable.javaClass.name
}
