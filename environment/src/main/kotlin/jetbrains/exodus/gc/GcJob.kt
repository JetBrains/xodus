/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.gc

import jetbrains.exodus.core.dataStructures.Priority
import jetbrains.exodus.core.execution.Job
import java.lang.ref.WeakReference

internal open class GcJob(gc: GarbageCollector?, private val unitOfWork: (() -> Unit)? = null) : Job() {

    private var gcRef = WeakReference(gc)

    init {
        processor = gc?.cleaner?.getJobProcessor()
    }

    protected val gc get() = gcRef.get()

    override fun execute() {
        gc?.run {
            val actualProcessor = cleaner.getJobProcessor()
            if (actualProcessor != processor) {
                processor = actualProcessor
                actualProcessor.queue(this@GcJob, Priority.highest)
            } else {
                unitOfWork.let {
                    if (it == null) {
                        doJob()
                    } else {
                        it()
                    }
                }
            }
        }
    }

    override fun getGroup() = gc?.environment?.location ?: "<finished>"

    /**
     * Cancels job so that it never will be executed again.
     */
    fun cancel() {
        gcRef = WeakReference(null)
        processor = null
    }

    protected open fun doJob() {}
}