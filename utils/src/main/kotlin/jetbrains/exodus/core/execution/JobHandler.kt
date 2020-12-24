/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.core.execution

abstract class JobHandler {

    abstract fun handle(job: Job)

    companion object {

        @JvmStatic
        fun append(handlers: Array<JobHandler>?, handle: JobHandler) = handlers?.let { it + handle } ?: arrayOf(handle)

        @JvmStatic
        fun remove(handlers: Array<JobHandler>, handle: JobHandler): Array<JobHandler>? {
            return if (handlers.size <= 1) {
                null
            } else {
                handlers.filter { it != handle }.toTypedArray()
            }
        }

        @JvmStatic
        fun invokeHandlers(handlers: Array<JobHandler>?, job: Job) {
            handlers?.forEach {
                it.handle(job)
            }
        }
    }
}