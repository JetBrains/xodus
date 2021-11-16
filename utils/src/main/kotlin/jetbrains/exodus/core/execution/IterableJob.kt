/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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

/**
 * For each value of iterable, executes jobAction successively in a separate job.
 */
class IterableJob<T>(private val iterable: Iterable<T>, private val jobAction: (T) -> Unit) : Job() {

    override fun execute() {
        queueJob(iterable.iterator())
    }

    private fun queueJob(it: Iterator<T>) {
        processor.queue(object : Job() {
            override fun execute() {
                if (it.hasNext()) {
                    jobAction(it.next())
                    queueJob(it)
                }
            }
        })
    }
}

fun <T> JobProcessor.executeIterable(iterable: Iterable<T>, jobAction: (T) -> Unit) {
    queue(IterableJob(iterable, jobAction))
}