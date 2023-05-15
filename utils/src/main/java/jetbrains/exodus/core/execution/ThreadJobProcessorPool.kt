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

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.execution.ThreadJobProcessor.ThreadCreator
import java.security.AccessController
import java.security.PrivilegedAction

object ThreadJobProcessorPool {
    private val PROCESSORS: MutableMap<String, ThreadJobProcessor> = HashMap()
    private val SPAWNER = ThreadJobProcessor("threadJobProcessorPoolSpawner")
    fun getOrCreateJobProcessor(processorName: String): ThreadJobProcessor {
        var result = PROCESSORS[processorName]
        if (result == null) {
            synchronized(PROCESSORS) {
                result = PROCESSORS[processorName]
                if (result == null) {
                    SPAWNER.start()
                    PROCESSORS[processorName] =
                        ThreadJobProcessor(processorName, ThreadCreator { body: Runnable?, name: String? ->
                            //   This method is invoked first time from constructor (two lines above)
                            // constructor execution waits for latch since processor being created
                            // invokes createProcessorThread() method.
                            //   Calling thread waits for latch to be released by SPAWNER thread in LatchJob below.
                            // Also, this method can be invoked on processor re-creation and blocks calling thread
                            // just like it was invoked from constructor.
                            val resultContainer = ThreadContainer()
                            SPAWNER.waitForLatchJob(object : LatchJob() {
                                override fun execute() {
                                    resultContainer.thread = AccessController.doPrivileged(PrivilegedAction {
                                        Thread(
                                            body,
                                            name
                                        )
                                    } as PrivilegedAction<Thread>)
                                    release()
                                }
                            }, 100)
                            val thread = resultContainer.thread
                            resultContainer.thread = null // paranoiac cleaning of thread reference
                            if (thread == null) {
                                throw IllegalStateException("Can't create JobProcessor thread!")
                            } else {
                                return@ThreadCreator thread
                            }
                        }).also { result = it }
                }
                result.setExceptionHandler(DefaultExceptionHandler())
                result!!.start()
            }
        }
        return result!!
    }

    val processors: Collection<JobProcessor>
        get() {
            synchronized(PROCESSORS) { return ArrayList<JobProcessor>(PROCESSORS.values) }
        }

    private class ThreadContainer {
        val thread: Thread? = null
    }
}
