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
package jetbrains.exodus.core.execution;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.jetbrains.annotations.NotNull;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

@SuppressWarnings({"UtilityClassWithoutPrivateConstructor"/* public constructor is necessary for hosted server */})
public class ThreadJobProcessorPool {

    private static final Map<String, ThreadJobProcessor> PROCESSORS = new HashMap<>();
    private static final ThreadJobProcessor SPAWNER = new ThreadJobProcessor("threadJobProcessorPoolSpawner");

    @NotNull
    public static ThreadJobProcessor getOrCreateJobProcessor(final String processorName) {
        ThreadJobProcessor result = PROCESSORS.get(processorName);
        if (result == null) {
            synchronized (PROCESSORS) {
                result = PROCESSORS.get(processorName);
                if (result == null) {
                    SPAWNER.start();
                    PROCESSORS.put(processorName, result = new ThreadJobProcessor(processorName, new ThreadJobProcessor.ThreadCreator() {
                        @Override
                        public Thread createThread(final Runnable body, final String name) {
                            //   This method is invoked first time from constructor (two lines above)
                            // constructor execution waits for latch since processor being created
                            // invokes createProcessorThread() method.
                            //   Calling thread waits for latch to be released by SPAWNER thread in LatchJob below.
                            // Also, this method can be invoked on processor re-creation and blocks calling thread
                            // just like it was invoked from constructor.
                            final ThreadContainer resultContainer = new ThreadContainer();
                            SPAWNER.waitForLatchJob(new LatchJob() {
                                @Override
                                protected void execute() {
                                    resultContainer.thread = AccessController.doPrivileged(new PrivilegedAction<Thread>() {
                                        @Override
                                        public Thread run() {
                                            return new Thread(body, name);
                                        }
                                    });
                                    release();
                                }
                            }, 100);
                            Thread thread = resultContainer.thread;
                            resultContainer.thread = null; // paranoiac cleaning of thread reference
                            if (thread == null) {
                                throw new IllegalStateException("Can't create JobProcessor thread!");
                            } else {
                                return thread;
                            }
                        }
                    }));
                }

                result.setExceptionHandler(new DefaultExceptionHandler());

                result.start();
            }
        }
        return result;
    }

    public static Collection<JobProcessor> getProcessors() {
        synchronized (PROCESSORS) {
            return new ArrayList<JobProcessor>(PROCESSORS.values());
        }
    }

    private static class ThreadContainer {
        private Thread thread;
    }

}
