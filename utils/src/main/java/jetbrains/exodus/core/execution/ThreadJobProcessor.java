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

import jetbrains.exodus.core.dataStructures.Priority;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ThreadJobProcessor extends JobProcessorQueueAdapter {

    private final String name;
    private final Runnable body;
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Thread thread;
    private final ClassLoader classLoader;
    @Nullable
    private final ThreadJobProcessor.ThreadCreator creator;


    public ThreadJobProcessor(@NotNull final String name) {
        this(name, null);
    }

    public ThreadJobProcessor(@NotNull final String name, @Nullable ThreadJobProcessor.ThreadCreator creator) {
        classLoader = getClass().getClassLoader();
        this.name = name;
        body = new Runnable() {
            @Override
            public void run() {
                ThreadJobProcessor.this.run();
            }
        };
        this.creator = creator;
        createProcessorThread();
    }

    /**
     * Starts processor thread.
     */
    @Override
    public synchronized void start() {
        if (!started.getAndSet(true)) {
            finished.set(false);
            thread.start();
        }
    }

    /**
     * Signals that the processor to finish and waits until it finishes.
     */
    @Override
    public void finish() {
        if (started.get() && !finished.getAndSet(true)) {
            waitUntilFinished();
            super.finish();
            // recreate thread (don't start) for processor reuse
            createProcessorThread();
            clearQueues();
            started.set(false);
        }
    }

    public boolean queueFinish() {
        return new Job(this, Priority.lowest) {
            @Override
            protected void execute() {
                finished.set(true);
            }
        }.wasQueued();
    }

    public void waitUntilFinished() {
        awake.release();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    public String toString() {
        return thread.toString();
    }

    public long getId() {
        return thread.getId();
    }

    public boolean isCurrentThread() {
        return thread == Thread.currentThread();
    }

    @SuppressWarnings({"RefusedRequest"})
    public final void run() {
        processorStarted();
        while (!isFinished()) {
            doJobs();
        }
        processorFinished();
    }

    private void createProcessorThread() {
        thread = creator == null ? new Thread(body, name) : creator.createThread(body, name);
        thread.setContextClassLoader(classLoader);
        thread.setDaemon(true);
    }

    @Override
    protected void executeJob(@Nullable final Job job) {
        if (job != null) {
            JobProcessor processor = job.getProcessor();
            if (processor != null && !processor.isFinished()) {
                JobProcessorExceptionHandler exceptionHandler = processor.getExceptionHandler();
                try {
                    processor.beforeProcessingJob(job);
                    JobHandler.invokeHandlers(jobStartingHandlers, job);
                    try {
                        thread.setContextClassLoader(job.getClass().getClassLoader());
                        job.run(exceptionHandler);
                    } finally {
                        thread.setContextClassLoader(classLoader);
                        JobHandler.invokeHandlers(jobFinishedHandlers, job);
                    }
                    processor.afterProcessingJob(job);
                } catch (Throwable t) {
                    handleThrowable(job, exceptionHandler, t);
                }
            }
        }
    }

    public interface ThreadCreator {
        Thread createThread(Runnable body, String name);
    }

}
