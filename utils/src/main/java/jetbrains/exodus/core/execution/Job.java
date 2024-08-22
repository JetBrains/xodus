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
package jetbrains.exodus.core.execution;

import jetbrains.exodus.core.dataStructures.Priority;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;

public abstract class Job {

    private JobProcessor processor;
    private boolean wasQueued;
    @Nullable
    private JobHandler[] jobStartingHandlers;
    @Nullable
    private JobHandler[] jobFinishedHandlers;
    private Thread executingThread;
    private long startedAt;

    protected Job() {
        processor = null;
        wasQueued = false;
        jobStartingHandlers = null;
        jobFinishedHandlers = null;
        executingThread = null;
        startedAt = 0L;
    }

    protected Job(@NotNull final JobProcessor processor) {
        this(processor, Priority.normal);
    }

    protected Job(@NotNull final JobProcessor processor, @NotNull final Priority priority) {
        this.processor = processor;
        jobStartingHandlers = null;
        jobFinishedHandlers = null;
        queue(priority);
    }

    public boolean queue(Priority priority) {
        return queue(processor, priority);
    }

    public boolean queue(JobProcessor processor, Priority priority) {
        return (wasQueued = processor.queue(this, priority));
    }

    public JobProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(final JobProcessor processor) {
        this.processor = processor;
    }

    public boolean wasQueued() {
        return wasQueued;
    }

    public String getName() {
        final String name = getClass().getSimpleName();
        return name.isEmpty() ? "<anonymous>" : name;
    }

    public String getGroup() {
        return getClass().getName();
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(100);
        result.append(getGroup());
        result.append(": ");
        result.append(getName());
        if (startedAt > 0L) {
            result.append(", started at: ");
            result.append(new Date(startedAt).toString()
                    .substring(4) // skip day of the week
            );
        }
        return result.toString();
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    public long getStartedAt() {
        return startedAt;
    }

    /**
     * Registers a handler to be invoked before the job is executed.
     * All handlers are executed in the same processor as the {@link #execute()} method.
     *
     * @param handler an instance of {@link JobHandler} to be invoked before the job is started.
     */
    public void registerJobStartingHandler(@NotNull final JobHandler handler) {
        jobStartingHandlers = JobHandler.append(jobStartingHandlers, handler);
    }

    /**
     * Registers a handler to be invoked after the job has executed.
     * All handlers are executed in the same processor as the {@link #execute()} method and independently
     * of exceptions it may throw during execution.
     *
     * @param handler an instance of {@link JobHandler} to be invoked after the job has finished.
     */
    public void registerJobFinishedHandler(@NotNull final JobHandler handler) {
        jobFinishedHandlers = JobHandler.append(jobFinishedHandlers, handler);
    }

    final void run(@Nullable final JobProcessorExceptionHandler handler,
                   @NotNull final Thread executingThread) {
        this.executingThread = executingThread;
        startedAt = System.currentTimeMillis();
        Throwable exception = null;
        JobHandler.invokeHandlers(jobStartingHandlers, this);
        try {
            execute();
        } catch (Throwable t) {
            exception = t;
        } finally {
            JobHandler.invokeHandlers(jobFinishedHandlers, this);
        }
        if (exception != null && handler != null) {
            handler.handle(processor, this, exception);
        }
    }

    public final boolean equals(Object job) {
        if (job == this) return true;
        /* this.getClass() returns some class derived from Job, hence
if getClass() == job.getClass() then 'job' is instanceOf Job too */
        return job != null && getClass() == job.getClass() && isEqualTo((Job) job);
    }

    public boolean isEqualTo(Job job) {
        return super.equals(job);
    }

    protected abstract void execute() throws Throwable;

}
