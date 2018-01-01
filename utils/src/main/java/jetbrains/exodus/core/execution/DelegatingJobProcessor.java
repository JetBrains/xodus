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

public class DelegatingJobProcessor<T extends JobProcessorAdapter> extends JobProcessorAdapter {

    protected final T delegate;

    public DelegatingJobProcessor(@NotNull T delegate) {
        this.delegate = delegate;
    }

    public T getDelegate() {
        return delegate;
    }

    @Override
    public void start() {
        if (!started.getAndSet(true)) {
            delegate.start();
            finished.set(false);
            Job startJob = new Job() {
                @Override
                protected void execute() throws Throwable {
                    processorStarted();
                }
            };
            startJob.setProcessor(this);
            delegate.queue(startJob, Priority.highest);
        }
    }

    @Override
    public void finish() {
        if (started.get() && !finished.getAndSet(true)) {
            delegate.waitForLatchJob(new LatchJob() {
                @Override
                protected void execute() throws Throwable {
                    try {
                        processorFinished();
                    } catch (Throwable t) {
                        final JobProcessorExceptionHandler exceptionHandler = getExceptionHandler();
                        if (exceptionHandler != null) {
                            exceptionHandler.handle(DelegatingJobProcessor.this, this, t);
                        }
                    } finally {
                        release();
                    }
                }
            }, 100);
            setExceptionHandler(null);
            started.set(false);
        }
    }

    @Override
    public int pendingJobs() {
        // this value is not relevant, but we can't afford its graceful evaluation
        return delegate.pendingJobs();
    }

    @Override
    public void waitForJobs(long spinTimeout) {
        delegate.waitForJobs(spinTimeout);
    }

    @Override
    public void waitForTimedJobs(long spinTimeout) {
        delegate.waitForTimedJobs(spinTimeout);
    }

    @Nullable
    @Override
    public Job getCurrentJob() {
        return delegate.getCurrentJob();
    }

    @Override
    public long getCurrentJobStartedAt() {
        return delegate.getCurrentJobStartedAt();
    }

    @NotNull
    @Override
    public Iterable<Job> getPendingJobs() {
        return delegate.getPendingJobs();
    }

    @Override
    public int pendingTimedJobs() {
        // this value is not relevant, but we can't afford its graceful evaluation
        return delegate.pendingTimedJobs();
    }

    public String toString() {
        return "delegating -> " + delegate;
    }

    @Override
    protected boolean queueLowest(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean queueLowestTimed(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean push(Job job, Priority priority) {
        if (isFinished()) return false;
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        return delegate.push(job, priority);
    }

    @Override
    protected Job pushAt(Job job, long millis) {
        if (isFinished()) return null;
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        return delegate.pushAt(job, millis);
    }

}
