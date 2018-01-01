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
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;

public abstract class MultiThreadDelegatingJobProcessor extends JobProcessorAdapter {

    @NonNls
    private static final String UNSUPPORTED_TIMED_JOBS_MESSAGE = "Timed jobs are not supported by MultiThreadDelegatingJobProcessor";

    protected final ThreadJobProcessor[] jobProcessors;

    protected MultiThreadDelegatingJobProcessor(String name, int threadCount) {
        jobProcessors = new ThreadJobProcessor[threadCount];
        for (int i = 0; i < jobProcessors.length; i++) {
            jobProcessors[i] = ThreadJobProcessorPool.getOrCreateJobProcessor(name + i);
        }
    }

    @Override
    protected Job pushAt(Job job, long millis) {
        throw new UnsupportedOperationException(UNSUPPORTED_TIMED_JOBS_MESSAGE);
    }

    @Override
    public void waitForJobs(long spinTimeout) {
        for (final ThreadJobProcessor processor : jobProcessors) {
            processor.waitForJobs(spinTimeout);
        }
    }

    @Override
    public void waitForTimedJobs(long spinTimeout) {
        for (final ThreadJobProcessor processor : jobProcessors) {
            processor.waitForTimedJobs(spinTimeout);
        }
    }

    @Override
    protected boolean queueLowest(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean queueLowestTimed(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Job getCurrentJob() {
        return null;
    }

    @Override
    public long getCurrentJobStartedAt() {
        return 0;
    }

    @NotNull
    @Override
    public Iterable<Job> getPendingJobs() {
        return Collections.emptyList();
    }

    @Override
    public int pendingTimedJobs() {
        return 0;
    }

    @Override
    public void start() {
        if (!started.getAndSet(true)) {
            finished.set(false);
            for (final ThreadJobProcessor jobProcessor : jobProcessors) {
                jobProcessor.start();
            }
        }
    }

    @Override
    protected final void processorStarted() {
    }

    @Override
    protected final void processorFinished() {
    }

    @Override
    public void finish() {
        if (started.get() && !finished.getAndSet(true)) {
            for (final ThreadJobProcessor processor : jobProcessors) {
                // wait for each processor to execute current job (to prevent us from shutting down
                // while our job is being executed right now)
                processor.waitForLatchJob(new LatchJob() {
                    @Override
                    protected void execute() throws Throwable {
                        release();
                    }
                }, 100);
            }
            started.set(false);
        }
    }

    public boolean isDispatcherThread() {
        final long currentThreadId = Thread.currentThread().getId();
        for (final ThreadJobProcessor processor : jobProcessors) {
            if (currentThreadId == processor.getId()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int pendingJobs() {
        int result = 0;
        for (final ThreadJobProcessor processor : jobProcessors) {
            result += processor.pendingJobs();
        }
        return result;
    }

    public int getThreadCount() {
        return jobProcessors.length;
    }

    @Override
    protected boolean push(final Job job, final Priority priority) {
        if (isFinished()) return false;
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        final int hc = job.hashCode();
        final int processorNumber = ((hc & 0xffff) + (hc >>> 16)) % jobProcessors.length;
        return jobProcessors[processorNumber].queue(job, priority);
    }

}
