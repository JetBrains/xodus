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
package jetbrains.exodus.gc;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.execution.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BackgroundCleaner {

    private static final Logger logger = LoggerFactory.getLogger(BackgroundCleaner.class);

    @NotNull
    private final GarbageCollector gc;
    @NotNull
    private final BackgroundCleaningJob backgroundCleaningJob;
    @NotNull
    private JobProcessorAdapter processor;
    private long threadId;

    private volatile boolean isSuspended;
    private volatile boolean isCleaning;

    // background cleaner is always created suspended
    BackgroundCleaner(@NotNull final GarbageCollector gc) {
        this.gc = gc;
        backgroundCleaningJob = new BackgroundCleaningJob(gc);
        if (gc.getEnvironment().getEnvironmentConfig().isLogCacheShared()) {
            setJobProcessor(new DelegatingJobProcessor<>(
                    ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared background cleaner")));
        } else {
            setJobProcessor(new ThreadJobProcessor("Exodus background cleaner for " + gc.getEnvironment().getLocation()));
        }
    }

    void setJobProcessor(@NotNull final JobProcessorAdapter processor) {
        JobProcessorAdapter result = null;
        if (processor instanceof ThreadJobProcessor) {
            result = processor;
            threadId = ((ThreadJobProcessor) processor).getId();
        } else if (processor instanceof DelegatingJobProcessor) {
            final JobProcessorAdapter delegate = ((DelegatingJobProcessor) processor).getDelegate();
            if (delegate instanceof ThreadJobProcessor) {
                result = processor;
                threadId = ((ThreadJobProcessor) delegate).getId();
            }
        }
        if (result == null) {
            throw new ExodusException("Unexpected job processor: " + processor);
        }
        if (result.getExceptionHandler() == null) {
            result.setExceptionHandler(new JobProcessorExceptionHandler() {
                @Override
                public void handle(JobProcessor processor, Job job, Throwable t) {
                    logger.error(t.getMessage(), t);
                }
            });
        }
        result.start();
        this.processor = result;
    }

    @NotNull
    JobProcessorAdapter getJobProcessor() {
        return processor;
    }

    void finish() {
        backgroundCleaningJob.cancel();
        processor.waitForLatchJob(new LatchJob() {
            @Override
            protected void execute() throws Throwable {
                try {
                    gc.deletePendingFiles();
                } finally {
                    release();
                }
            }
        }, 100);
        processor.finish();
    }

    boolean isFinished() {
        return processor.isFinished();
    }

    void suspend() {
        synchronized (backgroundCleaningJob) {
            if (!isSuspended) {
                isSuspended = true;
            }
        }
    }

    void resume() {
        synchronized (backgroundCleaningJob) {
            isSuspended = false;
        }
    }

    boolean isSuspended() {
        return isSuspended;
    }

    boolean isCurrentThread() {
        return threadId == Thread.currentThread().getId();
    }

    void queueCleaningJob() {
        if (gc.getEnvironment().getEnvironmentConfig().isGcEnabled()) {
            processor.queue(backgroundCleaningJob);
        }
    }

    void queueCleaningJobAt(final long millis) {
        if (gc.getEnvironment().getEnvironmentConfig().isGcEnabled()) {
            processor.queueAt(backgroundCleaningJob, millis);
        }
    }

    boolean isCleaning() {
        return isCleaning;
    }

    void setCleaning(boolean isCleaning) {
        this.isCleaning = isCleaning;
    }

    void cleanWholeLog() {
        processor.waitForLatchJob(new CleanWholeLogJob(gc), 0);
    }

    void checkThread() {
        if (!isCurrentThread()) {
            throw new ExodusException("Background cleaner thread expected as current one");
        }
    }
}
