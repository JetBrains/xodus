/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

final class BackgroundCleaner {

    private static final Log logging = LogFactory.getLog(BackgroundCleaner.class);

    @NotNull
    private final JobProcessorAdapter processor;
    private final long threadId;
    @NotNull
    private final GarbageCollector gc;
    @NotNull
    private final BackgroundCleaningJob backgroundCleaningJob;
    private volatile boolean isSuspended;

    // background cleaner is always created suspended
    BackgroundCleaner(@NotNull final GarbageCollector gc) {
        if (gc.getEnvironment().getEnvironmentConfig().isLogCacheShared()) {
            final ThreadJobProcessor threadJobProcessor =
                    ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared background cleaner");
            threadId = threadJobProcessor.getId();
            //noinspection unchecked
            processor = new DelegatingJobProcessor<ThreadJobProcessor>(threadJobProcessor);
            processor.start();
        } else {
            final ThreadJobProcessor threadJobProcessor =
                    new ThreadJobProcessor("Exodus background cleaner for " + gc.getEnvironment().getLocation());
            threadJobProcessor.start();
            threadId = threadJobProcessor.getId();
            processor = threadJobProcessor;
        }
        processor.setExceptionHandler(new JobProcessorExceptionHandler() {
            @Override
            public void handle(JobProcessor processor, Job job, Throwable t) {
                logging.error(t, t);
            }
        });
        this.gc = gc;
        backgroundCleaningJob = new BackgroundCleaningJob(gc);
    }

    void finish() {
        backgroundCleaningJob.cancel();
        processor.finish();
    }

    boolean isFinished() {
        return processor.isFinished();
    }

    void suspend() {
        synchronized (backgroundCleaningJob) {
            if (!isSuspended) {
                isSuspended = true;
                // wait for trivial latch job in order to make sure that last started cleaning job has finished
                processor.waitForLatchJob(new LatchJob() {
                    @Override
                    protected void execute() throws Throwable {
                        release();
                    }
                }, 100);
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
            processor.queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    processor.queue(backgroundCleaningJob);
                }
            });
        }
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
