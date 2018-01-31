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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"ProtectedField"})
public abstract class JobProcessorAdapter implements JobProcessor {

    protected final AtomicBoolean started;
    protected final AtomicBoolean finished;
    @Nullable
    protected JobProcessorExceptionHandler exceptionHandler;
    @Nullable
    protected JobHandler[] jobStartingHandlers;
    @Nullable
    protected JobHandler[] jobFinishedHandlers;
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @NotNull
    private final Semaphore suspendSemaphore;
    @NotNull
    private final SuspendLatchJob suspendLatchJob = new SuspendLatchJob();
    @NotNull
    private final ResumeLatchJob resumeLatchJob = new ResumeLatchJob();
    @NotNull
    private final WaitJob waitJob = new WaitJob();
    @NotNull
    private final WaitJob timedWaitJob = new WaitJob();
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private boolean isSuspended;

    protected JobProcessorAdapter() {
        started = new AtomicBoolean(false);
        finished = new AtomicBoolean(true);
        suspendSemaphore = new Semaphore(1, true);
        isSuspended = false;
    }

    @Override
    public boolean isFinished() {
        return finished.get();
    }

    @Override
    public void setExceptionHandler(@Nullable final JobProcessorExceptionHandler handler) {
        exceptionHandler = handler;
    }

    @Override
    @Nullable
    public JobProcessorExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Queues a job for execution with the normal (default) priority.
     *
     * @param job job to execute.
     * @return true if the job was actually queued, else if it was merged with an equal job queued earlier
     * and still not executed.
     */
    @Override
    public final boolean queue(final Job job) {
        return push(job, Priority.normal);
    }

    /**
     * Queues a job for execution with specified priority.
     *
     * @param job      job to execute.
     * @param priority priority if the job in the job queue.
     * @return true if the job was actually queued, else if it was merged with an equal job queued earlier
     * and still not executed.
     */
    @Override
    public final boolean queue(final Job job, final Priority priority) {
        return push(job, priority);
    }

    /**
     * Queues a job for execution at specified time.
     *
     * @param job    the job.
     * @param millis time to execute the job.
     */
    @Override
    public final Job queueAt(final Job job, final long millis) {
        return pushAt(job, millis);
    }

    /**
     * Queues a job for execution in specified time.
     *
     * @param job    the job.
     * @param millis execute the job in this time.
     */
    @Override
    public final Job queueIn(final Job job, final long millis) {
        return pushAt(job, System.currentTimeMillis() + millis);
    }

    @Override
    public void finish() {
        waitJob.release();
        timedWaitJob.release();
    }

    @Override
    public void waitForJobs(final long spinTimeout) {
        waitForJobs(waitJob, false, spinTimeout);
    }

    @Override
    public void waitForTimedJobs(final long spinTimeout) {
        waitForJobs(timedWaitJob, true, spinTimeout);
    }

    @Override
    public void suspend() throws InterruptedException {
        synchronized (suspendLatchJob) {
            if (!isSuspended) {
                if (suspendSemaphore.tryAcquire(0, TimeUnit.SECONDS)) {
                    if (waitForLatchJob(suspendLatchJob, 100)) {
                        suspendLatchJob.release();
                    }
                } else {
                    throw new IllegalStateException("Can't acquire suspend semaphore!");
                }
            }
        }
    }

    @Override
    public void resume() throws InterruptedException {
        synchronized (suspendLatchJob) {
            if (isSuspended) {
                suspendSemaphore.release();
                if (waitForLatchJob(resumeLatchJob, 100)) {
                    resumeLatchJob.release();
                }
            }
        }
    }

    @Override
    public boolean isSuspended() {
        return isSuspended;
    }

    @Override
    public void beforeProcessingJob(@NotNull final Job job) {
    }

    @Override
    public void afterProcessingJob(@NotNull final Job job) {
    }

    protected abstract boolean queueLowest(@NotNull final Job job);

    protected abstract boolean queueLowestTimed(@NotNull final Job job);

    public boolean waitForLatchJob(final LatchJob latchJob, final long spinTimeout, final Priority priority) {
        if (!acquireLatchJob(latchJob, spinTimeout)) {
            return false;
        }
        // latchJob is queued without processor to ensure delegate will execute it
        if (queue(latchJob, priority)) {
            if (acquireLatchJob(latchJob, spinTimeout)) {
                return true;
            }
        }
        return false;
    }

    public boolean waitForLatchJob(final LatchJob latchJob, final long spinTimeout) {
        return waitForLatchJob(latchJob, spinTimeout, Priority.highest);
    }

    protected abstract boolean push(Job job, Priority priority);

    protected abstract Job pushAt(Job job, long millis);

    protected void processorStarted() {
    }

    protected void processorFinished() {
    }

    protected void executeJob(@Nullable final Job job) {
        if (job != null) {
            JobProcessor processor = job.getProcessor();
            if (processor != null && !processor.isFinished()) {
                JobProcessorExceptionHandler exceptionHandler = processor.getExceptionHandler();
                try {
                    processor.beforeProcessingJob(job);
                    JobHandler.invokeHandlers(jobStartingHandlers, job);
                    try {
                        job.run(exceptionHandler);
                    } finally {
                        JobHandler.invokeHandlers(jobFinishedHandlers, job);
                    }
                    processor.afterProcessingJob(job);
                } catch (Throwable t) {
                    handleThrowable(job, exceptionHandler, t);
                }
            }
        }
    }

    void handleThrowable(Job job, JobProcessorExceptionHandler exceptionHandler, Throwable t) {
        if (exceptionHandler != null) {
            try {
                exceptionHandler.handle(this, job, t);
            } catch (Throwable tt) {
                //noinspection CallToPrintStackTrace
                t.printStackTrace();
            }
        } else {
            //noinspection CallToPrintStackTrace
            t.printStackTrace();
        }
    }

    private void waitForJobs(@NotNull final LatchJob waitJob, final boolean timed, final long spinTimeout) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (waitJob) {
            try {
                if (!acquireLatchJob(waitJob, spinTimeout)) {
                    return;
                }
                // latchJob is queued without processor to ensure delegate will execute it
                if (timed ? queueLowestTimed(waitJob) : queueLowest(waitJob)) {
                    acquireLatchJob(waitJob, spinTimeout);
                }
            } finally {
                waitJob.release();
            }
        }
    }

    private boolean acquireLatchJob(@NotNull final LatchJob waitJob, final long spinTimeout) {
        while (!isFinished()) {
            try {
                if (waitJob.acquire(spinTimeout)) {
                    return true;
                }
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
        return false;
    }

    private static final class WaitJob extends LatchJob {
        @Override
        protected void execute() throws Throwable {
            release();
        }
    }

    @SuppressWarnings({"EqualsAndHashcode"})
    private final class SuspendLatchJob extends LatchJob {
        @Override
        protected void execute() throws InterruptedException {
            isSuspended = true;
            release();
            suspendSemaphore.acquire();
            suspendSemaphore.release();
        }

        @Override
        public boolean isEqualTo(Job job) {
            return true;
        }

        @Override
        public int hashCode() {
            return 239;
        }
    }

    @SuppressWarnings({"EqualsAndHashcode"})
    private final class ResumeLatchJob extends LatchJob {

        @Override
        protected void execute() {
            isSuspended = false;
            release();
        }

        @Override
        public boolean isEqualTo(Job job) {
            return true;
        }

        @Override
        public int hashCode() {
            return 239;
        }
    }
}
