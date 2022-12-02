package jetbrains.exodus.core.execution;

import jetbrains.exodus.core.dataStructures.Priority;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

public abstract class MultiThreadDelegatingJobProcessor extends JobProcessorAdapter {
    private final static String UNSUPPORTED_TIMED_JOBS_MESSAGE =
            "Timed jobs are not supported by MultiThreadDelegatingJobProcessor";

    private final int threadCount;

    private final AtomicReferenceArray<ThreadJobProcessor> jobProcessors;

    protected MultiThreadDelegatingJobProcessor(String name, int threadCount) {
        this(name, threadCount, 0);
    }

    protected MultiThreadDelegatingJobProcessor(String name, int threadCount, long jobTimeout) {
        this.threadCount = threadCount;

        if (jobTimeout > 0L) {
            SharedTimer.registerPeriodicTaskIn(new WatchDog(jobTimeout), jobTimeout);
        }

        jobProcessors = new AtomicReferenceArray<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            jobProcessors.set(i, ThreadJobProcessorPool.getOrCreateJobProcessor(name + i));
        }
    }

    @Override
    public void setExceptionHandler(@Nullable JobProcessorExceptionHandler handler) {
        super.setExceptionHandler(handler);
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobProcessors.get(i).setExceptionHandler(handler);
        }
    }

    @SuppressWarnings("unused")
    public void forEachSubProcessor(Consumer<ThreadJobProcessor> action) {
        for (int i = 0; i < jobProcessors.length(); i++) {
            action.accept(jobProcessors.get(i));
        }
    }

    @SuppressWarnings("unused")
    public Job[] currentJobs() {
        final Job[] jobs = new Job[threadCount];
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobs[i] = jobProcessors.get(i).getCurrentJob();
        }

        return jobs;
    }

    public boolean isDispatcherThread() {
        for (int i = 0; i < jobProcessors.length(); i++) {
            if (jobProcessors.get(i).isCurrentThread()) {
                return true;
            }
        }

        return false;
    }

    public int getThreadCount() {
        return threadCount;
    }

    @Override
    protected Job pushAt(Job job, long millis) {
        throw new UnsupportedOperationException(UNSUPPORTED_TIMED_JOBS_MESSAGE);
    }

    @Override
    public void waitForTimedJobs(long spinTimeout) {
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobProcessors.get(i).waitForTimedJobs(spinTimeout);
        }
    }

    @Override
    public void waitForJobs(long spinTimeout) {
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobProcessors.get(i).waitForJobs(spinTimeout);
        }
    }

    @Override
    public void suspend() throws InterruptedException {
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobProcessors.get(i).suspend();
        }
    }

    @Override
    public void resume() {
        for (int i = 0; i < jobProcessors.length(); i++) {
            jobProcessors.get(i).resume();
        }
    }

    @Override
    protected boolean queueLowestTimed(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean queueLowest(@NotNull Job job) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Job getCurrentJob() {
        return null;
    }

    @Override
    public long getCurrentJobStartedAt() {
        return 0;
    }

    @Override
    public int pendingTimedJobs() {
        return 0;
    }

    @Override
    public @NotNull Iterable<Job> getPendingJobs() {
        return Collections.emptyList();
    }

    @Override
    public void start() {
        if (!started.getAndSet(true)) {
            finished.set(false);
            for (int i = 0; i < jobProcessors.length(); i++) {
                final ThreadJobProcessor jobProcessor = jobProcessors.get(i);
                jobProcessor.start();
            }
        }
    }

    @Override
    public void finish() {
        if (started.get() && !finished.getAndSet(true)) {

            for (int i = 0; i < jobProcessors.length(); i++) {
                ThreadJobProcessor processor = jobProcessors.get(i);
                // wait for each processor to execute current job (to prevent us from shutting down
                // while our job is being executed right now)
                processor.waitForLatchJob(new LatchJob() {
                    @Override
                    protected void execute() {
                        release();
                    }
                }, 100);
            }

            started.set(false);
        }
    }

    @Override
    public int pendingJobs() {
        int jobs = 0;
        for (int i = 0; i < jobProcessors.length(); i++) {
            final ThreadJobProcessor processor = jobProcessors.get(i);
            jobs += processor.pendingJobs();
        }

        return jobs;
    }

    @Override
    protected boolean push(Job job, Priority priority) {
        if (isFinished()) {
            return false;
        }
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }

        int hc = job.hashCode();
        // if you change the way of computing processorNumber then make sure you've changed
        // EntityIterableAsyncInstantiation.hashCode() correspondingly
        int processorNumber = ((hc & 0xffff) + (hc >>> 16)) % jobProcessors.length();
        return job.queue(jobProcessors.get(processorNumber), priority);
    }

    private final class WatchDog implements SharedTimer.ExpirablePeriodicTask {
        private final long jobTimeout;

        private WatchDog(long jobTimeout) {
            this.jobTimeout = jobTimeout;
        }

        @Override
        public boolean isExpired() {
            return isFinished();
        }

        @Override
        public void run() {
            final long currentTime = System.currentTimeMillis();
            for (int i = 0; i < jobProcessors.length(); i++) {
                final ThreadJobProcessor processor = jobProcessors.get(i);
                final Job currentJob = processor.getCurrentJob();
                if (currentJob != null && currentJob.getStartedAt() + jobTimeout < currentTime) {
                    final ThreadJobProcessor newProcessor =
                            ThreadJobProcessorPool.getOrCreateJobProcessor(processor.getName() + '+');
                    jobProcessors.set(i, newProcessor);
                    newProcessor.setExceptionHandler(exceptionHandler);
                    processor.moveTo(newProcessor);
                    processor.queueFinish();
                }
            }
        }
    }

}
