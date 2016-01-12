/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public abstract class JobProcessorQueueAdapter extends JobProcessorAdapter {

    public static final String CONCURRENT_QUEUE_PROPERTY = "jetbrains.exodus.core.execution.concurrentQueue";

    private final PriorityQueue<Priority, Job> queue;
    private final PriorityQueue<Long, Job> timeQueue;
    private volatile int outdatedJobsCount;
    private volatile Job currentJob;
    protected final Semaphore awake;

    @SuppressWarnings("unchecked")
    protected JobProcessorQueueAdapter() {
        queue = createQueue();
        timeQueue = createQueue();
        awake = new Semaphore(0);
    }

    @Override
    protected boolean queueLowest(@NotNull Job job) {
        if (isFinished()) return false;
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        queue.lock();
        try {
            final Pair<Priority, Job> pair = queue.floorPair();
            final Priority priority = pair == null ? Priority.highest : pair.getFirst();
            if (queue.push(priority, job) != null) {
                return false;
            }
        } finally {
            queue.unlock();
        }
        awake.release();
        return true;
    }

    @Override
    protected boolean push(final Job job, final Priority priority) {
        if (isFinished()) return false;
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        queue.lock();
        try {
            if (queue.push(priority, job) != null) {
                return false;
            }
        } finally {
            queue.unlock();
        }
        awake.release();
        return true;
    }

    @Override
    protected Job pushAt(final Job job, final long millis) {
        if (isFinished()) {
            return null;
        }
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        Job oldJob;
        final Pair<Long, Job> pair;
        final long priority = Long.MAX_VALUE - millis;
        timeQueue.lock();
        try {
            oldJob = timeQueue.push(priority, job);
            pair = timeQueue.peekPair();
        } finally {
            timeQueue.unlock();
        }
        if (pair != null && pair.getFirst() != priority) {
            return oldJob;
        }
        awake.release();
        return oldJob;
    }

    @Override
    protected boolean queueLowestTimed(@NotNull final Job job) {
        if (isFinished()) {
            return false;
        }
        if (job.getProcessor() == null) {
            job.setProcessor(this);
        }
        timeQueue.lock();
        try {
            final Pair<Long, Job> pair = timeQueue.floorPair();
            final long priority = pair == null ? Long.MAX_VALUE - System.currentTimeMillis() : pair.getFirst();
            if (timeQueue.push(priority, job) != null) {
                return false;
            }
        } finally {
            timeQueue.unlock();
        }
        awake.release();
        return true;
    }

    @Override
    public int pendingJobs() {
        return queue.size() + (currentJob == null ? 0 : 1);
    }

    @Override
    public int pendingTimedJobs() {
        return timeQueue.size() + outdatedJobsCount;
    }

    @Nullable
    @Override
    public Job getCurrentJob() {
        return currentJob;
    }

    @NotNull
    @Override
    public Iterable<Job> getPendingJobs() {
        return queue;
    }

    protected void doJobs() {
        final boolean jobsQueued;
        try {
            jobsQueued = waitForJobs();
        } catch (InterruptedException e) {
            return;
        }
        if (!isFinished()) {
            if (jobsQueued) {
                final Job job;
                queue.lock();
                try {
                    job = queue.pop();
                } finally {
                    queue.unlock();
                }
                doExecuteJob(job);
            } else {
                doTimedJobs();
            }
        }
    }

    protected void clearQueues() {
        queue.clear();
        timeQueue.clear();
    }

    protected void doTimedJobs() {
        final Collection<Job> outdatedJobs = new ArrayList<>();
        final long currentTimePriority = Long.MAX_VALUE - System.currentTimeMillis();
        final int count;
        timeQueue.lock();
        try {
            Pair<Long, Job> pair = timeQueue.peekPair();
            while (pair != null && pair.getFirst() >= currentTimePriority) {
                outdatedJobs.add(timeQueue.pop());
                pair = timeQueue.peekPair();
            }
            count = outdatedJobs.size();
        } finally {
            timeQueue.unlock();
        }
        // outdatedJobsCount is updated in single thread, so we won't bother with synchronization on its update
        outdatedJobsCount = count;
        for (final Job job : outdatedJobs) {
            executeImmediateJobsIfAny();
            if (isFinished()) {
                break;
            }
            doExecuteJob(job);
            --outdatedJobsCount;
        }
    }

    private void executeImmediateJobsIfAny() {
        //noinspection StatementWithEmptyBody
        while (!isFinished() && executeImmediateJobIfAny() != null) ;
    }

    /**
     * @return executed job or null if it was nothing to execute.
     */
    private Job executeImmediateJobIfAny() {
        Job urgentImmediateJob = null;
        queue.lock();
        try {
            final Pair<Priority, Job> peekPair = queue.peekPair();
            if (peekPair != null && peekPair.getFirst() == Priority.highest) {
                urgentImmediateJob = queue.pop();
            }
        } finally {
            queue.unlock();
        }
        if (urgentImmediateJob != null) {
            doExecuteJob(urgentImmediateJob);
        }
        return urgentImmediateJob;
    }

    // returns true if a job was queued within a timeout
    protected boolean waitForJobs() throws InterruptedException {
        final Pair<Long, Job> peekPair;
        timeQueue.lock();
        try {
            peekPair = timeQueue.peekPair();
        } finally {
            timeQueue.unlock();
        }
        if (peekPair == null) {
            awake.acquire();
            return true;
        }
        final long timeout = Long.MAX_VALUE - peekPair.getFirst() - System.currentTimeMillis();
        if (timeout < 0) {
            return false;
        }
        return awake.tryAcquire(timeout, TimeUnit.MILLISECONDS);
    }

    private void doExecuteJob(final Job job) {
        currentJob = job;
        try {
            executeJob(job);
        } finally {
            currentJob = null;
        }
    }

    @SuppressWarnings("rawtypes")
    private static PriorityQueue createQueue() {
        final String concurrentQueueProperty = System.getProperty(CONCURRENT_QUEUE_PROPERTY);
        if (concurrentQueueProperty != null && "false".equalsIgnoreCase(concurrentQueueProperty)) {
            return new StablePriorityQueue();
        }
        return new ConcurrentStablePriorityQueue();
    }
}