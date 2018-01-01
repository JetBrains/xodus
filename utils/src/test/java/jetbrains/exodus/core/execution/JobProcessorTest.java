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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobProcessorTest {

    private ThreadJobProcessor processor;

    private void createProcessor() {
        processor = new ThreadJobProcessor("Test Job Processor");
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(JobProcessorQueueAdapter.CONCURRENT_QUEUE_PROPERTY, "false");
    }

    @Test
    public void sleep() throws InterruptedException {
        createProcessor();
        processor.start();
        final long ticks = System.currentTimeMillis();
        final int sleepTicks = 2000;
        new SleepJob(processor, sleepTicks);
        Thread.sleep(500);
        processor.queueFinish();
        processor.waitUntilFinished();
        Assert.assertTrue(System.currentTimeMillis() - sleepTicks >= ticks);
    }

    @Test
    public void testWaitForJobs() throws InterruptedException {
        createProcessor();
        processor.start();
        for (int i = 0; i < 100; i++) { // this should be fast when everything waitForJobs works properly
            long time = System.currentTimeMillis();
            processor.queue(new IncrementJob(processor, 1), Priority.lowest);
            processor.waitForJobs(1000);
            time = System.currentTimeMillis() - time;
            Assert.assertTrue("Waiting took: " + time + " on iteration " + i, time < 500);
        }
    }

    @Test
    public void testWaitForTimedJobs() throws InterruptedException {
        createProcessor();
        processor.start();
        for (int i = 0; i < 10; i++) { // this should be fast when everything waitForJobs works properly
            long time = System.currentTimeMillis();
            processor.queueIn(new IncrementJob(processor, 1), 100);
            processor.waitForTimedJobs(1000);
            time = System.currentTimeMillis() - time;
            Assert.assertTrue("Waiting took: " + time + " on iteration " + i, time < 500);
        }
    }

    @Test
    public void testSuspend() throws InterruptedException {
        createProcessor();
        processor.start();
        processor.suspend();
        count = 0;
        processor.queue(new IncrementJob(processor, 1));
        Thread.sleep(200);
        Assert.assertEquals(0, count);
        processor.resume();
        processor.waitForJobs(10);
        Assert.assertEquals(1, count);
    }

    @Test
    public void testSuspend2() throws InterruptedException {
        createProcessor();
        processor.start();
        processor.suspend();
        count = 0;
        processor.queue(new IncrementJob(processor, 1));
        Thread.sleep(200);
        Assert.assertEquals(0, count);
        processor.resume();
        ResumeRunnable r = new ResumeRunnable(processor);
        Thread resumer = new Thread(r);
        resumer.start();
        processor.waitForJobs(10);
        Assert.assertEquals(1, count);
        Thread.sleep(200);
        if (r.state == ResumeRunnableState.INITIAL) {
            resumer.interrupt();
            Thread.sleep(200);
            final ResumeRunnableState state = r.state;
            if (state != ResumeRunnableState.RESUMED) {
                Assert.fail("Can't resume in other thread: " + state);
            }
        }
    }

    @Test
    public void testSuspend3() throws InterruptedException {
        createProcessor();
        processor.start();
        processor.suspend();
        SuspendRunnable r = new SuspendRunnable(processor);
        Thread suspender = new Thread(r);
        suspender.start();
        Thread.sleep(200);
        if (r.state == SuspendRunnableState.INITIAL) {
            suspender.interrupt();
            Thread.sleep(200);
            final SuspendRunnableState state = r.state;
            if (state != SuspendRunnableState.SUSPENDED) {
                Assert.fail("Can't suspend in other thread: " + state);
            }
        }
    }

    @Test
    public void testSuspend4() throws InterruptedException {
        createProcessor();
        processor.start();
        for (int i = 0; i < 100; ++i) {
            processor.suspend();
            processor.resume();
        }
    }

    @Test
    public void testSuspend5() throws InterruptedException {
        createProcessor();
        processor.start();
        for (int i = 0; i < 100; ++i) {
            processor.suspend();
            processor.suspend();
            processor.resume();
            processor.suspend();
        }
    }

    @Test
    public void equalJobs() throws InterruptedException {
        createProcessor();
        new AlwaysEqualJob(processor);
        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(new AlwaysEqualJob(processor).wasQueued());
        }
        processor.start();
        Thread.sleep(200);
        Assert.assertTrue(new AlwaysEqualJob(processor).wasQueued());
        processor.finish();
        // do not queue after processor has finished
        Assert.assertFalse(new AlwaysEqualJob(processor).wasQueued());
    }

    @Test
    public void priorityOrder() throws InterruptedException {
        createProcessor();
        count = 0;
        processor.start();
        AcquiringLatchJob job = new AcquiringLatchJob(processor);
        new IncrementJob(processor, 1);
        new SleepJob(processor, 300);
        new IncrementJob(processor, Priority.above_normal, 2);
        new SleepJob(processor, Priority.above_normal, 300);
        new IncrementJob(processor, Priority.highest, 3);
        new SleepJob(processor, Priority.highest, 300);
        job.release();
        Thread.sleep(200);
        Assert.assertEquals(3, count);
        Thread.sleep(300);
        Assert.assertEquals(5, count);
        Thread.sleep(300);
        Assert.assertEquals(6, count);
    }

    @Test
    public void pendingJobs() throws InterruptedException {
        createProcessor();
        count = 0;
        processor.start();
        AcquiringLatchJob job = new AcquiringLatchJob(processor);
        new IncrementJob(processor, 1);
        new SleepJob(processor, 1000);
        new IncrementJob(processor, Priority.above_normal, 2);
        new SleepJob(processor, Priority.above_normal, 1000);
        new IncrementJob(processor, Priority.highest, 3);
        new SleepJob(processor, Priority.highest, 1000);
        // the following sleep() is necessary to make sure that AcquiringLatchJob is been executed
        // otherwise a race condition is possible resulting in processor.pendingJobs() returns 8 instead of 7
        Thread.sleep(1000);
        Assert.assertEquals(7, processor.pendingJobs());
        job.release();
        Thread.sleep(700);
        Assert.assertEquals(5, processor.pendingJobs());
        Thread.sleep(1000);
        Assert.assertEquals(3, processor.pendingJobs());
        Thread.sleep(1000);
        Assert.assertEquals(1, processor.pendingJobs());
        Thread.sleep(700);
        Assert.assertEquals(0, processor.pendingJobs());
        Assert.assertEquals(6, count);
    }

    @Test
    public void queueAtFuture() throws InterruptedException {
        createProcessor();
        processor.start();
        count = 0;
        processor.queueAt(new IncrementJob(1), System.currentTimeMillis() + 200);
        processor.queueAt(new IncrementJob(2), System.currentTimeMillis() + 400);
        processor.queueAt(new IncrementJob(3), System.currentTimeMillis() + 600);
        Assert.assertEquals(0, count);
        Thread.sleep(300);
        Assert.assertEquals(1, count);
        Thread.sleep(200);
        Assert.assertEquals(3, count);
        Thread.sleep(200);
        Assert.assertEquals(6, count);
    }

    @Test
    public void queueAtMerge() throws InterruptedException {
        createProcessor();
        processor.start();
        count = 0;
        final IncrementJob job = new IncrementJob(1);
        processor.queueAt(job, System.currentTimeMillis() + 200);
        processor.queueAt(job, System.currentTimeMillis() + 400);
        processor.queueAt(job, System.currentTimeMillis() + 600);
        Thread.sleep(500);
        Assert.assertEquals(0, count);
        Thread.sleep(200);
        Assert.assertEquals(1, count);
        processor.queueAt(job, System.currentTimeMillis() + 600);
        processor.queueAt(job, System.currentTimeMillis() + 100);
        Thread.sleep(200);
        Assert.assertEquals(2, count);
    }

    @Test
    public void queueIn() throws InterruptedException {
        createProcessor();
        processor.start();
        count = 0;
        final IncrementJob job = new IncrementJob(1);
        processor.queueIn(job, 200);
        Thread.sleep(100);
        Assert.assertEquals(0, count);
        Thread.sleep(200);
        Assert.assertEquals(1, count);
    }

    @Test
    public void testFairness() throws InterruptedException {
        createProcessor();
        processor.start();
        new SleepTimeAfterTimeJob("third", processor, 2500, 1000);
        new SleepTimeAfterTimeJob("second", processor, 1500, 1000);
        new SleepTimeAfterTimeJob("first", processor, 500, 1000);
        Thread.sleep(1000);
        Job currentJob = processor.getCurrentJob();
        Assert.assertNotNull(currentJob);
        Assert.assertEquals("first", ((SleepTimeAfterTimeJob) currentJob).name);
        Thread.sleep(1000);
        currentJob = processor.getCurrentJob();
        Assert.assertNotNull(currentJob);
        Assert.assertEquals("second", ((SleepTimeAfterTimeJob) currentJob).name);
        Thread.sleep(1000);
        currentJob = processor.getCurrentJob();
        Assert.assertNotNull(currentJob);
        Assert.assertEquals("third", ((SleepTimeAfterTimeJob) currentJob).name);
        processor.finish();
    }

    private static class AcquiringLatchJob extends LatchJob {

        AcquiringLatchJob(final JobProcessor processor) {
            try {
                acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                // ignore
            }
            processor.queue(this, Priority.highest);
        }

        @Override
        protected void execute() throws Throwable {
            acquire();
        }

    }

    private static class SleepJob extends Job {

        private final long ticks;

        SleepJob(final JobProcessor processor, final long ticks) {
            super(processor);
            this.ticks = ticks;
        }

        SleepJob(final JobProcessor processor, final Priority priority, final long ticks) {
            super(processor, priority);
            this.ticks = ticks;
        }

        @Override
        protected void execute() {
            try {
                Thread.sleep(ticks);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
    }

    private static class SleepTimeAfterTimeJob extends Job {

        private final long ticks;
        private final String name;

        SleepTimeAfterTimeJob(final String name, final JobProcessor processor, final long timeout, final long ticks) {
            this.ticks = ticks;
            this.name = name;
            processor.queueIn(this, timeout);
        }

        @Override
        protected void execute() {
            try {
                System.out.println("Sleeping: " + name);
                Thread.sleep(ticks);
                getProcessor().queueIn(this, ticks);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
    }

    private static int count = 0;

    @SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
    private static class IncrementJob extends Job {
        private final int increment;

        IncrementJob(final int increment) {
            this.increment = increment;
        }

        IncrementJob(final JobProcessor processor, final int increment) {
            super(processor);
            this.increment = increment;
        }

        IncrementJob(final JobProcessor processor, final Priority priority, final int increment) {
            super(processor, priority);
            this.increment = increment;
        }

        @Override
        protected void execute() {
            long before = System.nanoTime();
            count += increment;
            System.out.println("Increment executed in: " + (System.nanoTime() - before) + "ns.");
        }
    }

    /**
     * All jobs of this type are always equal
     */
    private static class AlwaysEqualJob extends Job {

        private AlwaysEqualJob(final JobProcessor processor) {
            super(processor);
        }

        @Override
        public boolean isEqualTo(Job job) {
            return true;
        }

        public int hashCode() {
            return 0;
        }

        @Override
        protected void execute() {
        }
    }

    private static class ResumeRunnable implements Runnable {
        private volatile ResumeRunnableState state = ResumeRunnableState.INITIAL;
        private final JobProcessor processor;

        private ResumeRunnable(JobProcessor processor) {
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.resume();
                state = ResumeRunnableState.RESUMED;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                state = ResumeRunnableState.INTERRUPTED;
                e.printStackTrace();
            }
        }
    }

    private enum ResumeRunnableState {
        INITIAL(),
        INTERRUPTED(),
        RESUMED()
    }

    private static class SuspendRunnable implements Runnable {
        private volatile SuspendRunnableState state = SuspendRunnableState.INITIAL;
        private final JobProcessor processor;

        private SuspendRunnable(JobProcessor processor) {
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.suspend();
                state = SuspendRunnableState.SUSPENDED;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                state = SuspendRunnableState.INTERRUPTED;
                e.printStackTrace();
            }
        }
    }

    private enum SuspendRunnableState {
        INITIAL(),
        INTERRUPTED(),
        SUSPENDED()
    }
}
