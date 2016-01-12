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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestFor;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.ThreadJobProcessor;
import jetbrains.exodus.core.execution.locks.Latch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReentrantTransactionDispatcherTest {

    private ReentrantTransactionDispatcher dispatcher;

    @Before
    public void setUp() {
        dispatcher = new ReentrantTransactionDispatcher(10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDispatcher() {
        new ReentrantTransactionDispatcher(0);
    }

    @Test(expected = ExodusException.class)
    public void cantAcquireMoreTransaction() {
        dispatcher.acquireExclusiveTransaction(Thread.currentThread());
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
        dispatcher.acquireTransaction(Thread.currentThread());
    }

    @Test(expected = ExodusException.class)
    public void cantReleaseMorePermits() {
        dispatcher.acquireTransaction(Thread.currentThread());
        dispatcher.releaseTransaction(Thread.currentThread(), 2);
    }

    @Test
    public void exclusiveTransaction() {
        dispatcher.acquireExclusiveTransaction(Thread.currentThread());
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
    }

    @Test
    public void exclusiveTransaction2() {
        dispatcher.acquireTransaction(Thread.currentThread());
        Assert.assertEquals(9, dispatcher.getAvailablePermits());
        dispatcher.acquireExclusiveTransaction(Thread.currentThread());
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
    }

    @Test
    public void downgrade() {
        exclusiveTransaction();
        dispatcher.downgradeTransaction(Thread.currentThread(), 9);
        Assert.assertEquals(8, dispatcher.getAvailablePermits());
        dispatcher.downgradeTransaction(Thread.currentThread(), 1);
        Assert.assertEquals(8, dispatcher.getAvailablePermits());
        dispatcher.downgradeTransaction(Thread.currentThread(), 2);
        Assert.assertEquals(9, dispatcher.getAvailablePermits());
    }

    @Test
    public void tryAcquireExclusiveTransaction() throws InterruptedException {
        exclusiveTransaction2();
        final Latch latch = Latch.create();
        latch.acquire();
        new Thread(new Runnable() {
            @Override
            public void run() {
                latch.release();
                dispatcher.acquireTransaction(Thread.currentThread());
                latch.release();
            }
        }).start();
        latch.acquire();
        Thread.sleep(100);
        new Thread(new Runnable() {
            @Override
            public void run() {
                Assert.assertEquals(0, dispatcher.tryAcquireExclusiveTransaction(Thread.currentThread(), 100));
                latch.release();
            }
        }).start();
        latch.acquire();
        dispatcher.releaseTransaction(Thread.currentThread(), 10);
        latch.acquire();
        Assert.assertEquals(0, dispatcher.acquirerCount());
        Assert.assertEquals(0, dispatcher.exclusiveAcquirerCount());
    }

    @Test
    public void fairness() throws InterruptedException {
        final int maxTransactions = 20;
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(maxTransactions);
        dispatcher.acquireTransaction(Thread.currentThread());
        dispatcher.acquireTransaction(Thread.currentThread());
        final Latch latch = Latch.create();
        final int[] count = {0};
        latch.acquire();
        final Thread anotherExclusiveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                latch.release();
                final Thread thread = Thread.currentThread();
                final int permits = dispatcher.acquireExclusiveTransaction(thread);
                Assert.assertEquals(maxTransactions, permits);
                Assert.assertEquals(0, dispatcher.getAvailablePermits());
                Assert.assertEquals(maxTransactions, count[0]);
                dispatcher.releaseTransaction(thread, maxTransactions);
            }
        });
        anotherExclusiveThread.start();
        latch.acquire();
        Thread.sleep(100);
        for (int i = 0; i < maxTransactions; ++i) {
            final int ii = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    latch.release();
                    final Thread thread = Thread.currentThread();
                    dispatcher.acquireTransaction(thread);
                    Assert.assertEquals(count[0]++, ii);
                    dispatcher.releaseTransaction(thread, 1);
                }
            }).start();
            latch.acquire();
            Thread.sleep(100);
        }
        dispatcher.releaseTransaction(Thread.currentThread(), 1);
        Thread.sleep(100);
        dispatcher.releaseTransaction(Thread.currentThread(), 1);
        anotherExclusiveThread.join();
        Assert.assertEquals(maxTransactions, dispatcher.getAvailablePermits());
    }

    @Test
    @TestFor(issues = "XD-489")
    public void xd_489() throws InterruptedException {
        final int count = 50;
        final JobProcessor[] processors = new JobProcessor[count];
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new ThreadJobProcessor("xd-489: " + i);
        }
        final long timeout = 2000;
        final int permits = dispatcher.acquireExclusiveTransaction(Thread.currentThread());
        try {
            for (int i = 0; i < count - 1; ++i) {
                processors[i].start();
                processors[i].queue(new Job() {
                    @Override
                    protected void execute() throws Throwable {
                        dispatcher.tryAcquireExclusiveTransaction(Thread.currentThread(), timeout);
                    }
                });
            }
            Thread.sleep(timeout / 2);
            processors[count - 1].start();
            processors[count - 1].queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    dispatcher.acquireTransaction(Thread.currentThread());

                }
            });
            Thread.sleep(timeout / 2);
        } finally {
            dispatcher.releaseTransaction(Thread.currentThread(), permits);
        }
        for (int i = 0; i < count - 1; ++i) {
            processors[i].finish();
        }
        Thread.sleep(timeout);
        Assert.assertEquals(0, dispatcher.acquirerCount());
    }
}
