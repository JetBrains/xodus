/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.core.execution.locks.Latch;
import org.junit.Assert;
import org.junit.Test;

public class ReentrantTransactionDispatcherTest {

    @Test(expected = IllegalArgumentException.class)
    public void createDispatcher() {
        new ReentrantTransactionDispatcher(0);
    }

    @Test(expected = ExodusException.class)
    public void cantAcquireMoreTransaction() {
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(10);
        dispatcher.acquireTransaction(Thread.currentThread(), true);
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
        dispatcher.acquireTransaction(Thread.currentThread(), false);
    }

    @Test(expected = ExodusException.class)
    public void cantReleaseMorePermits() {
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(10);
        final int permits = dispatcher.acquireTransaction(Thread.currentThread(), false);
        dispatcher.releaseTransaction(Thread.currentThread(), permits + 1);
    }

    @Test
    public void exclusiveTransaction() {
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(10);
        dispatcher.acquireTransaction(Thread.currentThread(), true);
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
    }

    @Test
    public void exclusiveTransaction2() {
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(10);
        dispatcher.acquireTransaction(Thread.currentThread(), false);
        Assert.assertEquals(9, dispatcher.getAvailablePermits());
        dispatcher.acquireTransaction(Thread.currentThread(), true);
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
    }

    @Test
    public void fairness() throws InterruptedException {
        final int maxTransactions = 20;
        final ReentrantTransactionDispatcher dispatcher = new ReentrantTransactionDispatcher(maxTransactions);
        final int permits = dispatcher.acquireTransaction(Thread.currentThread(), true);
        Assert.assertEquals(0, dispatcher.getAvailablePermits());
        final Latch latch = Latch.create();
        final int[] count = {0};
        latch.acquire();
        final Thread anotherExclusiveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                latch.release();
                final Thread thread = Thread.currentThread();
                dispatcher.acquireTransaction(thread, true);
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
                    dispatcher.acquireTransaction(thread, false);
                    Assert.assertEquals(count[0]++, ii);
                    dispatcher.releaseTransaction(thread, 1);
                }
            }).start();
            latch.acquire();
            Thread.sleep(100);
        }
        dispatcher.releaseTransaction(Thread.currentThread(), permits);
        anotherExclusiveThread.join();
        Assert.assertEquals(maxTransactions, dispatcher.getAvailablePermits());
    }
}
