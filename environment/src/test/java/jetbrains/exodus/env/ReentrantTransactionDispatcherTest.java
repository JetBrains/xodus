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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
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
        // nested transaction always gets 1 permit
        Assert.assertEquals(8, dispatcher.getAvailablePermits());
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
}
