/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class ReentrantTransactionDispatcherTest {
    private var dispatcher: ReentrantTransactionDispatcher? = null
    @Before
    fun setUp() {
        dispatcher = ReentrantTransactionDispatcher(10)
    }

    @Test(expected = IllegalArgumentException::class)
    fun createDispatcher() {
        ReentrantTransactionDispatcher(0)
    }

    @Test(expected = ExodusException::class)
    fun cantAcquireMoreTransaction() {
        dispatcher!!.acquireExclusiveTransaction(Thread.currentThread())
        Assert.assertEquals(0, dispatcher!!.getAvailablePermits().toLong())
        dispatcher!!.acquireTransaction(Thread.currentThread())
    }

    @Test(expected = ExodusException::class)
    fun cantReleaseMorePermits() {
        dispatcher!!.acquireTransaction(Thread.currentThread())
        dispatcher!!.releaseTransaction(Thread.currentThread(), 2)
    }

    @Test
    fun exclusiveTransaction() {
        dispatcher!!.acquireExclusiveTransaction(Thread.currentThread())
        Assert.assertEquals(0, dispatcher!!.getAvailablePermits().toLong())
    }

    @Test
    fun exclusiveTransaction2() {
        dispatcher!!.acquireTransaction(Thread.currentThread())
        Assert.assertEquals(9, dispatcher!!.getAvailablePermits().toLong())
        dispatcher!!.acquireExclusiveTransaction(Thread.currentThread())
        // nested transaction always gets 1 permit
        Assert.assertEquals(8, dispatcher!!.getAvailablePermits().toLong())
    }

    @Test
    fun downgrade() {
        exclusiveTransaction()
        dispatcher!!.downgradeTransaction(Thread.currentThread(), 9)
        Assert.assertEquals(8, dispatcher!!.getAvailablePermits().toLong())
        dispatcher!!.downgradeTransaction(Thread.currentThread(), 1)
        Assert.assertEquals(8, dispatcher!!.getAvailablePermits().toLong())
        dispatcher!!.downgradeTransaction(Thread.currentThread(), 2)
        Assert.assertEquals(9, dispatcher!!.getAvailablePermits().toLong())
    }
}
