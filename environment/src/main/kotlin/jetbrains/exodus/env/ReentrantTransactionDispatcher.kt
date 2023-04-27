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
import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.execution.locks.CriticalSection
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition

internal class ReentrantTransactionDispatcher(maxSimultaneousTransactions: Int) {
    private val availablePermits: Int
    private val threadPermits: MutableMap<Thread, Int>
    private val regularQueue: NavigableMap<Long, Condition>
    private val nestedQueue: NavigableMap<Long, Condition>
    private val criticalSection: CriticalSection
    private var acquireOrder: Long
    private var acquiredPermits: Int

    init {
        require(maxSimultaneousTransactions >= 1) { "maxSimultaneousTransactions < 1" }
        availablePermits = maxSimultaneousTransactions
        threadPermits = HashMap()
        regularQueue = TreeMap()
        nestedQueue = TreeMap()
        criticalSection = CriticalSection(false /* we explicitly don't need fairness here */)
        acquireOrder = 0
        acquiredPermits = 0
    }

    fun getAvailablePermits(): Int {
        criticalSection.enter().use { return availablePermits - acquiredPermits }
    }

    /**
     * Acquire transaction with a single permit in a thread. Transactions are acquired reentrantly, i.e.
     * with respect to transactions already acquired in the thread.
     *
     * @return the number of acquired permits, identically equal to 1.
     */
    fun acquireTransaction(thread: Thread): Int {
        criticalSection.enter().use {
            val currentThreadPermits = getThreadPermitsToAcquire(thread)
            waitForPermits(thread, if (currentThreadPermits > 0) nestedQueue else regularQueue, 1, currentThreadPermits)
        }
        return 1
    }

    /**
     * Acquire exclusive transaction in a thread. Transactions are acquired reentrantly, i.e. with respect
     * to transactions already acquired in the thread.
     * NB! Nested transaction is never acquired as exclusive.
     *
     * @return the number of acquired permits.
     */
    fun acquireExclusiveTransaction(thread: Thread): Int {
        criticalSection.enter().use {
            val currentThreadPermits = getThreadPermitsToAcquire(thread)
            // if there are no permits acquired in the thread then we can acquire exclusive txn, i.e. all available permits
            if (currentThreadPermits == 0) {
                waitForPermits(thread, regularQueue, availablePermits, 0)
                return availablePermits
            }
            waitForPermits(thread, nestedQueue, 1, currentThreadPermits)
        }
        return 1
    }

    fun acquireTransaction(txn: ReadWriteTransaction, env: Environment) {
        val creatingThread = txn.creatingThread
        val acquiredPermits: Int
        if (txn.isExclusive) {
            if (txn.isGCTransaction) {
                val gcTransactionAcquireTimeout = env.environmentConfig.gcTransactionAcquireTimeout
                acquiredPermits = tryAcquireExclusiveTransaction(creatingThread, gcTransactionAcquireTimeout)
                if (acquiredPermits == 0) {
                    throw TransactionAcquireTimeoutException(gcTransactionAcquireTimeout)
                }
            } else {
                acquiredPermits = acquireExclusiveTransaction(creatingThread)
            }
            if (acquiredPermits == 1) {
                txn.isExclusive = false
            }
        } else {
            acquiredPermits = acquireTransaction(creatingThread)
        }
        txn.acquiredPermits = acquiredPermits
    }

    /**
     * Release transaction that was acquired in a thread with specified permits.
     */
    fun releaseTransaction(thread: Thread, permits: Int) {
        criticalSection.enter().use {
            var currentThreadPermits = getThreadPermits(thread)
            if (permits > currentThreadPermits) {
                throw ExodusException("Can't release more permits than it was acquired")
            }
            acquiredPermits -= permits
            currentThreadPermits -= permits
            if (currentThreadPermits == 0) {
                threadPermits.remove(thread)
            } else {
                threadPermits[thread] = currentThreadPermits
            }
            notifyNextWaiters()
        }
    }

    fun releaseTransaction(txn: ReadWriteTransaction) {
        releaseTransaction(txn.creatingThread, txn.acquiredPermits)
    }

    /**
     * Downgrade transaction (making it holding only 1 permit) that was acquired in a thread with specified permits.
     */
    fun downgradeTransaction(thread: Thread, permits: Int) {
        if (permits > 1) {
            criticalSection.enter().use {
                var currentThreadPermits = getThreadPermits(thread)
                if (permits > currentThreadPermits) {
                    throw ExodusException("Can't release more permits than it was acquired")
                }
                acquiredPermits -= permits - 1
                currentThreadPermits -= permits - 1
                threadPermits[thread] = currentThreadPermits
                notifyNextWaiters()
            }
        }
    }

    fun downgradeTransaction(txn: ReadWriteTransaction) {
        downgradeTransaction(txn.creatingThread, txn.acquiredPermits)
        txn.acquiredPermits = 1
    }

    fun getThreadPermits(thread: Thread): Int {
        val result = threadPermits[thread]
        return result ?: 0
    }

    private fun waitForPermits(
        thread: Thread,
        queue: NavigableMap<Long, Condition>,
        permits: Int,
        currentThreadPermits: Int
    ) {
        val condition = criticalSection.newCondition()
        val currentOrder = acquireOrder++
        queue[currentOrder] = condition
        while (acquiredPermits > availablePermits - permits || queue.firstKey() != currentOrder) {
            condition.awaitUninterruptibly()
        }
        queue.pollFirstEntry()
        acquiredPermits += permits
        threadPermits[thread] = currentThreadPermits + permits
        if (acquiredPermits < availablePermits) {
            notifyNextWaiters()
        }
    }

    /**
     * Wait for exclusive permit during a timeout in milliseconds.
     *
     * @return number of acquired permits if > 0
     */
    private fun tryAcquireExclusiveTransaction(thread: Thread, timeout: Int): Int {
        var nanos = TimeUnit.MILLISECONDS.toNanos(timeout.toLong())
        criticalSection.enter().use {
            if (getThreadPermits(thread) > 0) {
                throw ExodusException("Exclusive transaction can't be nested")
            }
            val condition = criticalSection.newCondition()
            val currentOrder = acquireOrder++
            regularQueue[currentOrder] = condition
            while (acquiredPermits > 0 || regularQueue.firstKey() != currentOrder) {
                try {
                    nanos = condition.awaitNanos(nanos)
                    if (nanos < 0) {
                        break
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
            }
            if (acquiredPermits == 0 && regularQueue.firstKey() == currentOrder) {
                regularQueue.pollFirstEntry()
                acquiredPermits = availablePermits
                threadPermits[thread] = availablePermits
                return availablePermits
            }
            regularQueue.remove(currentOrder)
            notifyNextWaiters()
        }
        return 0
    }

    private fun notifyNextWaiters() {
        if (!notifyNextWaiter(nestedQueue)) {
            notifyNextWaiter(regularQueue)
        }
    }

    private fun getThreadPermitsToAcquire(thread: Thread): Int {
        val currentThreadPermits = getThreadPermits(thread)
        if (currentThreadPermits == availablePermits) {
            throw ExodusException("No more permits are available to acquire a transaction")
        }
        return currentThreadPermits
    }

    companion object {
        private fun notifyNextWaiter(queue: NavigableMap<Long, Condition>): Boolean {
            if (!queue.isEmpty()) {
                queue.firstEntry().value.signal()
                return true
            }
            return false
        }
    }
}
