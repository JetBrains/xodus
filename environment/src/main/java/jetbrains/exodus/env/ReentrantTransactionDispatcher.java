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
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.execution.locks.CriticalSection;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

final class ReentrantTransactionDispatcher {

    private final int availablePermits;
    @NotNull
    private final Map<Thread, Integer> threadPermits;
    @NotNull
    private final NavigableMap<Long, Condition> regularQueue;
    @NotNull
    private final NavigableMap<Long, Condition> nestedQueue;
    @NotNull
    private final CriticalSection criticalSection;
    private long acquireOrder;
    private int acquiredPermits;

    ReentrantTransactionDispatcher(final int maxSimultaneousTransactions) {
        if (maxSimultaneousTransactions < 1) {
            throw new IllegalArgumentException("maxSimultaneousTransactions < 1");
        }
        availablePermits = maxSimultaneousTransactions;
        threadPermits = new HashMap<>();
        regularQueue = new TreeMap<>();
        nestedQueue = new TreeMap<>();
        criticalSection = new CriticalSection(false /* we explicitly don't need fairness here */);
        acquireOrder = 0;
        acquiredPermits = 0;
    }

    int getAvailablePermits() {
        try (CriticalSection ignored = criticalSection.enter()) {
            return availablePermits - acquiredPermits;
        }
    }

    /**
     * Acquire transaction with a single permit in a thread. Transactions are acquired reentrantly, i.e.
     * with respect to transactions already acquired in the thread.
     *
     * @return the number of acquired permits, identically equal to 1.
     */
    int acquireTransaction(@NotNull final Thread thread) {
        try (CriticalSection ignored = criticalSection.enter()) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            waitForPermits(thread, currentThreadPermits > 0 ? nestedQueue : regularQueue, 1, currentThreadPermits);
        }
        return 1;
    }

    /**
     * Acquire exclusive transaction in a thread. Transactions are acquired reentrantly, i.e. with respect
     * to transactions already acquired in the thread.
     * NB! Nested transaction is never acquired as exclusive.
     *
     * @return the number of acquired permits.
     */
    int acquireExclusiveTransaction(@NotNull final Thread thread) {
        try (CriticalSection ignored = criticalSection.enter()) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            // if there are no permits acquired in the thread then we can acquire exclusive txn, i.e. all available permits
            if (currentThreadPermits == 0) {
                waitForPermits(thread, regularQueue, availablePermits, 0);
                return availablePermits;
            }
            waitForPermits(thread, nestedQueue, 1, currentThreadPermits);
        }
        return 1;
    }

    void acquireTransaction(@NotNull final TransactionBase txn, @NotNull final Environment env) {
        final Thread creatingThread = txn.getCreatingThread();
        int acquiredPermits;
        if (txn.isExclusive()) {
            if (txn.isGCTransaction()) {
                final int gcTransactionAcquireTimeout = env.getEnvironmentConfig().getGcTransactionAcquireTimeout();
                acquiredPermits = tryAcquireExclusiveTransaction(creatingThread, gcTransactionAcquireTimeout);
                if (acquiredPermits == 0) {
                    throw new TransactionAcquireTimeoutException(gcTransactionAcquireTimeout);
                }
            } else {
                acquiredPermits = acquireExclusiveTransaction(creatingThread);
            }
            if (acquiredPermits == 1) {
                txn.setExclusive(false);
            }
        } else {
            acquiredPermits = acquireTransaction(creatingThread);
        }
        txn.setAcquiredPermits(acquiredPermits);
    }

    /**
     * Release transaction that was acquired in a thread with specified permits.
     */
    void releaseTransaction(@NotNull final Thread thread, final int permits) {
        try (CriticalSection ignored = criticalSection.enter()) {
            int currentThreadPermits = getThreadPermits(thread);
            if (permits > currentThreadPermits) {
                throw new ExodusException("Can't release more permits than it was acquired");
            }
            acquiredPermits -= permits;
            currentThreadPermits -= permits;
            if (currentThreadPermits == 0) {
                threadPermits.remove(thread);
            } else {
                threadPermits.put(thread, currentThreadPermits);
            }
            notifyNextWaiters();
        }
    }

    void releaseTransaction(@NotNull final TransactionBase txn) {
        releaseTransaction(txn.getCreatingThread(), txn.getAcquiredPermits());
    }

    /**
     * Downgrade transaction (making it holding only 1 permit) that was acquired in a thread with specified permits.
     */
    void downgradeTransaction(@NotNull final Thread thread, final int permits) {
        if (permits > 1) {
            try (CriticalSection ignored = criticalSection.enter()) {
                int currentThreadPermits = getThreadPermits(thread);
                if (permits > currentThreadPermits) {
                    throw new ExodusException("Can't release more permits than it was acquired");
                }
                acquiredPermits -= (permits - 1);
                currentThreadPermits -= (permits - 1);
                threadPermits.put(thread, currentThreadPermits);
                notifyNextWaiters();
            }
        }
    }

    void downgradeTransaction(@NotNull final TransactionBase txn) {
        downgradeTransaction(txn.getCreatingThread(), txn.getAcquiredPermits());
        txn.setAcquiredPermits(1);
    }

    int getThreadPermits(@NotNull final Thread thread) {
        final Integer result = threadPermits.get(thread);
        return result == null ? 0 : result;
    }

    private void waitForPermits(@NotNull final Thread thread,
                                @NotNull final NavigableMap<Long, Condition> queue,
                                final int permits,
                                final int currentThreadPermits) {
        final Condition condition = criticalSection.newCondition();
        final long currentOrder = acquireOrder++;
        queue.put(currentOrder, condition);
        while (acquiredPermits > availablePermits - permits || queue.firstKey() != currentOrder) {
            condition.awaitUninterruptibly();
        }
        queue.pollFirstEntry();
        acquiredPermits += permits;
        threadPermits.put(thread, currentThreadPermits + permits);
        if (acquiredPermits < availablePermits) {
            notifyNextWaiters();
        }
    }

    /**
     * Wait for exclusive permit during a timeout in milliseconds.
     *
     * @return number of acquired permits if > 0
     */
    private int tryAcquireExclusiveTransaction(@NotNull final Thread thread, final int timeout) {
        long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);
        try (CriticalSection ignored = criticalSection.enter()) {
            if (getThreadPermits(thread) > 0) {
                throw new ExodusException("Exclusive transaction can't be nested");
            }
            final Condition condition = criticalSection.newCondition();
            final long currentOrder = acquireOrder++;
            regularQueue.put(currentOrder, condition);
            while (acquiredPermits > 0 || regularQueue.firstKey() != currentOrder) {
                try {
                    nanos = condition.awaitNanos(nanos);
                    if (nanos < 0) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (acquiredPermits == 0 && regularQueue.firstKey() == currentOrder) {
                regularQueue.pollFirstEntry();
                acquiredPermits = availablePermits;
                threadPermits.put(thread, availablePermits);
                return availablePermits;
            }
            regularQueue.remove(currentOrder);
            notifyNextWaiters();
        }
        return 0;
    }

    private void notifyNextWaiters() {
        if (!notifyNextWaiter(nestedQueue)) {
            notifyNextWaiter(regularQueue);
        }
    }

    private int getThreadPermitsToAcquire(@NotNull Thread thread) {
        final int currentThreadPermits = getThreadPermits(thread);
        if (currentThreadPermits == availablePermits) {
            throw new ExodusException("No more permits are available to acquire a transaction");
        }
        return currentThreadPermits;
    }

    private static boolean notifyNextWaiter(@NotNull final NavigableMap<Long, Condition> queue) {
        if (!queue.isEmpty()) {
            queue.firstEntry().getValue().signal();
            return true;
        }
        return false;
    }
}
