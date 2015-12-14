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
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class ReentrantTransactionDispatcher {

    private final int availablePermits;
    @NotNull
    private final Map<Thread, Integer> threadPermits;
    @NotNull
    private final NavigableMap<Long, Condition> regularQueue;
    @NotNull
    private final NavigableMap<Long, Condition> exclusiveQueue;
    @NotNull
    private final ReentrantLock lock;
    private long acquireOrder;
    private int acquiredPermits;

    ReentrantTransactionDispatcher(final int maxSimultaneousTransactions) {
        if (maxSimultaneousTransactions < 1) {
            throw new IllegalArgumentException("maxSimultaneousTransactions < 1");
        }
        availablePermits = maxSimultaneousTransactions;
        threadPermits = new HashMap<>();
        regularQueue = new TreeMap<>();
        exclusiveQueue = new TreeMap<>();
        lock = new ReentrantLock(false); // we explicitly don't need fairness here
        acquireOrder = 0;
        acquiredPermits = 0;
    }

    int getAvailablePermits() {
        try (CriticalSection ignored = new CriticalSection(lock)) {
            return availablePermits - acquiredPermits;
        }
    }

    /**
     * Acquire transaction with a single permit in a thread. Transactions are acquired reentrantly, i.e.
     * with respect to transactions already acquired in the thread.
     */
    void acquireTransaction(@NotNull final Thread thread) {
        try (CriticalSection ignored = new CriticalSection(lock)) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            final long currentOrder = acquireOrder++;
            final Condition condition = lock.newCondition();
            regularQueue.put(currentOrder, condition);
            while (acquiredPermits == availablePermits || regularQueue.firstKey() != currentOrder) {
                condition.awaitUninterruptibly();
            }
            regularQueue.pollFirstEntry();
            ++acquiredPermits;
            threadPermits.put(thread, currentThreadPermits + 1);
            notifyNextWaiter(regularQueue);
        }
    }

    /**
     * Acquire exclusive transaction in a thread. Transactions are acquired reentrantly, i.e. with respect
     * to transactions already acquired in the thread.
     *
     * @return the number of acquired permits.
     */
    int acquireExclusiveTransaction(@NotNull final Thread thread) {
        try (CriticalSection ignored = new CriticalSection(lock)) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            final int permitsToAcquire = availablePermits - currentThreadPermits;
            final long currentOrder = acquireOrder++;
            final Condition condition = lock.newCondition();
            NavigableMap<Long, Condition> threadQueue = regularQueue;
            threadQueue.put(currentOrder, condition);
            while (true) {
                if (threadQueue.firstKey() == currentOrder) {
                    if (acquiredPermits <= availablePermits - permitsToAcquire) {
                        break;
                    }
                    // if an exclusive transaction cannot be acquired fairly (in its turn)
                    // try to shuffle it to the queue of exclusive transactions
                    if (threadQueue == regularQueue) {
                        threadQueue.pollFirstEntry();
                        threadQueue = exclusiveQueue;
                        threadQueue.put(currentOrder, condition);
                        notifyNextWaiter(regularQueue);
                    }
                }
                condition.awaitUninterruptibly();
            }
            threadQueue.pollFirstEntry();
            acquiredPermits += permitsToAcquire;
            threadPermits.put(thread, currentThreadPermits + permitsToAcquire);
            return permitsToAcquire;
        }
    }

    /**
     * Acquire exclusive transaction in a thread with specified timeout in milliseconds.
     *
     * @return the number of acquired permits or 0 if acquisition failed.
     */
    int tryAcquireExclusiveTransaction(@NotNull final Thread thread, long timeout) {
        timeout = TimeUnit.MILLISECONDS.toNanos(timeout);
        try (CriticalSection ignored = new CriticalSection(lock)) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            int permitsToAcquire = availablePermits - currentThreadPermits;
            final long currentOrder = acquireOrder++;
            final Condition condition = lock.newCondition();
            NavigableMap<Long, Condition> threadQueue = regularQueue;
            threadQueue.put(currentOrder, condition);
            while (true) {
                if (threadQueue.firstKey() == currentOrder) {
                    if (acquiredPermits <= availablePermits - permitsToAcquire) {
                        break;
                    }
                    // if an exclusive transaction cannot be acquired fairly (in its turn)
                    // try to shuffle it to the queue of exclusive transactions
                    if (permitsToAcquire > 1 && threadQueue == regularQueue) {
                        threadQueue.pollFirstEntry();
                        threadQueue = exclusiveQueue;
                        threadQueue.put(currentOrder, condition);
                        notifyNextWaiter(regularQueue);
                    }
                }
                try {
                    timeout = condition.awaitNanos(timeout);
                } catch (InterruptedException e) {
                    timeout = 0;
                }
                if (timeout < 0) {
                    timeout = 0;
                }
                if (timeout == 0) {
                    if (permitsToAcquire == 1) {
                        threadQueue.remove(currentOrder);
                        notifyNextWaiters();
                        return 0;
                    }
                    // if failed to acquire transaction within timeout then downgrade it
                    permitsToAcquire = 1;
                }
            }
            threadQueue.pollFirstEntry();
            acquiredPermits += permitsToAcquire;
            threadPermits.put(thread, currentThreadPermits + permitsToAcquire);
            notifyNextWaiter(regularQueue);
            return permitsToAcquire;
        }
    }

    void acquireTransaction(@NotNull final TransactionBase txn, @NotNull final Environment env) {
        final Thread creatingThread = txn.getCreatingThread();
        if (txn.isExclusive()) {
            final boolean isGCTransaction = txn.isGCTransaction();
            if (txn.wasCreatedExclusive() && !isGCTransaction) {
                txn.setAcquiredPermits(acquireExclusiveTransaction(creatingThread));
                return;
            }
            final EnvironmentConfig ec = env.getEnvironmentConfig();
            final int acquiredPermits = tryAcquireExclusiveTransaction(creatingThread,
                    isGCTransaction ? ec.getGcTransactionAcquireTimeout() : ec.getEnvTxnReplayTimeout());
            if (acquiredPermits <= 1) {
                txn.setExclusive(false);
            }
            if (acquiredPermits > 0) {
                txn.setAcquiredPermits(acquiredPermits);
                return;
            }
        }
        acquireTransaction(creatingThread);
        txn.setAcquiredPermits(1);
    }

    /**
     * Release transaction that was acquired in a thread with specified permits.
     */
    void releaseTransaction(@NotNull final Thread thread, final int permits) {
        try (CriticalSection ignored = new CriticalSection(lock)) {
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
            try (CriticalSection ignored = new CriticalSection(lock)) {
                int currentThreadPermits = getThreadPermits(thread);
                if (permits > currentThreadPermits) {
                    throw new ExodusException("Can't release more permits than it was acquired");
                }
                acquiredPermits -= (permits - 1);
                currentThreadPermits -= (permits - 1);
                threadPermits.put(thread, currentThreadPermits);
                notifyNextWaiter(regularQueue);
            }
        }
    }

    void downgradeTransaction(@NotNull final TransactionBase txn) {
        downgradeTransaction(txn.getCreatingThread(), txn.getAcquiredPermits());
        txn.setAcquiredPermits(1);
    }

    // for tests only
    int acquirerCount() {
        try (CriticalSection ignored = new CriticalSection(lock)) {
            return regularQueue.size();
        }
    }

    // for tests only
    int exclusiveAcquirerCount() {
        try (CriticalSection ignored = new CriticalSection(lock)) {
            return exclusiveQueue.size();
        }
    }

    private void notifyNextWaiters() {
        if (acquiredPermits == 0) {
            if (!exclusiveQueue.isEmpty()) {
                exclusiveQueue.firstEntry().getValue().signal();
            }
        }
        notifyNextWaiter(regularQueue);
    }

    private int getThreadPermits(@NotNull final Thread thread) {
        final Integer result = threadPermits.get(thread);
        return result == null ? 0 : result;
    }

    private int getThreadPermitsToAcquire(@NotNull Thread thread) {
        final int currentThreadPermits = getThreadPermits(thread);
        if (currentThreadPermits == availablePermits) {
            throw new ExodusException("No more permits are available to acquire a transaction");
        }
        return currentThreadPermits;
    }

    private static void notifyNextWaiter(@NotNull final NavigableMap<Long, Condition> queue) {
        if (!queue.isEmpty()) {
            queue.firstEntry().getValue().signal();
        }
    }

    private static final class CriticalSection implements AutoCloseable {

        @NotNull
        private final ReentrantLock lock;

        private CriticalSection(@NotNull final ReentrantLock lock) {
            this.lock = lock;
            lock.lock();
        }

        @Override
        public void close() {
            lock.unlock();
        }
    }
}
