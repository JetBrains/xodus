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

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

final class ReentrantTransactionDispatcher {

    private final int availablePermits;
    @NotNull
    private final Map<Thread, Integer> threadPermits;
    @NotNull
    private final Queue<Thread> regularThreadQueue;
    @NotNull
    private final Queue<Thread> exclusiveThreadQueue;
    @NotNull
    private final Object syncObject;
    private int acquiredPermits;

    ReentrantTransactionDispatcher(final int maxSimultaneousTransactions) {
        if (maxSimultaneousTransactions < 1) {
            throw new IllegalArgumentException("maxSimultaneousTransactions < 1");
        }
        availablePermits = maxSimultaneousTransactions;
        threadPermits = new HashMap<>();
        regularThreadQueue = new LinkedList<>();
        exclusiveThreadQueue = new LinkedList<>();
        syncObject = new Object();
        acquiredPermits = 0;
    }

    int getAvailablePermits() {
        synchronized (syncObject) {
            return availablePermits - acquiredPermits;
        }
    }

    /**
     * Acquire transaction with a single permit in a thread. Transactions are acquired reentrantly, i.e.
     * with respect to transactions already acquired in the thread.
     */
    void acquireTransaction(@NotNull final Thread thread) {
        synchronized (syncObject) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            if (acquiredPermits == availablePermits || !regularThreadQueue.isEmpty()) {
                regularThreadQueue.offer(thread);
                while (true) {
                    waitForSyncObject();
                    if (acquiredPermits < availablePermits && regularThreadQueue.peek().equals(thread)) {
                        break;
                    }
                }
                regularThreadQueue.poll();
            }
            ++acquiredPermits;
            threadPermits.put(thread, currentThreadPermits + 1);
        }
    }

    /**
     * Acquire exclusive transaction in a thread. Transactions are acquired reentrantly, i.e. with respect
     * to transactions already acquired in the thread.
     *
     * @return the number of acquired permits.
     */
    int acquireExclusiveTransaction(@NotNull final Thread thread) {
        synchronized (syncObject) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            final int permitsToAcquire = availablePermits - currentThreadPermits;
            if (acquiredPermits > availablePermits - permitsToAcquire || !regularThreadQueue.isEmpty()) {
                Queue<Thread> threadQueue = regularThreadQueue;
                threadQueue.offer(thread);
                while (true) {
                    waitForSyncObject();
                    if (!threadQueue.peek().equals(thread)) {
                        continue;
                    }
                    if (acquiredPermits <= availablePermits - permitsToAcquire) {
                        break;
                    }
                    // if an exclusive transaction cannot be acquired fairly (in its turn)
                    // try to shuffle it to the queue of exclusive transactions
                    if (threadQueue == regularThreadQueue) {
                        threadQueue.poll();
                        threadQueue = exclusiveThreadQueue;
                        threadQueue.offer(thread);
                        syncObject.notifyAll();
                    }
                }
                threadQueue.poll();
            }
            acquiredPermits += permitsToAcquire;
            threadPermits.put(thread, currentThreadPermits + permitsToAcquire);
            return permitsToAcquire;
        }
    }

    /**
     * Acquire exclusive transaction in a thread if no other transaction tries to acquire exclusive permits.
     *
     * @return the number of acquired permits or 0 if acquisition failed.
     */
    int tryAcquireExclusiveTransaction(@NotNull final Thread thread) {
        synchronized (syncObject) {
            final int currentThreadPermits = getThreadPermitsToAcquire(thread);
            int permitsToAcquire = availablePermits - currentThreadPermits;
            if (acquiredPermits > availablePermits - permitsToAcquire || !regularThreadQueue.isEmpty()) {
                Queue<Thread> threadQueue = regularThreadQueue;
                threadQueue.offer(thread);
                while (true) {
                    waitForSyncObject();
                    if (!threadQueue.peek().equals(thread)) {
                        continue;
                    }
                    if (acquiredPermits <= availablePermits - permitsToAcquire) {
                        break;
                    }
                    // if an exclusive transaction cannot be acquired fairly (in its turn)
                    // try to shuffle it to the queue of exclusive transactions
                    if (threadQueue == regularThreadQueue) {
                        threadQueue.poll();
                        if (!exclusiveThreadQueue.isEmpty()) {
                            syncObject.notifyAll();
                            return 0;
                        }
                        threadQueue = exclusiveThreadQueue;
                        threadQueue.offer(thread);
                        syncObject.notifyAll();
                    }
                }
                threadQueue.poll();
            }
            acquiredPermits += permitsToAcquire;
            threadPermits.put(thread, currentThreadPermits + permitsToAcquire);
            return permitsToAcquire;
        }
    }

    void acquireTransaction(@NotNull final TransactionBase txn) {
        final Thread creatingThread = txn.getCreatingThread();
        if (txn.isExclusive()) {
            if (txn.wasCreatedExclusive()) {
                txn.setAcquiredPermits(acquireExclusiveTransaction(creatingThread));
                return;
            }
            final int acquiredPermits = tryAcquireExclusiveTransaction(creatingThread);
            if (acquiredPermits > 0) {
                txn.setAcquiredPermits(acquiredPermits);
                return;
            }
            txn.setExclusive(false);
        }
        acquireTransaction(creatingThread);
        txn.setAcquiredPermits(1);
    }

    /**
     * Release transaction that was acquired in a thread with specified permits.
     */
    void releaseTransaction(@NotNull final Thread thread, final int permits) {
        synchronized (syncObject) {
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
            syncObject.notifyAll();
        }
    }

    void releaseTransaction(@NotNull final TransactionBase txn) {
        releaseTransaction(txn.getCreatingThread(), txn.getAcquiredPermits());
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

    private void waitForSyncObject() {
        try {
            syncObject.wait();
        } catch (InterruptedException e) {
            throw ExodusException.toExodusException(e);
        }
    }
}
