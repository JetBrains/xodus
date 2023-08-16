package jetbrains.exodus.diskann.collections;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingIntArrayQueue {
    private final AtomicInteger head = new AtomicInteger();
    private final AtomicInteger tail = new AtomicInteger();

    private final AtomicIntegerArray array;
    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();
    private final AtomicLong notEmptyWaiters = new AtomicLong();

    private final Condition notFull = lock.newCondition();

    private boolean stop;

    public BlockingIntArrayQueue(int capacity) {
        capacity = Integer.highestOneBit(capacity - 1) << 1;
        array = new AtomicIntegerArray(capacity);
    }

    public void stop() {
        lock.lock();
        try {
            stop = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void add(int value) throws InterruptedException {
        while (true) {
            var head = this.head.get();
            var tail = this.tail.get();

            if (head - tail == array.length()) {
                lock.lock();
                try {
                    head = this.head.get();
                    tail = this.tail.get();

                    if (head - tail == array.length()) {
                        if (stop) {
                            throw new IllegalStateException("Queue is stopped");
                        }
                        notFull.await();
                    }
                } finally {
                    lock.unlock();
                }
            }

            if (this.head.compareAndSet(head, head + 1)) {
                array.set(head & (array.length() - 1), value);
                if (notEmptyWaiters.get() > 0) {
                    lock.lock();
                    try {
                        notEmpty.signal();
                    } finally {
                        lock.unlock();
                    }
                }

                return;
            }
        }
    }

    public int dequeue() throws InterruptedException {
        while (true) {
            var head = this.head.get();
            var tail = this.tail.get();

            if (head == tail) {
                lock.lock();
                try {
                    head = this.head.get();
                    tail = this.tail.get();

                    if (head == tail) {
                        if (stop) {
                            return -1;
                        }

                        notEmptyWaiters.incrementAndGet();
                        try {
                            notEmpty.await();
                        } finally {
                            notEmptyWaiters.decrementAndGet();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }

            var value = array.get(head & (array.length() - 1));
            if (this.head.compareAndSet(head, head + 1)) {
                lock.lock();
                try {
                    notFull.signal();
                } finally {
                    lock.unlock();
                }

                return value;
            }
        }
    }
}
