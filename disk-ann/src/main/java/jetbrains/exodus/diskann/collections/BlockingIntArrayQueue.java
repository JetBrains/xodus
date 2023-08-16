package jetbrains.exodus.diskann.collections;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingIntArrayQueue {
    private AtomicInteger position = new AtomicInteger();

    private volatile AtomicIntegerArray array;
    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();
    private final AtomicLong notEmptyWaiters = new AtomicLong();

    private final Condition notFull = lock.newCondition();

    private final int maxCapacity;

    public BlockingIntArrayQueue(int capacity, int maxCapacity) {
        capacity = Integer.highestOneBit(capacity - 1) << 1;
        array = new AtomicIntegerArray(capacity);

        maxCapacity = Integer.highestOneBit(maxCapacity - 1) << 1;
        this.maxCapacity = Math.max(maxCapacity, capacity);
    }

    public void add(int value) throws InterruptedException {
        while (true) {
            var position = this.position.get();
            if (position == array.length()) {
                break;
            }

            if (this.position.compareAndSet(position, position + 1)) {
                if (array.compareAndSet(position, Integer.MIN_VALUE, value)) {
                    if (notEmptyWaiters.get() == 0) {
                        return;
                    } else {
                        break;
                    }
                }
            }
        }


        lock.lock();
        try {
            var position = this.position.get();

            while (position == array.length()) {
                if (position < maxCapacity) {
                    var newArray = new AtomicIntegerArray(position << 1);

                    for (var i = 0; i < position; i++) {
                        newArray.set(i, array.get(i));
                    }
                    array = newArray;
                } else {
                    notFull.await();
                }

            }

            while (!this.position.compareAndSet(position, position + 1)) {
                if (array.compareAndSet(position, Integer.MIN_VALUE, value)) {
                    break;
                }

                position = this.position.get();
            }

            if (notEmptyWaiters.get() > 0) {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public int dequeue() throws InterruptedException {
        lock.lock();
        try {
            while (position.get() == 0) {
                notEmptyWaiters.incrementAndGet();
                notEmpty.await();
            }

            notEmptyWaiters.decrementAndGet();

            var position = this.position.decrementAndGet();
            var value = array.get(position);
            array.set(position, Integer.MIN_VALUE);

            return value;
        } finally {
            lock.unlock();
        }
    }
}
