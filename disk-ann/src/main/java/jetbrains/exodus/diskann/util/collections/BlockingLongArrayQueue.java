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
package jetbrains.exodus.diskann.util.collections;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingLongArrayQueue {
    private long[] queue;

    private final Lock lock = new ReentrantLock();
    private long head;
    private long tail;


    public BlockingLongArrayQueue(int initialCapacity) {
        queue = new long[Integer.highestOneBit(initialCapacity - 1) << 1];
    }

    public void enqueue(long value) {
        lock.lock();
        try {
            if(tail - head == queue.length) {
                var newQueue = new long[queue.length << 1];
                System.arraycopy(queue, 0, newQueue, 0, queue.length);
                queue = newQueue;
            }

            queue[(int) (tail & (queue.length - 1))] = value;
            tail++;
        } finally {
            lock.unlock();
        }
    }

    public long dequeue() throws InterruptedException {
        lock.lock();
        try {
            if (tail == head) {
                return -1;
            }

            long value = queue[(int) (head & (queue.length - 1))];
            head++;

            return value;
        } finally {
            lock.unlock();
        }
    }
}
