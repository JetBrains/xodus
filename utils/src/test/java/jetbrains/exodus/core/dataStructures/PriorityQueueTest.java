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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.hash.IntHashSet;
import jetbrains.exodus.core.execution.locks.Guard;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class PriorityQueueTest {

    protected abstract PriorityQueue createQueue();

    @Test
    public void empty() {
        assertEquals(0, createQueue().size());
        Assert.assertTrue(createQueue().isEmpty());
    }

    @Test
    public void emptyPop() {
        Assert.assertNull(createQueue().pop());
    }

    @Test
    public void clear() {
        final PriorityQueue<Integer, String> queue = createQueue();
        queue.push(0, "1");
        Assert.assertTrue(queue.size() > 0);
        Assert.assertFalse(queue.isEmpty());
        queue.clear();
        assertEquals(0, queue.size());
        Assert.assertTrue(queue.isEmpty());
        Assert.assertNull(queue.peek());
        Assert.assertNull(queue.pop());
    }

    @Test
    public void pushPop() {
        final PriorityQueue<Priority, String> queue = createQueue();
        queue.push(Priority.normal, "1");
        queue.push(Priority.above_normal, "2");
        queue.push(Priority.below_normal, "3");
        queue.push(Priority.above_normal, "4");
        queue.push(Priority.below_normal, "5");
        queue.push(Priority.lowest, "6");
        queue.push(Priority.highest, "7");
        queue.push(Priority.lowest, "8");
        assertEquals("7", queue.pop());
        assertEquals("2", queue.pop());
        assertEquals("4", queue.pop());
        assertEquals("1", queue.pop());
        assertEquals("3", queue.pop());
        assertEquals("5", queue.pop());
        assertEquals("6", queue.pop());
        assertEquals("8", queue.pop());
    }

    @Test
    public void pushPeek() {
        final PriorityQueue<Priority, String> queue = createQueue();
        queue.push(Priority.normal, "1");
        queue.push(Priority.above_normal, "2");
        queue.push(Priority.below_normal, "3");
        queue.push(Priority.above_normal, "4");
        queue.push(Priority.below_normal, "5");
        queue.push(Priority.lowest, "6");
        queue.push(Priority.highest, "7");
        queue.push(Priority.lowest, "8");
        assertEquals("7", queue.peek());
        assertEquals("7", queue.pop());
        assertEquals("2", queue.peek());
        assertEquals("2", queue.pop());
        assertEquals("4", queue.peek());
        assertEquals("4", queue.pop());
        assertEquals("1", queue.peek());
        assertEquals("1", queue.pop());
        assertEquals("3", queue.peek());
        assertEquals("3", queue.pop());
        assertEquals("5", queue.peek());
        assertEquals("5", queue.pop());
        assertEquals("6", queue.peek());
        assertEquals("6", queue.pop());
        assertEquals("8", queue.peek());
        assertEquals("8", queue.pop());
    }

    @Test
    public void meanPriority() {
        assertEquals(Priority.normal, Priority.mean(Priority.above_normal, Priority.below_normal));
        assertEquals(Priority.normal, Priority.mean(Priority.lowest, Priority.highest));
        assertEquals(Priority.normal, Priority.mean(Priority.normal, Priority.normal));
    }

    @Test
    public void meanPriorityPushes() {
        final PriorityQueue<Priority, String> queue = createQueue();
        queue.push(Priority.normal, "1");
        queue.push(Priority.mean(Priority.normal, Priority.above_normal), "2");
        queue.push(Priority.mean(Priority.normal, Priority.below_normal), "4");
        queue.push(Priority.mean(Priority.normal, Priority.below_normal), "3");
        queue.push(Priority.mean(Priority.normal, Priority.lowest), "5");
        queue.push(Priority.mean(Priority.above_normal, Priority.lowest), "6");
        queue.push(Priority.mean(Priority.above_normal, Priority.highest), "7");
        assertEquals("7", queue.pop());
        assertEquals("2", queue.pop());
        assertEquals("1", queue.pop());
        assertEquals("4", queue.pop());
        assertEquals("3", queue.pop());
        assertEquals("6", queue.pop());
        assertEquals("5", queue.pop());
    }

    @Test
    public void merge() {
        final PriorityQueue<Integer, TestObject> queue = createQueue();
        queue.push(0, new TestObject(0));
        queue.push(1, new TestObject(1));
        queue.push(2, new TestObject(2));
        queue.push(0, new TestObject(1));
        queue.push(2, new TestObject(0));
        queue.push(1, new TestObject(1));
        queue.push(0, new TestObject(0));
        assertEquals(3, queue.size());
        assertEquals(new TestObject(2), queue.pop());
        assertEquals(new TestObject(1), queue.pop());
        assertEquals(new TestObject(0), queue.pop());
    }

    @TestFor(issues = "XD-600")
    @Test
    public void mergePushedOut() {
        final PriorityQueue<Integer, TestObject> queue = createQueue();
        final TestObject firstValue = new TestObject(0);
        TestObject pushedOut = queue.push(0, firstValue);
        assertNull(pushedOut);
        final TestObject secondValue = new TestObject(0);
        pushedOut = queue.push(0, secondValue);
        assertSame(firstValue, pushedOut);
        pushedOut.number = 1;
        assertEquals(1, queue.size());
        assertNotNull(queue.pop());
        assertNull(queue.pop());
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Test
    public void concurrentBenchmark() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger();
        final PriorityQueue<Priority, TestObject> queue = createQueue();
        final Runnable threadFunction = new Runnable() {
            @Override
            public void run() {
                try {
                    //noinspection InfiniteLoopStatement
                    while (true) {
                        try (Guard ignored = queue.lock()) {
                            final TestObject value = new TestObject(counter);
                            final Priority p;
                            switch (value.number % 5) {
                                case 0:
                                    p = Priority.lowest;
                                    break;
                                case 1:
                                    p = Priority.below_normal;
                                    break;
                                case 2:
                                    p = Priority.normal;
                                    break;
                                case 3:
                                    p = Priority.above_normal;
                                    break;
                                default:
                                    p = Priority.highest;
                                    break;
                            }
                            queue.push(p, value);
                        }
                    }
                } catch (RuntimeException e) {
                    // ignore
                }
            }
        };

        TestUtil.time("concurrentBenchmark", new Runnable() {
            @Override
            public void run() {
                final int numberOfThreads = 4;
                final Thread[] threads = new Thread[numberOfThreads];
                for (int i = 0; i < numberOfThreads; ++i) {
                    threads[i] = new Thread(threadFunction);
                }
                for (int i = 0; i < numberOfThreads; ++i) {
                    threads[i].start();
                }
                for (int i = 0; i < numberOfThreads; ++i) {
                    try {
                        threads[i].join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // ignore
                    }
                }
            }
        });

        final IntHashSet numbers = new IntHashSet();
        TestObject to;
        while ((to = queue.pop()) != null) {
            numbers.add(to.number);
        }
        assertEquals(TestObject.MAX_TEST_OBJECTS, numbers.size());
    }

    private static class TestObject {

        private static final int MAX_TEST_OBJECTS = 3000000;

        private int number;

        private TestObject(final int number) {
            this.number = number;
            if (number >= MAX_TEST_OBJECTS) {
                throw new RuntimeException();
            }
        }

        private TestObject(final AtomicInteger counter) {
            this(counter.getAndIncrement());
        }

        @Override
        public boolean equals(Object o) {
            return number == ((TestObject) o).number;
        }

        @Override
        public int hashCode() {
            return number;
        }
    }
}
                                        