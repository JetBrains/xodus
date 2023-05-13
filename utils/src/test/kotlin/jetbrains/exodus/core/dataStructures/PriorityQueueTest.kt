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
package jetbrains.exodus.core.dataStructures

import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil.time
import jetbrains.exodus.core.dataStructures.hash.IntHashSet
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

abstract class PriorityQueueTest {
    protected abstract fun <T : Comparable<T>, V> createQueue(): PriorityQueue<T, V>

    @Test
    fun empty() {
        Assert.assertEquals(0, createQueue<String, Any>().size().toLong())
        Assert.assertTrue(createQueue<String, Any>().isEmpty)
    }

    @Test
    fun emptyPop() {
        Assert.assertNull(createQueue<String, Any>().pop())
    }

    @Test
    fun clear() {
        val queue: PriorityQueue<Int, String> = createQueue()
        queue.push(0, "1")
        Assert.assertTrue(queue.size() > 0)
        Assert.assertFalse(queue.isEmpty)
        queue.clear()
        Assert.assertEquals(0, queue.size().toLong())
        Assert.assertTrue(queue.isEmpty)
        Assert.assertNull(queue.peek())
        Assert.assertNull(queue.pop())
    }

    @Test
    fun pushPop() {
        val queue = populateQueue()
        Assert.assertEquals("7", queue.pop())
        Assert.assertEquals("2", queue.pop())
        Assert.assertEquals("4", queue.pop())
        Assert.assertEquals("1", queue.pop())
        Assert.assertEquals("3", queue.pop())
        Assert.assertEquals("5", queue.pop())
        Assert.assertEquals("6", queue.pop())
        Assert.assertEquals("8", queue.pop())
    }

    @Test
    fun pushIterate() {
        val queue = populateQueue()
        val it: Iterator<String> = queue.iterator()
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("7", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("2", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("4", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("1", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("3", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("5", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("6", it.next())
        Assert.assertTrue(it.hasNext())
        Assert.assertEquals("8", it.next())
        Assert.assertFalse(it.hasNext())
    }

    @Test
    fun pushPeek() {
        val queue = populateQueue()
        Assert.assertEquals("7", queue.peek())
        Assert.assertEquals("7", queue.pop())
        Assert.assertEquals("2", queue.peek())
        Assert.assertEquals("2", queue.pop())
        Assert.assertEquals("4", queue.peek())
        Assert.assertEquals("4", queue.pop())
        Assert.assertEquals("1", queue.peek())
        Assert.assertEquals("1", queue.pop())
        Assert.assertEquals("3", queue.peek())
        Assert.assertEquals("3", queue.pop())
        Assert.assertEquals("5", queue.peek())
        Assert.assertEquals("5", queue.pop())
        Assert.assertEquals("6", queue.peek())
        Assert.assertEquals("6", queue.pop())
        Assert.assertEquals("8", queue.peek())
        Assert.assertEquals("8", queue.pop())
    }

    @Test
    fun pushCopyPop() {
        val queue = populateAndCopyQueue()
        Assert.assertEquals("7", queue.pop())
        Assert.assertEquals("2", queue.pop())
        Assert.assertEquals("4", queue.pop())
        Assert.assertEquals("1", queue.pop())
        Assert.assertEquals("3", queue.pop())
        Assert.assertEquals("5", queue.pop())
        Assert.assertEquals("6", queue.pop())
        Assert.assertEquals("8", queue.pop())
    }

    @Test
    fun pushCopyPeek() {
        val queue = populateAndCopyQueue()
        Assert.assertEquals("7", queue.peek())
        Assert.assertEquals("7", queue.pop())
        Assert.assertEquals("2", queue.peek())
        Assert.assertEquals("2", queue.pop())
        Assert.assertEquals("4", queue.peek())
        Assert.assertEquals("4", queue.pop())
        Assert.assertEquals("1", queue.peek())
        Assert.assertEquals("1", queue.pop())
        Assert.assertEquals("3", queue.peek())
        Assert.assertEquals("3", queue.pop())
        Assert.assertEquals("5", queue.peek())
        Assert.assertEquals("5", queue.pop())
        Assert.assertEquals("6", queue.peek())
        Assert.assertEquals("6", queue.pop())
        Assert.assertEquals("8", queue.peek())
        Assert.assertEquals("8", queue.pop())
    }

    @Test
    fun meanPriority() {
        Assert.assertEquals(Priority.normal, Priority.mean(Priority.above_normal, Priority.below_normal))
        Assert.assertEquals(Priority.normal, Priority.mean(Priority.lowest, Priority.highest))
        Assert.assertEquals(Priority.normal, Priority.mean(Priority.normal, Priority.normal))
    }

    @Test
    fun meanPriorityPushes() {
        val queue: PriorityQueue<Priority, String> = createQueue()
        queue.push(Priority.normal, "1")
        queue.push(Priority.mean(Priority.normal, Priority.above_normal), "2")
        queue.push(Priority.mean(Priority.normal, Priority.below_normal), "4")
        queue.push(Priority.mean(Priority.normal, Priority.below_normal), "3")
        queue.push(Priority.mean(Priority.normal, Priority.lowest), "5")
        queue.push(Priority.mean(Priority.above_normal, Priority.lowest), "6")
        queue.push(Priority.mean(Priority.above_normal, Priority.highest), "7")
        Assert.assertEquals("7", queue.pop())
        Assert.assertEquals("2", queue.pop())
        Assert.assertEquals("1", queue.pop())
        Assert.assertEquals("4", queue.pop())
        Assert.assertEquals("3", queue.pop())
        Assert.assertEquals("6", queue.pop())
        Assert.assertEquals("5", queue.pop())
    }

    @Test
    fun merge() {
        val queue: PriorityQueue<Int, TestObject> = createQueue()
        queue.push(0, TestObject(0))
        queue.push(1, TestObject(1))
        queue.push(2, TestObject(2))
        queue.push(0, TestObject(1))
        queue.push(2, TestObject(0))
        queue.push(1, TestObject(1))
        queue.push(0, TestObject(0))
        Assert.assertEquals(3, queue.size().toLong())
        Assert.assertEquals(TestObject(2), queue.pop())
        Assert.assertEquals(TestObject(1), queue.pop())
        Assert.assertEquals(TestObject(0), queue.pop())
    }

    @TestFor(issue = "XD-600")
    @Test
    fun mergePushedOut() {
        val queue: PriorityQueue<Int, TestObject> = createQueue()
        val firstValue = TestObject(0)
        var pushedOut = queue.push(0, firstValue)
        Assert.assertNull(pushedOut)
        val secondValue = TestObject(0)
        pushedOut = queue.push(0, secondValue)
        Assert.assertSame(firstValue, pushedOut)
        pushedOut.number = 1
        Assert.assertEquals(1, queue.size().toLong())
        Assert.assertNotNull(queue.pop())
        Assert.assertNull(queue.pop())
    }

    @Test
    fun concurrentBenchmark() {
        val counter = AtomicInteger()
        val queue: PriorityQueue<Priority, TestObject> = createQueue()
        val threadFunction = Runnable {
            try {
                while (true) {
                    queue.lock().use { ignored ->
                        val value = TestObject(counter)
                        val p: Priority
                        p = when (value.number % 5) {
                            0 -> Priority.lowest
                            1 -> Priority.below_normal
                            2 -> Priority.normal
                            3 -> Priority.above_normal
                            else -> Priority.highest
                        }
                        queue.push(p, value)
                    }
                }
            } catch (e: RuntimeException) {
                // ignore
            }
        }
        time("concurrentBenchmark") {
            val numberOfThreads = 4
            val threads = arrayOfNulls<Thread>(numberOfThreads)
            for (i in 0 until numberOfThreads) {
                threads[i] = Thread(threadFunction)
            }
            for (i in 0 until numberOfThreads) {
                threads[i]!!.start()
            }
            for (i in 0 until numberOfThreads) {
                try {
                    threads[i]!!.join()
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    // ignore
                }
            }
        }
        val numbers = IntHashSet()
        var to: TestObject?
        while (queue.pop().also { to = it } != null) {
            numbers.add(to!!.number)
        }
        Assert.assertEquals(TestObject.MAX_TEST_OBJECTS.toLong(), numbers.size.toLong())
    }

    private fun populateQueue(): PriorityQueue<Priority, String> {
        val queue: PriorityQueue<Priority, String> = createQueue()
        queue.push(Priority.normal, "1")
        queue.push(Priority.above_normal, "2")
        queue.push(Priority.below_normal, "3")
        queue.push(Priority.above_normal, "4")
        queue.push(Priority.below_normal, "5")
        queue.push(Priority.lowest, "6")
        queue.push(Priority.highest, "7")
        queue.push(Priority.lowest, "8")
        return queue
    }

    private fun populateAndCopyQueue(): PriorityQueue<Priority, String> {
        val source = populateQueue()
        val result: PriorityQueue<Priority, String> = createQueue()
        PriorityQueue.moveQueue(source, result)
        return result
    }

    private class TestObject(var number: Int) {
        init {
            if (number >= MAX_TEST_OBJECTS) {
                throw RuntimeException()
            }
        }

        constructor(counter: AtomicInteger) : this(counter.getAndIncrement())

        override fun equals(o: Any?): Boolean {
            return number == (o as TestObject?)!!.number
        }

        override fun hashCode(): Int {
            return number
        }

        companion object {
            const val MAX_TEST_OBJECTS = 3000000
        }
    }
}
