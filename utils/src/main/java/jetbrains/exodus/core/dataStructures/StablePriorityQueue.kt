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

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet
import jetbrains.exodus.core.execution.locks.CriticalSection
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class StablePriorityQueue<P : Comparable<P>?, E> : PriorityQueue<P, E>() {
    private val theQueue: TreeMap<P?, LinkedHashSet<E>>
    private val priorities: MutableMap<E, Pair<E, P>>
    private val size: AtomicInteger
    private val criticalSection: CriticalSection

    init {
        theQueue = TreeMap()
        priorities = HashMap()
        size = AtomicInteger(0)
        criticalSection = CriticalSection()
    }

    override val isEmpty: Boolean
        get() = size.get() == 0

    override fun size(): Int {
        return size.get()
    }

    override fun push(priority: P, value: E): E {
        var values: LinkedHashSet<E>?
        val oldPair = priorities.remove(value)
        priorities[value] = Pair(value, priority)
        invalidateSize()
        val oldPriority = oldPair?.getSecond()
        val oldValue = oldPair?.getFirst()
        if (oldPriority != null && theQueue[oldPriority].also { values = it } != null) {
            values!!.remove(value)
            if (values!!.isEmpty()) {
                theQueue.remove(oldPriority)
            }
        }
        values = theQueue[priority]
        if (values == null) {
            values = LinkedHashSet()
            theQueue[priority] = values!!
        }
        values!!.add(value)
        return oldValue
    }

    override fun peekPair(): Pair<P, E>? {
        if (priorities.size == 0) {
            return null
        }
        val queue = theQueue
        val priority: P = queue.lastKey()
        val values = queue[priority]!!
        return Pair(priority, values.back)
    }

    override fun floorPair(): Pair<P, E>? {
        if (priorities.size == 0) {
            return null
        }
        val queue = theQueue
        val priority: P = queue.firstKey()
        val values = queue[priority]!!
        return Pair(priority, values.top)
    }

    override fun pop(): E {
        if (priorities.size == 0) {
            return null
        }
        val queue = theQueue
        val priority = queue.lastKey()
        val values: MutableSet<E> = queue[priority]!!
        val result = values.iterator().next()
        priorities.remove(result)
        invalidateSize()
        values.remove(result)
        if (values.isEmpty()) {
            queue.remove(priority)
        }
        return result
    }

    override fun clear() {
        lock().use { ignored ->
            theQueue.clear()
            priorities.clear()
            size.set(0)
        }
    }

    override fun lock(): CriticalSection? {
        return criticalSection.enter()
    }

    override fun unlock() {
        criticalSection.unlock()
    }

    override fun iterator(): MutableIterator<E> {
        return QueueIterator()
    }

    fun remove(value: E): Boolean {
        val pair = priorities.remove(value) ?: return false
        invalidateSize()
        val priority = pair.getSecond()
        val values = theQueue[priority]!!
        values.remove(value)
        if (values.isEmpty()) {
            theQueue.remove(priority)
        }
        return true
    }

    private fun invalidateSize() {
        size.set(priorities.size)
    }

    private inner class QueueIterator private constructor() : MutableIterator<E?> {
        private val priorityIt: Iterator<Map.Entry<P?, LinkedHashSet<E>>>
        private var currentIt: Iterator<E?>

        init {
            priorityIt = theQueue.descendingMap().entries.iterator()
            currentIt = Collections.EMPTY_LIST.iterator()
            checkCurrentIterator()
        }

        override fun hasNext(): Boolean {
            return currentIt.hasNext()
        }

        override fun next(): E? {
            val result = currentIt.next()
            checkCurrentIterator()
            return result
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }

        private fun checkCurrentIterator() {
            while (!currentIt.hasNext()) {
                if (!priorityIt.hasNext()) {
                    break
                }
                val (_, value) = priorityIt.next()
                currentIt = value.iterator()
            }
        }
    }
}
