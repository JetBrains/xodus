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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Selector
import java.util.*

private val noQueue = arrayOf<Any?>()
private val noData = arrayOf<Comparable<Any>?>()

private val maxArraySize = Integer.MAX_VALUE - 8

private fun hugeCapacity(minCapacity: Int): Int {
    if (minCapacity < 0) {
        throw OutOfMemoryError()
    }
    return if (minCapacity > maxArraySize) {
        Integer.MAX_VALUE
    } else {
        maxArraySize
    }
}

class PriorityMap<E>(initialCapacity: Int, private val comparator: Comparator<Comparable<Any>>, private val valueGettter: Selector<E>) : AbstractQueue<E>() {

    private var queue = if (initialCapacity <= 0) {
        noQueue
    } else {
        arrayOfNulls(initialCapacity)
    }
    private var data = if (initialCapacity <= 0) {
        noData
    } else {
        arrayOfNulls(initialCapacity)
    }
    override var size: Int = 0
        private set
    private var modCount = 0

    private fun grow(minCapacity: Int) {
        val oldCapacity = queue.size
        var newCapacity = oldCapacity + if (oldCapacity < 64) {
            oldCapacity + 2
        } else {
            oldCapacity shr 1
        }
        if (newCapacity - maxArraySize > 0) {
            newCapacity = hugeCapacity(minCapacity)
        }
        queue = Arrays.copyOf(queue, newCapacity)
        data = Arrays.copyOf(data, newCapacity)
    }

    override fun add(element: E) = offer(element)

    override fun offer(e: E?): Boolean {
        if (e == null)
            throw NullPointerException()
        modCount++
        val i = size
        if (i >= queue.size) {
            grow(i + 1)
        }
        size = i + 1
        if (i == 0) {
            queue[0] = e
            data[0] = valueGettter.select(e)
        } else {
            up(i, e, valueGettter.select(e))
        }
        return true
    }

    @Suppress("UNCHECKED_CAST")
    override fun peek(): E? {
        return if (size == 0) {
            null
        } else {
            queue[0] as E
        }
    }

    override fun clear() {
        modCount++
        for (i in 0 until size) {
            queue[i] = null
        }
        for (i in 0 until size) {
            data[i] = null
        }
        size = 0
    }

    @Suppress("UNCHECKED_CAST")
    override fun poll(): E? {
        if (size == 0) {
            return null
        }
        val s = --size
        modCount++
        val result = queue[0] as E
        val x = queue[s] as E
        queue[s] = null
        val v = data[s]
        data[s] = null
        if (s != 0) {
            down(0, x, v!!)
        }
        return result
    }

    override fun iterator(): MutableIterator<E> = Itr()

    @Suppress("UNCHECKED_CAST")
    private fun up(key: Int, x: E, v: Comparable<Any>) {
        var k = key
        while (k > 0) {
            val parent = (k - 1).ushr(1)
            val ev = data[parent]
            if (comparator.compare(v, ev) >= 0) {
                break
            }
            queue[k] = queue[parent]
            data[k] = ev
            k = parent
        }
        queue[k] = x
        data[k] = v
    }

    @Suppress("UNCHECKED_CAST")
    private fun down(key: Int, x: E, v: Comparable<Any>) {
        var k = key
        val half = size.ushr(1)
        while (k < half) {
            var child = (k shl 1) + 1
            var cv = data[child]
            val right = child + 1
            if (right < size && comparator.compare(cv, data[right]) > 0) {
                child = right
                cv = data[child]
            }
            if (comparator.compare(v, cv) <= 0) {
                break
            }
            data[k] = cv
            queue[k] = queue[child]
            k = child
        }
        queue[k] = x
        data[k] = v
    }

    /*@Suppress("UNCHECKED_CAST")
    private fun removeAt(i: Int): E? {
        modCount++
        val s = --size
        if (s == i) {
            queue[i] = null
            data[i] = null
        } else {
            val moved = queue[s] as E
            queue[s] = null
            val v = data[s]
            down(i, moved, v!!)
            if (queue[i] === moved) {
                up(i, moved, v)
                if (queue[i] !== moved) {
                    return moved
                }
            }
        }
        return null
    }

    private fun removeEq(o: Any): Boolean {
        for (i in 0 until size) {
            if (o === queue[i]) {
                removeAt(i)
                return true
            }
        }
        return false
    }*/

    private inner class Itr : MutableIterator<E> {
        private var cursor = 0
        private var lastRet = -1
        private var forgetMeNot: ArrayDeque<E>? = null
        private var lastRetElt: E? = null
        private var expectedModCount = modCount

        override fun hasNext(): Boolean {
            return cursor < size || forgetMeNot != null && !forgetMeNot!!.isEmpty()
        }

        @Suppress("UNCHECKED_CAST")
        override fun next(): E {
            if (expectedModCount != modCount) {
                throw ConcurrentModificationException()
            }
            if (cursor < size) {
                val index = cursor++
                lastRet = index
                return queue[index] as E
            }
            if (forgetMeNot != null) {
                lastRet = -1
                lastRetElt = forgetMeNot!!.poll()
                lastRetElt?.let { return it }
            }
            throw NoSuchElementException()
        }

        override fun remove() {
            TODO()
            /* if (expectedModCount != modCount) {
                 throw ConcurrentModificationException()
             }
             if (lastRet != -1) {
                 val moved = removeAt(lastRet)
                 lastRet = -1
                 if (moved == null) {
                     cursor--
                 } else {
                     if (forgetMeNot == null) {
                         forgetMeNot = ArrayDeque()
                     }
                     forgetMeNot!!.add(moved)
                 }
             } else {
                 lastRetElt?.let {
                     removeEq(it)
                     lastRetElt = null
                 } ?: throw IllegalStateException()
             }
             expectedModCount = modCount*/
        }
    }
}
