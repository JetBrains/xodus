/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import com.github.penemue.keap.PriorityQueue
import java.util.*

class BoundedPriorityQueue<E>(private val capacity: Int, private val comparator: Comparator<in E>) : AbstractQueue<E>() {

    private val maxHeap = PriorityQueue(capacity, Comparator<E> { o1, o2 -> comparator.compare(o2, o1) })
    private var minHeap: PriorityQueue<E>? = null

    override val size: Int
        get() = getMinHeap().size

    override fun add(element: E) = offer(element)

    override fun offer(e: E): Boolean {
        if (maxHeap.size >= capacity) {
            if (comparator.compare(e, maxHeap.peek()) > 0) {
                return false
            }
            maxHeap.pollRaw()
        }

        return maxHeap.offer(e)
    }

    override fun poll(): E? = getMinHeap().pollRaw()

    override fun peek(): E? = getMinHeap().peek()

    override fun iterator() = getMinHeap().iterator()

    private fun getMinHeap(): PriorityQueue<E> {
        return minHeap ?: PriorityQueue(maxHeap, comparator).apply {
            minHeap = this
        }
    }
}
