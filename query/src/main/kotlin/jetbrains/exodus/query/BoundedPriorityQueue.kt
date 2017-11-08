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

class BoundedPriorityQueue<E>(private val capacity: Int, val comparator: Comparator<in E>) : AbstractQueue<E>() {
    private val queue = PriorityQueue(capacity, comparator)

    override val size: Int
        get() = queue.size

    override fun add(element: E) = offer(element)

    override fun offer(e: E): Boolean {
        if (queue.size >= capacity) {
            if (comparator.compare(e, queue.peek()) < 1) {
                return false
            }

            queue.pollRaw()
        }

        val result = queue.offer(e)
        if (!result) {
            queue.offer(e)
        }
        return result
    }

    override fun poll(): E? = queue.poll()

    override fun peek(): E? = queue.peek()

    override fun iterator() = queue.iterator()
}
