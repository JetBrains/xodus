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

import jetbrains.exodus.core.execution.locks.Guard

abstract class PriorityQueue<P : Comparable<P>?, E> : Iterable<E> {
    abstract val isEmpty: Boolean
    abstract fun size(): Int
    abstract fun push(priority: P, value: E): E
    fun peek(): E? {
        val pair = peekPair()
        return pair?.getSecond()
    }

    abstract fun peekPair(): Pair<P, E>?
    abstract fun floorPair(): Pair<P, E>?
    abstract fun pop(): E?
    abstract fun clear()
    abstract fun lock(): Guard?
    abstract fun unlock()

    companion object {
        // Returns size of the destination (and obviously of the source) queue
        fun <P : Comparable<P>?, E> moveQueue(
            source: PriorityQueue<P, E>,
            dest: PriorityQueue<P?, E?>
        ): Int {
            source.lock().use { ignored ->
                dest.lock().use { ignore ->
                    while (true) {
                        val pair = source.peekPair() ?: break
                        dest.push(pair.getFirst(), pair.getSecond())
                        source.pop()
                    }
                    return dest.size()
                }
            }
        }
    }
}
