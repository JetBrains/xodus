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
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.kotlin.notNull
import java.util.*

class InMemoryKeapSortIterable(source: Iterable<Entity>, comparator: Comparator<Entity>) : SortEngine.InMemorySortIterable(source, comparator) {

    override fun iterator(): MutableIterator<Entity> {
        return object : MutableIterator<Entity> {

            val queue: PriorityQueue<Entity> = PriorityQueue(comparator)

            init {
                source.forEach { queue.offer(it) }
            }

            override fun hasNext(): Boolean {
                return !queue.isEmpty()
            }

            override fun next(): Entity {
                return queue.pollRaw().notNull
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }
}