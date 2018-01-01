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

import jetbrains.exodus.entitystore.ComparableGetter
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.kotlin.notNull
import java.util.*

class InMemoryMergeSortIterableWithValueGetter(private val source: Iterable<Entity>, private val valueGetter: ComparableGetter, private val comparator: Comparator<Comparable<Any>>) : Iterable<Entity> {

    override fun iterator(): MutableIterator<Entity> {
        return object : MutableIterator<Entity> {

            private var values: Array<Comparable<Any>?> = arrayOfNulls(1)
            private var src: Array<Entity?> = arrayOfNulls(1)
            private var size: Int = 0
            private var height: Int = 0
            private var size2: Int = 0
            private var next: IntArray
            private var current: Int = 0

            init {
                src = arrayOfNulls<Entity>(1)
                for (entity in source) {
                    if (size >= src.size) {
                        val size = src.size shl 1
                        src = Arrays.copyOf(src, size)
                        values = Arrays.copyOf(values, size)
                    }
                    val currentSize = size
                    src[currentSize] = entity
                    values[currentSize] = valueGetter.select(entity)
                    size++
                }
                height = 1
                run {
                    var i = size
                    while (i > 1) {
                        height++
                        i = i + 1 shr 1
                    }
                }
                next = IntArray(1 shl height)
                size2 = 1 shl height - 1
                for (i in 0..size - 1) {
                    next[i + size2] = i
                }
                for (i in size2 + size..next.size - 1) {
                    next[i] = size
                }
                for (i in 1..size2 - 1) {
                    next[i] = -1
                }
                current = 0
            }

            override fun hasNext(): Boolean {
                return current < size
            }

            override fun next(): Entity {
                var segment = 1
                // next[current] is index of the least remaining element on current segment
                // next[current] == -1 means minimum on current segment is not counted yet
                // next[current] == src.size() means current segment is exhausted
                while (next[1] < 0) {
                    segment = segment shl 1
                    if (segment >= size2 || next[segment] >= 0 && next[segment + 1] >= 0) {
                        if (next[segment + 1] >= size || next[segment] < size && comparator.compare(values[next[segment]], values[next[segment + 1]]) <= 0) {
                            next[segment shr 1] = next[segment]
                        } else {
                            next[segment shr 1] = next[segment + 1]
                        }
                        segment = segment shr 2
                    } else if (next[segment] >= 0) {
                        segment++
                    }
                }
                val r = next[1]
                if (r >= size) {
                    throw NoSuchElementException()
                }
                next[r + size2] = size
                var i = r + size2 shr 1
                while (i >= 1) {
                    next[i] = -1
                    i = i shr 1
                }
                current++
                return src[r].notNull
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }
}
