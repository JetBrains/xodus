/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate.binop

import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator
import java.util.*

internal fun toEntityIdIterator(it: EntityIterator): Iterator<EntityId?> {
    return object : Iterator<EntityId?> {

        override fun hasNext() = it.hasNext()

        override fun next() = it.nextId()
    }
}

internal fun toSortedEntityIdIterator(it: EntityIterator): Iterator<EntityId?> {
    var array = arrayOfNulls<EntityId>(8)
    var size = 0
    while (it.hasNext()) {
        if (size == array.size) {
            array = array.copyOf(size * 2)
        }
        array[size++] = it.nextId()
    }
    if (size > 1) {
        Arrays.sort(array, 0, size) { o1, o2 ->
            when {
                o1 == null -> 1
                o2 == null -> -1
                else -> o1.compareTo(o2)
            }
        }
    }
    return object : Iterator<EntityId?> {
        var i = 0

        override fun hasNext() = i < size

        override fun next() = array[i++]
    }
}