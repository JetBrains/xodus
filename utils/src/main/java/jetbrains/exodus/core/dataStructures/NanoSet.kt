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

import java.util.*

class NanoSet<E>(private val element: E) : AbstractSet<E>() {
    override fun iterator(): MutableIterator<E> {
        return NanoIterator(this)
    }

    override fun size(): Int {
        return 1
    }

    override fun isEmpty(): Boolean {
        return false
    }

    override operator fun contains(o: Any): Boolean {
        return element === o || element == o
    }

    override fun add(e: E): Boolean {
        throw UnsupportedOperationException()
    }

    override fun addAll(c: Collection<E>): Boolean {
        throw UnsupportedOperationException()
    }

    override fun remove(o: Any): Boolean {
        throw UnsupportedOperationException()
    }

    override fun removeAll(c: Collection<*>?): Boolean {
        throw UnsupportedOperationException()
    }

    override fun clear() {
        throw UnsupportedOperationException()
    }

    private class NanoIterator<E>(set: NanoSet<E>) : MutableIterator<E?> {
        private var element: E?

        init {
            element = set.element
        }

        override fun hasNext(): Boolean {
            return element != null
        }

        override fun next(): E? {
            val result = element
            element = null
            return result
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }
}
