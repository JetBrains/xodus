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
package jetbrains.exodus.core.dataStructures.decorators


class LinkedHashSetDecorator<E>() : MutableSet<E> {
    private var decorated: MutableSet<E>? = null

    init {
        clear()
    }

    override val size: Int
        get() {
            return decorated?.size ?: 0
        }

    override fun isEmpty(): Boolean {
        return decorated.isNullOrEmpty()
    }

    override operator fun contains(element: E): Boolean {
        return decorated?.contains(element) ?: false
    }

    override fun iterator(): MutableIterator<E> {
        return decorated?.iterator() ?: (mutableSetOf<E>().iterator())
    }

    override fun add(element: E): Boolean {
        checkDecorated()
        return decorated!!.add(element)
    }

    override fun remove(element: E): Boolean {
        val decorated = decorated ?: return false

        val result = decorated.remove(element)
        if (result && decorated.isEmpty()) {
            clear()
        }
        return result
    }

    override fun containsAll(elements: Collection<E>): Boolean {
        return decorated?.containsAll(elements) ?: false
    }

    override fun addAll(elements: Collection<E>): Boolean {
        checkDecorated()
        return decorated!!.addAll(elements)
    }

    override fun retainAll(elements: Collection<E>): Boolean {
        checkDecorated()
        return decorated!!.retainAll(elements.toSet())
    }

    override fun removeAll(elements: Collection<E>): Boolean {
        if (decorated == null) {
            return false
        }

        val result = decorated!!.removeAll(elements.toSet())
        if (result && decorated!!.isEmpty()) {
            clear()
        }
        return result
    }

    override fun clear() {
        decorated = null
    }

    private fun checkDecorated() {
        if (decorated === emptySet<Any>()) {
            decorated = LinkedHashSet()
        }
    }
}