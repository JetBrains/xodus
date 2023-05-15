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
package jetbrains.exodus.core.dataStructures.persistent

class PersistentStack<T> : Iterable<T> {
    private val element: T?
    private val size: Int
    private val next: PersistentStack<T>?

    private constructor() {
        element = null
        size = 0
        next = null
    }

    private constructor(e: T, stack: PersistentStack<T>) {
        element = e
        size = stack.size + 1
        next = stack
    }

    val isEmpty: Boolean
        get() = element == null

    fun size(): Int {
        return size
    }

    fun push(e: T): PersistentStack<T> {
        return PersistentStack(e, this)
    }

    fun peek(): T? {
        if (isEmpty) {
            throw NoSuchElementException()
        }
        return element
    }

    fun skip(): PersistentStack<T>? {
        if (isEmpty) {
            throw NoSuchElementException()
        }
        return next
    }

    fun reverse(): PersistentStack<T> {
        var result = PersistentStack<T>()
        var stack: PersistentStack<T>? = this
        while (!stack!!.isEmpty) {
            result = PersistentStack(stack.peek(), result)
            stack = stack.skip()
        }
        return result
    }

    override fun hashCode(): Int {
        return if (isEmpty) 271828182 else element.hashCode() + next.hashCode()
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is PersistentStack<*>) {
            return false
        }
        val stack = obj as PersistentStack<T>
        if (isEmpty) {
            return stack.isEmpty
        }
        return if (stack.isEmpty) {
            false
        } else element == stack.element && next == stack.next
    }

    override fun iterator(): MutableIterator<T> {
        val current = arrayOf<PersistentStack<*>?>(this)
        return object : MutableIterator<T> {
            override fun hasNext(): Boolean {
                return !current[0]!!.isEmpty
            }

            override fun next(): T {
                val result = current[0]!!.element as T
                current[0] = current[0]!!.next
                return result
            }

            override fun remove() {
                throw UnsupportedOperationException("remove")
            }
        }
    }

    companion object {
        val EMPTY_STACK: PersistentStack<*> = PersistentStack<Any?>()
    }
}
