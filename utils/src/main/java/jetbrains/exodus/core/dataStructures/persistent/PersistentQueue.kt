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

class PersistentQueue<T> {
    private val incoming: PersistentStack<T?>

    // invariant: if outgoing is empty then incoming is also empty
    private val outgoing: PersistentStack<T?>

    private constructor() {
        incoming = PersistentStack.Companion.EMPTY_STACK
        outgoing = PersistentStack.Companion.EMPTY_STACK
    }

    private constructor(`in`: PersistentStack<T?>, out: PersistentStack<T?>) {
        incoming = `in`
        outgoing = out
    }

    fun size(): Int {
        return incoming.size() + outgoing.size()
    }

    fun add(element: T): PersistentQueue<T?> {
        return if (isEmpty) {
            PersistentQueue<Any?>(
                PersistentStack.Companion.EMPTY_STACK,
                PersistentStack.Companion.EMPTY_STACK.push(element)
            )
        } else PersistentQueue<T?>(incoming.push(element), outgoing)
    }

    val isEmpty: Boolean
        get() = outgoing.isEmpty

    fun peek(): T? {
        if (isEmpty) {
            throw NoSuchElementException()
        }
        return outgoing.peek()
    }

    fun skip(): PersistentQueue<T?> {
        if (isEmpty) {
            throw NoSuchElementException()
        }
        val out = outgoing.skip()
        return if (out.isEmpty) {
            PersistentQueue<Any?>(PersistentStack.Companion.EMPTY_STACK, incoming.reverse())
        } else PersistentQueue<T?>(incoming, out)
    }

    override fun hashCode(): Int {
        return incoming.hashCode() + outgoing.hashCode()
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is PersistentQueue<*>) {
            return false
        }
        val queue = obj as PersistentQueue<T>
        return incoming == queue.incoming && outgoing == queue.outgoing
    }

    companion object {
        val EMPTY: PersistentQueue<*> = PersistentQueue<Any?>()
    }
}
