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

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree
import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree.MutableTree
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet.MutablePersistentHashSet
import jetbrains.exodus.core.execution.locks.Guard
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class ConcurrentStablePriorityQueue<P : Comparable<P>?, E> : PriorityQueue<P, E>() {
    private val rootPair: AtomicReference<Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>?>

    init {
        rootPair = AtomicReference()
    }

    override val isEmpty: Boolean
        get() {
            val currentPair = current
            return currentPair == null || currentPair.getFirst().isEmpty()
        }

    override fun size(): Int {
        val currentPair = current
        return currentPair?.getFirst()?.size() ?: 0
    }

    override fun push(priority: P, value: E): E {
        var currentPair: Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>?
        var newPair: Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>
        var result: E
        do {
            result = null
            currentPair = current
            val queue: Persistent23Tree<TreeNode<P, E>>?
            val values: PersistentHashSet<IdentifiedTreeNode<P, E>>?
            val mutableQueue: MutableTree<TreeNode<P, E>>?
            val mutableValues: MutablePersistentHashSet<IdentifiedTreeNode<P, E>>?
            val node = TreeNode(priority, value)
            val idNode = IdentifiedTreeNode(node)
            if (currentPair == null) {
                queue = Persistent23Tree()
                values = PersistentHashSet()
                mutableQueue = queue.beginWrite()
                mutableValues = values.beginWrite()
            } else {
                queue = currentPair.getFirst().getClone()
                values = currentPair.getSecond().getClone()
                mutableQueue = queue.beginWrite()
                mutableValues = values.beginWrite()
                val oldIdNode = mutableValues.getKey(idNode)
                if (oldIdNode != null) {
                    val oldNode = oldIdNode.node
                    result = oldNode.value
                    mutableValues.remove(oldIdNode)
                    mutableQueue.exclude(oldNode)
                }
            }
            mutableQueue.add(node)
            mutableValues.add(idNode)
            // commit trees and then try to commit pair of trees
            // no need to check endWrite() results since they commit cloned trees
            mutableQueue.endWrite()
            mutableValues.endWrite()
            newPair = Pair(queue, values)
            // commit pair if no other pair was already committed
        } while (!rootPair.compareAndSet(currentPair, newPair))
        return result
    }

    override fun peekPair(): Pair<P, E>? {
        val currentPair = current ?: return null
        val max = currentPair.getFirst().getMaximum()
        return if (max == null) null else Pair(max.priority, max.value)
    }

    override fun floorPair(): Pair<P, E>? {
        val currentPair = current ?: return null
        val min = currentPair.getFirst().getMinimum()
        return if (min == null) null else Pair(min.priority, min.value)
    }

    override fun pop(): E {
        var currentPair: Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>?
        var newPair: Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>?
        var result: E
        do {
            result = null
            currentPair = current
            if (currentPair == null) {
                break
            }
            val queue = currentPair.getFirst().getClone()
            val values = currentPair.getSecond().getClone()
            val mutableQueue = queue!!.beginWrite()
            val mutableValues = values!!.beginWrite()
            val max = mutableQueue.maximum ?: break
            mutableQueue!!.exclude(max)
            mutableValues!!.remove(IdentifiedTreeNode(max))
            result = max.value
            // commit trees and then try to commit pair of trees
            // no need to check endWrite() results since they commit cloned trees
            mutableQueue!!.endWrite()
            mutableValues!!.endWrite()
            // if the queue becomes empty the newPair reference can be null
            newPair = if (queue.isEmpty) null else Pair(queue, values)
            // commit pair if no other pair was already committed
        } while (!rootPair.compareAndSet(currentPair, newPair))
        return result
    }

    override fun clear() {
        rootPair.set(null)
    }

    override fun lock(): Guard? {
        return Guard.Companion.EMPTY
    }

    override fun unlock() {}
    override fun iterator(): MutableIterator<E> {
        val currentPair = current ?: return Collections.emptyIterator()
        val iterator = currentPair.getFirst().reverseIterator()
        return object : MutableIterator<E> {
            override fun hasNext(): Boolean {
                return iterator!!.hasNext()
            }

            override fun next(): E {
                return iterator!!.next().value
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    private val current: Pair<Persistent23Tree<TreeNode<P, E>>?, PersistentHashSet<IdentifiedTreeNode<P, E>>?>?
        private get() = rootPair.get()

    private class TreeNode<P : Comparable<P>?, E> private constructor(
        val priority: P,
        private val samePriorityOrder: Int,
        val value: E
    ) : Comparable<TreeNode<P, E>> {
        constructor(priority: P, value: E) : this(priority, orderCounter.getAndDecrement(), value)

        override fun compareTo(o: TreeNode<P, E>): Int {
            var result = priority!!.compareTo(o.priority)
            if (result == 0) {
                result = samePriorityOrder - o.samePriorityOrder
            }
            return result
        }

        companion object {
            private val orderCounter = AtomicInteger(Int.MAX_VALUE)
        }
    }

    private class IdentifiedTreeNode<P : Comparable<P>?, E>(val node: TreeNode<P, E>) {
        override fun hashCode(): Int {
            return node.value.hashCode()
        }

        override fun equals(obj: Any?): Boolean {
            val o = obj as IdentifiedTreeNode<P, E>?
            return node.value == o!!.node.value
        }
    }
}
