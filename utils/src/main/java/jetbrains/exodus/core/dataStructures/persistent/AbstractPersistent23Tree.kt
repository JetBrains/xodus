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

import jetbrains.exodus.core.dataStructures.*
import jetbrains.exodus.core.dataStructures.Stack
import jetbrains.exodus.util.*
import java.util.*

abstract class AbstractPersistent23Tree<K : Comparable<K>?> : Iterable<K> {
    abstract val root: RootNode<K>?
    operator fun contains(key: K): Boolean {
        val root: Node<K>? = this.root
        return root != null && root[key] != null
    }

    val isEmpty: Boolean
        get() = this.root == null

    fun size(): Int {
        val root = this.root
        return root?.size ?: 0
    }

    val minimum: K?
        get() {
            val it: Iterator<K> = iterator()
            return if (it.hasNext()) it.next() else null
        }
    val maximum: K?
        get() {
            val it = reverseIterator()
            return if (it.hasNext()) it.next() else null
        }

    class TreePos<K : Comparable<K>?> internal constructor(var node: Node<K>?) {
        var pos = 0
    }

    override fun iterator(): MutableIterator<K> {
        val root = this.root ?: return Collections.EMPTY_LIST.iterator()
        val stack: Array<TreePos<K>> = arrayOfNulls<TreePos<*>>(
            MathUtil.integerLogarithm(
                root.size
            ) + 1
        )
        for (i in stack.indices) {
            stack[i] = TreePos(root)
        }
        return object : MutableIterator<K> {
            private var i = 0
            private var hasNext = false
            private var hasNextValid = false
            override fun hasNext(): Boolean {
                if (hasNextValid) {
                    return hasNext
                }
                hasNextValid = true
                var treePos = stack[i]
                if (treePos.node!!.isLeaf) {
                    while (treePos.pos >= if (treePos.node!!.isTernary) 2 else 1) {
                        if (--i < 0) {
                            hasNext = false
                            return false
                        }
                        treePos = stack[i]
                    }
                } else {
                    var newPos = stack[++i]
                    newPos.pos = 0
                    if (treePos.pos == 0) {
                        newPos.node = treePos.node!!.firstChild
                    } else if (treePos.pos == 1) {
                        newPos.node = treePos.node!!.secondChild
                    } else {
                        newPos.node = treePos.node!!.thirdChild
                    }
                    treePos = newPos
                    while (!treePos.node!!.isLeaf) {
                        newPos = stack[++i]
                        newPos.pos = 0
                        newPos.node = treePos.node!!.firstChild
                        treePos = newPos
                    }
                }
                treePos.pos++
                return true.also { hasNext = it }
            }

            override fun next(): K {
                if (!hasNext()) {
                    throw NoSuchElementException()
                }
                hasNextValid = false
                val treePos = stack[i]
                // treePos.pos must be 1 or 2 here
                return if (treePos.pos == 1) treePos.node!!.firstKey else treePos.node!!.secondKey
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    /**
     * Returns an iterator that iterates over all elements greater or equal to key in ascending order
     *
     * @param key key
     * @return iterator
     */
    fun tailIterator(key: K): Iterator<K> {
        return object : MutableIterator<K> {
            private var stack: Stack<TreePos<K>>? = null
            private var hasNext = false
            private var hasNextValid = false
            override fun hasNext(): Boolean {
                if (hasNextValid) {
                    return hasNext
                }
                hasNextValid = true
                if (stack == null) {
                    val root: Node<K> = this.root
                    if (root == null) {
                        hasNext = false
                        return false
                    }
                    stack = Stack()
                    if (!root.getLess(key, stack!!)) {
                        stack!!.push(TreePos(root))
                    }
                }
                var treePos = stack!!.peek()!!
                if (treePos.node!!.isLeaf) {
                    while (treePos.pos >= if (treePos.node!!.isTernary) 2 else 1) {
                        stack!!.pop()
                        if (stack!!.isEmpty()) {
                            hasNext = false
                            return false
                        }
                        treePos = stack!!.peek()!!
                    }
                } else {
                    treePos = if (treePos.pos == 0) {
                        TreePos(
                            treePos.node!!.firstChild
                        )
                    } else if (treePos.pos == 1) {
                        TreePos(
                            treePos.node!!.secondChild
                        )
                    } else {
                        TreePos(
                            treePos.node!!.thirdChild
                        )
                    }
                    stack!!.push(treePos)
                    while (!treePos.node!!.isLeaf) {
                        treePos = TreePos(
                            treePos.node!!.firstChild
                        )
                        stack!!.push(treePos)
                    }
                }
                treePos.pos++
                hasNext = true
                return true
            }

            override fun next(): K {
                if (!hasNext()) {
                    throw NoSuchElementException()
                }
                hasNextValid = false
                val treePos = stack!!.peek()!!
                // treePos.pos must be 1 or 2 here
                return if (treePos.pos == 1) treePos.node!!.firstKey else treePos.node!!.secondKey
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    private class TreePosRev<K : Comparable<K>?> internal constructor(node: Node<K>?) {
        var node: Node<K>? = null
        var pos = 0

        init {
            setNode(node)
        }

        fun setNode(node: Node<K>?) {
            this.node = node
            pos = 2
            if (node!!.isTernary) {
                pos = 3
            }
        }
    }

    fun reverseIterator(): Iterator<K?> {
        if (isEmpty) {
            return Collections.EMPTY_LIST.iterator()
        }
        val root = this.root
        val stack: Array<TreePosRev<K>> = arrayOfNulls<TreePosRev<*>>(
            MathUtil.integerLogarithm(
                root!!.size
            ) + 1
        )
        for (i in stack.indices) {
            stack[i] = TreePosRev(root)
        }
        return object : MutableIterator<K> {
            private var i = 0
            private var hasNext = false
            private var hasNextValid = false
            override fun hasNext(): Boolean {
                if (hasNextValid) {
                    return hasNext
                }
                hasNextValid = true
                var treePos = stack[i]
                if (treePos.node!!.isLeaf) {
                    while (treePos.pos <= 1) {
                        if (--i < 0) {
                            hasNext = false
                            return false
                        }
                        treePos = stack[i]
                    }
                } else {
                    var newPos = stack[++i]
                    if (treePos.pos == 1) {
                        newPos.setNode(treePos.node!!.firstChild)
                    } else if (treePos.pos == 2) {
                        newPos.setNode(treePos.node!!.secondChild)
                    } else {
                        newPos.setNode(treePos.node!!.thirdChild)
                    }
                    treePos = newPos
                    while (!treePos.node!!.isLeaf) {
                        newPos = stack[++i]
                        newPos.setNode(if (treePos.node!!.isTernary) treePos.node!!.thirdChild else treePos.node!!.secondChild)
                        treePos = newPos
                    }
                }
                treePos.pos--
                return true.also { hasNext = it }
            }

            override fun next(): K {
                if (!hasNext()) {
                    throw NoSuchElementException()
                }
                hasNextValid = false
                val treePos = stack[i]
                // treePos.pos must be 1 or 2 here
                return if (treePos.pos == 1) treePos.node!!.firstKey else treePos.node!!.secondKey
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    /**
     * Returns an iterator that iterates over all elements greater or equal to key in descending order
     *
     * @param key key
     * @return iterator
     */
    fun tailReverseIterator(key: K): Iterator<K> {
        return object : MutableIterator<K> {
            private var stack: Stack<TreePosRev<K>>? = null
            private var hasNext = false
            private var hasNextValid = false
            private var bound: K? = null
            override fun hasNext(): Boolean {
                if (hasNextValid) {
                    return hasNext
                }
                hasNextValid = true
                if (stack == null) {
                    val root: Node<K> = this.root
                    if (root == null) {
                        hasNext = false
                        return false
                    }
                    bound = getLess(root, key)
                    stack = Stack()
                    stack!!.push(TreePosRev(root))
                }
                var treePos = stack!!.peek()!!
                if (treePos.node!!.isLeaf) {
                    while (treePos.pos <= 1) {
                        stack!!.pop()
                        if (stack!!.isEmpty()) {
                            hasNext = false
                            return false
                        }
                        treePos = stack!!.peek()!!
                    }
                } else {
                    treePos = if (treePos.pos == 1) {
                        TreePosRev(treePos.node!!.firstChild)
                    } else if (treePos.pos == 2) {
                        TreePosRev(treePos.node!!.secondChild)
                    } else {
                        TreePosRev(treePos.node!!.thirdChild)
                    }
                    stack!!.push(treePos)
                    while (!treePos.node!!.isLeaf) {
                        treePos =
                            TreePosRev(if (treePos.node!!.isTernary) treePos.node!!.thirdChild else treePos.node!!.secondChild)
                        stack!!.push(treePos)
                    }
                }
                treePos.pos--
                hasNext = tryNext() !== bound
                return hasNext
            }

            override fun next(): K {
                if (!hasNext()) {
                    throw NoSuchElementException()
                }
                hasNextValid = false
                return tryNext()
            }

            private fun tryNext(): K {
                val treePos = stack!!.peek()!!
                // treePos.pos must be 1 or 2 here
                return if (treePos.pos == 1) treePos.node!!.firstKey else treePos.node!!.secondKey
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    interface Node<K : Comparable<K>?> {
        val firstChild: Node<K>?
        val secondChild: Node<K>?
        val thirdChild: Node<K>?
        val firstKey: K
        val secondKey: K
        val isLeaf: Boolean
        val isTernary: Boolean
        fun asRoot(size: Int): RootNode<K>

        /**
         * Inserts key into tree.
         * If tree already contains K k for which key.compareTo(k) == 0 tree *is* modified.
         * This is used by implementation of map using this set.
         *
         * @param key key to insert
         * @return result of insertion: either resulting node or two nodes with key to be added on the parent level
         */
        fun insert(key: K): SplitResult<K>

        /**
         * @param key    key to remove
         * @param strict if strict is true removes key, else removes the first greater key
         * @return Replacing node of the new version of the tree and the removed key.
         * Node will be null if key's not found, replacing node of the resulting tree if no merge required,
         * node with single child and no key if merge is needed.
         * Key will be null if there's no such key.
         */
        fun remove(key: K, strict: Boolean): Pair<Node<K>, K>?
        operator fun get(key: K): K
        fun getByWeight(weight: Long): K
        fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean

        /**
         * checks the subtree of this node
         *
         * @param from every key in this subtree must be at least from
         * @param to   every key in this subtree must be at most to
         * @return the depth of this subtree: must be the same for all the children
         */
        fun checkNode(from: K?, to: K?): Int
        fun print(): String
        fun count(count: IntArray)
    }

    interface RootNode<K : Comparable<K>?> : Node<K> {
        val size: Int
    }

    internal class RootBinaryNode<K : Comparable<K>?>(firstKey: K, override val size: Int) : BinaryNode<K>(firstKey),
        RootNode<K>

    internal open class BinaryNode<K : Comparable<K>?>(override val firstKey: K) : Node<K> {

        override val firstChild: Node<K>?
            get() = null
        override val secondChild: Node<K>?
            get() = null
        override val thirdChild: Node<K>?
            get() {
                throw UnsupportedOperationException()
            }
        override val secondKey: K
            get() {
                throw UnsupportedOperationException()
            }
        override val isLeaf: Boolean
            get() = true
        override val isTernary: Boolean
            get() = false

        override fun asRoot(size: Int): RootNode<K> {
            return RootBinaryNode(firstKey, size)
        }

        override fun insert(key: K): SplitResult<K> {
            val comp = key!!.compareTo(firstKey)
            if (comp < 0) {
                return SplitResult<K>().fill(TernaryNode(key, firstKey)).setSizeChanged()
            }
            return if (comp == 0) {
                SplitResult<K>().fill(
                    BinaryNode(
                        key
                    )
                )
            } else SplitResult<K>().fill(TernaryNode(firstKey, key)).setSizeChanged()
        }

        override fun remove(key: K, strict: Boolean): Pair<Node<K>, K>? {
            val comp = if (strict) key!!.compareTo(firstKey) else -1
            return if (strict && comp != 0) {
                null
            } else {
                Pair(RemovedNode(null), firstKey)
            }
        }

        override fun get(key: K): K {
            return if (key!!.compareTo(firstKey) == 0) firstKey else null
        }

        override fun getByWeight(weight: Long): K {
            val firstKeyWeight = (firstKey as LongComparable<*>).weight
            return if (firstKeyWeight == weight) firstKey else null
        }

        override fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean {
            getLess(this, stack)
            if (firstKey!!.compareTo(key) >= 0) {
                stack.pop()
                return false
            }
            stack.peek()!!.pos++
            return true
        }

        override fun checkNode(from: K, to: K): Int {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (to != null && to.compareTo(firstKey) <= 0) {
                throw RuntimeException("Not a search tree.")
            }
            return 1
        }

        override fun print(): String {
            return firstKey.toString()
        }

        override fun count(count: IntArray) {
            count[0]++
        }
    }

    internal class RootInternalBinaryNode<K : Comparable<K>?>(
        firstChild: Node<K>,
        firstKey: K,
        secondChild: Node<K>,
        override val size: Int
    ) : InternalBinaryNode<K>(firstChild, firstKey, secondChild), RootNode<K>

    internal open class InternalBinaryNode<K : Comparable<K>?>(
        private override val firstChild: Node<K>,
        override val firstKey: K,
        private override val secondChild: Node<K>
    ) : Node<K> {
        override fun getFirstChild(): Node<K>? {
            return firstChild
        }

        override fun getSecondChild(): Node<K>? {
            return secondChild
        }

        override val thirdChild: Node<K>?
            get() {
                throw UnsupportedOperationException()
            }
        override val secondKey: K
            get() {
                throw UnsupportedOperationException()
            }
        override val isLeaf: Boolean
            get() = false
        override val isTernary: Boolean
            get() = false

        override fun asRoot(size: Int): RootNode<K> {
            return RootInternalBinaryNode(firstChild, firstKey, secondChild, size)
        }

        override fun insert(key: K): SplitResult<K> {
            val comp = key!!.compareTo(firstKey)
            if (comp < 0) {
                val splitResult = firstChild.insert(key)
                val firstNode = splitResult.firstNode
                val splitKey =
                    splitResult.key ?: return splitResult.fill(InternalBinaryNode(firstNode, firstKey, secondChild))
                // no split
                // split occurred
                return splitResult.fill(
                    InternalTernaryNode(
                        firstNode,
                        splitKey,
                        splitResult.secondNode,
                        firstKey,
                        secondChild
                    )
                )
            }
            if (comp == 0) {
                return SplitResult<K>().fill(InternalBinaryNode(firstChild, key, secondChild))
            }
            val splitResult = secondChild.insert(key)
            val firstNode = splitResult.firstNode
            val splitKey =
                splitResult.key ?: return splitResult.fill(InternalBinaryNode(firstChild, firstKey, firstNode))
            // no split
            // split occurred
            return splitResult.fill(
                InternalTernaryNode(
                    firstChild,
                    firstKey,
                    firstNode,
                    splitKey,
                    splitResult.secondNode
                )
            )
        }

        override fun remove(key: K, strict: Boolean): Pair<Node<K>, K>? {
            val comp = if (strict) key!!.compareTo(firstKey) else -1
            if (comp < 0) {
                val removeResult = firstChild.remove(key, strict) ?: return null
                val resultNode = removeResult.getFirst()
                val removedKey = removeResult.getSecond()
                return if (resultNode is RemovedNode<*>) {
                    val removedNodeResult = resultNode.firstChild
                    if (!secondChild.isTernary) {
                        val node = createNode(
                            removedNodeResult,
                            firstKey,
                            secondChild.firstChild,
                            secondChild.firstKey,
                            secondChild.secondChild
                        )
                        Pair(RemovedNode(node), removedKey)
                    } else {
                        Pair(
                            InternalBinaryNode(
                                createNode(removedNodeResult, firstKey, secondChild.firstChild), secondChild.firstKey,
                                createNode(secondChild.secondChild, secondChild.secondKey, secondChild.thirdChild)
                            ), removedKey
                        )
                    }
                } else {
                    Pair(createNode(resultNode, firstKey, secondChild), removedKey)
                }
            }
            // strict is true here
            val removeResult = secondChild.remove(key, comp != 0) ?: return null
            val resultNode = removeResult.getFirst()
            val removedKey = if (comp == 0) firstKey else removeResult.getSecond()
            val newNodeKey = if (comp != 0) firstKey else removeResult.getSecond()
            return if (resultNode is RemovedNode<*>) {
                val removedNodeResult = resultNode.firstChild
                if (!firstChild.isTernary) {
                    Pair(
                        RemovedNode(
                            createNode(
                                firstChild.firstChild,
                                firstChild.firstKey,
                                firstChild.secondChild,
                                newNodeKey,
                                removedNodeResult
                            )
                        ), removedKey
                    )
                } else {
                    Pair(
                        InternalBinaryNode(
                            createNode(firstChild.firstChild, firstChild.firstKey, firstChild.secondChild),
                            firstChild.secondKey, createNode(firstChild.thirdChild, newNodeKey, removedNodeResult)
                        ), removedKey
                    )
                }
            } else {
                Pair(
                    InternalBinaryNode(
                        firstChild, newNodeKey, resultNode
                    ), removedKey
                )
            }
        }

        override fun get(key: K): K {
            val comp = key!!.compareTo(firstKey)
            if (comp == 0) {
                return firstKey
            }
            return if (comp < 0) {
                firstChild[key]
            } else {
                secondChild[key]
            }
        }

        override fun getByWeight(weight: Long): K {
            val firstKeyWeight = (firstKey as LongComparable<*>).weight
            if (firstKeyWeight == weight) {
                return firstKey
            }
            return if (weight < firstKeyWeight) {
                firstChild.getByWeight(weight)
            } else {
                secondChild.getByWeight(weight)
            }
        }

        override fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean {
            getLess(this, stack)
            val comp = firstKey!!.compareTo(key)
            if (comp < 0) {
                stack.peek()!!.pos++
                secondChild.getLess(key, stack)
            } else if (!firstChild.getLess(key, stack)) {
                stack.pop()
                return false
            }
            return true
        }

        override fun checkNode(from: K, to: K): Int {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (to != null && to.compareTo(firstKey) <= 0) {
                throw RuntimeException("Not a search tree.")
            }
            val firstDepth = firstChild.checkNode(from, firstKey)
            val secondDepth = secondChild.checkNode(firstKey, to)
            if (firstDepth != secondDepth) {
                throw RuntimeException("Not balanced tree.")
            }
            return firstDepth + 1
        }

        override fun print(): String {
            return '('.toString() + firstChild.print() + ") " + firstKey + " (" + secondChild.print() + ')'
        }

        override fun count(count: IntArray) {
            count[1]++
            firstChild.count(count)
            secondChild.count(count)
        }
    }

    internal class RootTernaryNode<K : Comparable<K>?>(firstKey: K, secondKey: K, override val size: Int) :
        TernaryNode<K>(firstKey, secondKey), RootNode<K>

    internal open class TernaryNode<K : Comparable<K>?>(override val firstKey: K, override val secondKey: K) : Node<K> {

        override val firstChild: Node<K>?
            get() = null
        override val secondChild: Node<K>?
            get() = null
        override val thirdChild: Node<K>?
            get() = null
        override val isLeaf: Boolean
            get() = true
        override val isTernary: Boolean
            get() = true

        override fun asRoot(size: Int): RootNode<K> {
            return RootTernaryNode(firstKey, secondKey, size)
        }

        override fun insert(key: K): SplitResult<K> {
            var comp = key!!.compareTo(firstKey)
            if (comp < 0) {
                return SplitResult<K>().fill(
                    BinaryNode(key), firstKey, BinaryNode(
                        secondKey
                    )
                ).setSizeChanged()
            }
            if (comp == 0) {
                return SplitResult<K>().fill(TernaryNode(key, secondKey))
            }
            comp = key.compareTo(secondKey)
            if (comp < 0) {
                return SplitResult<K>().fill(
                    BinaryNode(
                        firstKey
                    ), key, BinaryNode(secondKey)
                ).setSizeChanged()
            }
            return if (comp == 0) {
                SplitResult<K>().fill(TernaryNode(firstKey, key))
            } else SplitResult<K>().fill(
                BinaryNode(
                    firstKey
                ), secondKey, BinaryNode(key)
            ).setSizeChanged()
        }

        override fun remove(key: K, strict: Boolean): Pair<Node<K>, K>? {
            val compFirst = if (strict) key!!.compareTo(firstKey) else -1
            if (compFirst < 0 && strict) {
                return null
            }
            if (compFirst <= 0) {
                return Pair(
                    BinaryNode(
                        secondKey
                    ), firstKey
                )
            }
            // strict is true here
            val compSecond: Int
            compSecond = key!!.compareTo(secondKey)
            return if (compSecond != 0) {
                null
            } else {
                Pair(
                    BinaryNode(
                        firstKey
                    ), secondKey
                )
            }
        }

        override fun get(key: K): K {
            val comp = key!!.compareTo(firstKey)
            if (comp == 0) {
                return firstKey
            }
            return if (comp > 0 && key.compareTo(secondKey) == 0) {
                secondKey
            } else null
        }

        override fun getByWeight(weight: Long): K {
            val keyWeight = (firstKey as LongComparable<*>).weight
            if (keyWeight == weight) {
                return firstKey
            }
            return if (weight > keyWeight && weight == (secondKey as LongComparable<*>).weight) {
                secondKey
            } else null
        }

        override fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean {
            getLess(this, stack)
            if (firstKey!!.compareTo(key) >= 0) {
                stack.pop()
                return false
            }
            if (secondKey!!.compareTo(key) < 0) {
                stack.peek()!!.pos += 2
            } else {
                stack.peek()!!.pos++
            }
            return true
        }

        override fun checkNode(from: K, to: K): Int {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (firstKey!!.compareTo(secondKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (to != null && to.compareTo(secondKey) <= 0) {
                throw RuntimeException("Not a search tree.")
            }
            return 1
        }

        override fun print(): String {
            return firstKey.toString() + ", " + secondKey
        }

        override fun count(count: IntArray) {
            count[2]++
        }
    }

    internal class RootInternalTernaryNode<K : Comparable<K>?>(
        firstChild: Node<K>, firstKey: K,
        secondChild: Node<K>, secondKey: K,
        thirdChild: Node<K>, override val size: Int
    ) : InternalTernaryNode<K>(firstChild, firstKey, secondChild, secondKey, thirdChild), RootNode<K>

    internal open class InternalTernaryNode<K : Comparable<K>?>(
        private override val firstChild: Node<K>,
        override val firstKey: K,
        private override val secondChild: Node<K>,
        override val secondKey: K,
        private override val thirdChild: Node<K>
    ) : Node<K> {
        override fun getFirstChild(): Node<K>? {
            return firstChild
        }

        override fun getSecondChild(): Node<K>? {
            return secondChild
        }

        override fun getThirdChild(): Node<K>? {
            return thirdChild
        }

        override val isLeaf: Boolean
            get() = false
        override val isTernary: Boolean
            get() = true

        override fun asRoot(size: Int): RootNode<K> {
            return RootInternalTernaryNode(firstChild, firstKey, secondChild, secondKey, thirdChild, size)
        }

        override fun insert(key: K): SplitResult<K> {
            var comp = key!!.compareTo(firstKey)
            if (comp < 0) {
                val splitResult = firstChild.insert(key)
                val firstNode = splitResult.firstNode
                val splitKey = splitResult.key ?: return splitResult.fill(cloneReplacingChild(firstChild, firstNode!!))
                // no split
                // split occurred
                return splitResult.fill(
                    InternalBinaryNode(firstNode, splitKey, splitResult.secondNode), firstKey, InternalBinaryNode(
                        secondChild, secondKey, thirdChild
                    )
                )
            }
            if (comp == 0) {
                return SplitResult<K>().fill(InternalTernaryNode(firstChild, key, secondChild, secondKey, thirdChild))
            }
            comp = key.compareTo(secondKey)
            if (comp < 0) {
                val splitResult = secondChild.insert(key)
                val firstNode = splitResult.firstNode
                val splitKey = splitResult.key ?: return splitResult.fill(cloneReplacingChild(secondChild, firstNode!!))
                // no split
                // split occurred
                return splitResult.fill(
                    InternalBinaryNode(firstChild, firstKey, firstNode), splitKey, InternalBinaryNode(
                        splitResult.secondNode, secondKey, thirdChild
                    )
                )
            }
            if (comp == 0) {
                return SplitResult<K>().fill(InternalTernaryNode(firstChild, firstKey, secondChild, key, thirdChild))
            }

            // node.secondKey != null
            val splitResult = thirdChild.insert(key)
            val firstNode = splitResult.firstNode
            val splitKey = splitResult.key ?: return splitResult.fill(cloneReplacingChild(thirdChild, firstNode!!))
            // no split
            // split occurred
            return splitResult.fill(
                InternalBinaryNode(firstChild, firstKey, secondChild),
                secondKey,
                InternalBinaryNode(firstNode, splitKey, splitResult.secondNode)
            )
        }

        override fun remove(key: K, strict: Boolean): Pair<Node<K>, K>? {
            val compFirst = if (strict) key!!.compareTo(firstKey) else -1
            if (compFirst < 0) {
                val removeResult = firstChild.remove(key, strict) ?: return null
                val resultNode = removeResult.getFirst()
                val removedKey = removeResult.getSecond()
                return if (resultNode is RemovedNode<*>) {
                    val removedNodeResult = resultNode.firstChild
                    if (!secondChild.isTernary) {
                        Pair(
                            InternalBinaryNode(
                                createNode(
                                    removedNodeResult,
                                    firstKey,
                                    secondChild.firstChild,
                                    secondChild.firstKey,
                                    secondChild.secondChild
                                ), secondKey, thirdChild
                            ), removedKey
                        )
                    } else {
                        Pair(
                            InternalTernaryNode(
                                createNode(removedNodeResult, firstKey, secondChild.firstChild),
                                secondChild.firstKey,
                                createNode(secondChild.secondChild, secondChild.secondKey, secondChild.thirdChild),
                                secondKey,
                                thirdChild
                            ), removedKey
                        )
                    }
                } else {
                    Pair(
                        cloneReplacingChild(
                            firstChild, resultNode
                        ), removedKey
                    )
                }
            }
            // strict is true here
            var compSecond = -1
            if (compFirst > 0) {
                compSecond = key!!.compareTo(secondKey)
            }
            if (compSecond < 0) {
                val removeResult = secondChild.remove(key, compFirst != 0) ?: return null
                val resultNode = removeResult.getFirst()
                val removedKey = if (compFirst == 0) firstKey else removeResult.getSecond()
                val newNodeKey = if (compFirst != 0) firstKey else removeResult.getSecond()
                return if (resultNode is RemovedNode<*>) {
                    val removedNodeResult = resultNode.firstChild
                    if (!firstChild.isTernary) {
                        Pair(
                            InternalBinaryNode(
                                createNode(
                                    firstChild.firstChild,
                                    firstChild.firstKey,
                                    firstChild.secondChild,
                                    newNodeKey,
                                    removedNodeResult
                                ), secondKey, thirdChild
                            ), removedKey
                        )
                    } else {
                        Pair(
                            InternalTernaryNode(
                                createNode(firstChild.firstChild, firstChild.firstKey, firstChild.secondChild),
                                firstChild.secondKey,
                                createNode(firstChild.thirdChild, newNodeKey, removedNodeResult),
                                secondKey,
                                thirdChild
                            ), removedKey
                        )
                    }
                } else {
                    Pair(
                        InternalTernaryNode(
                            firstChild, newNodeKey, resultNode, secondKey, thirdChild
                        ), removedKey
                    )
                }
            }
            val removeResult = thirdChild.remove(key, compSecond != 0) ?: return null
            val resultNode = removeResult.getFirst()
            val removedKey = if (compSecond == 0) secondKey else removeResult.getSecond()
            val newNodeKey = if (compSecond != 0) secondKey else removeResult.getSecond()
            return if (resultNode is RemovedNode<*>) {
                val removedNodeResult = resultNode.firstChild
                if (!secondChild.isTernary) {
                    Pair(
                        InternalBinaryNode(
                            firstChild, firstKey, createNode(
                                secondChild.firstChild,
                                secondChild.firstKey,
                                secondChild.secondChild,
                                newNodeKey,
                                removedNodeResult
                            )
                        ), removedKey
                    )
                } else {
                    Pair(
                        InternalTernaryNode(
                            firstChild, firstKey,
                            createNode(secondChild.firstChild, secondChild.firstKey, secondChild.secondChild),
                            secondChild.secondKey, createNode(secondChild.thirdChild, newNodeKey, removedNodeResult)
                        ), removedKey
                    )
                }
            } else {
                Pair(
                    InternalTernaryNode(
                        firstChild, firstKey, secondChild, newNodeKey, resultNode
                    ), removedKey
                )
            }
        }

        override fun get(key: K): K {
            var comp = key!!.compareTo(firstKey)
            if (comp < 0) {
                return firstChild[key]
            }
            if (comp == 0) {
                return firstKey
            }
            comp = key.compareTo(secondKey)
            return if (comp == 0) {
                secondKey
            } else {
                if (comp < 0) secondChild[key] else thirdChild[key]
            }
        }

        override fun getByWeight(weight: Long): K {
            var keyWeight = (firstKey as LongComparable<*>).weight
            if (weight < keyWeight) {
                return firstChild.getByWeight(weight)
            }
            if (keyWeight == weight) {
                return firstKey
            }
            keyWeight = (secondKey as LongComparable<*>).weight
            return if (keyWeight == weight) {
                secondKey
            } else {
                if (weight < keyWeight) {
                    secondChild.getByWeight(weight)
                } else {
                    thirdChild.getByWeight(weight)
                }
            }
        }

        override fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean {
            getLess(this, stack)
            var comp = secondKey!!.compareTo(key)
            if (comp < 0) {
                stack.peek()!!.pos += 2
                thirdChild.getLess(key, stack)
                return true
            }
            comp = firstKey!!.compareTo(key)
            if (comp < 0) {
                stack.peek()!!.pos++
                secondChild.getLess(key, stack)
            } else if (!firstChild.getLess(key, stack)) {
                stack.pop()
                return false
            }
            return true
        }

        fun cloneReplacingChild(oldChild: Node<K>, newChild: Node<K>): Node<K> {
            return InternalTernaryNode(
                if (firstChild === oldChild) newChild else firstChild, firstKey,
                if (secondChild === oldChild) newChild else secondChild, secondKey,
                if (thirdChild === oldChild) newChild else thirdChild
            )
        }

        override fun checkNode(from: K, to: K): Int {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (firstKey!!.compareTo(secondKey) >= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (to != null && to.compareTo(secondKey) <= 0) {
                throw RuntimeException("Not a search tree.")
            }
            if (firstChild == null || secondChild == null) {
                throw RuntimeException("The node has not enough children.")
            }
            val depth = firstChild.checkNode(from, firstKey)
            if (depth != secondChild.checkNode(firstKey, secondKey) ||
                depth != thirdChild.checkNode(secondKey, to)
            ) {
                throw RuntimeException("Not a balanced tree.")
            }
            return depth + 1
        }

        override fun print(): String {
            return '('.toString() + firstChild.print() + ") " + firstKey + " (" + secondChild.print() + ") " + secondKey + " (" + thirdChild.print() + ')'
        }

        override fun count(count: IntArray) {
            count[3]++
            firstChild.count(count)
            secondChild.count(count)
            thirdChild.count(count)
        }
    }

    internal class RemovedNode<K : Comparable<K>?>(override val firstChild: Node<K>?) : Node<K> {

        override val secondChild: Node<K>?
            get() {
                throw UnsupportedOperationException()
            }
        override val thirdChild: Node<K>?
            get() {
                throw UnsupportedOperationException()
            }
        override val firstKey: K
            get() {
                throw UnsupportedOperationException()
            }
        override val secondKey: K
            get() {
                throw UnsupportedOperationException()
            }
        override val isLeaf: Boolean
            get() {
                throw UnsupportedOperationException()
            }
        override val isTernary: Boolean
            get() {
                throw UnsupportedOperationException()
            }

        override fun asRoot(size: Int): RootNode<K> {
            throw UnsupportedOperationException()
        }

        override fun insert(key: K): SplitResult<K> {
            throw UnsupportedOperationException("Can't insert into a temporary tree.")
        }

        override fun remove(key: K, strict: Boolean): Pair<Node<K>, K>? {
            throw UnsupportedOperationException("Can't remove from a temporary tree.")
        }

        override fun get(key: K): K {
            throw UnsupportedOperationException()
        }

        override fun getByWeight(weight: Long): K {
            throw UnsupportedOperationException()
        }

        override fun getLess(key: K, stack: Stack<TreePos<K>>): Boolean {
            throw UnsupportedOperationException()
        }

        override fun checkNode(from: K?, to: K?): Int {
            throw UnsupportedOperationException()
        }

        override fun print(): String {
            throw UnsupportedOperationException()
        }

        override fun count(count: IntArray) {
            throw UnsupportedOperationException()
        }
    }

    /**
     * Used for adding values. Can be in one of the following states:
     * 1) Only firstNode is not null.
     * Means there was no split.
     * 2) All fields are not null.
     * Means the key should be inserted into the parent with firstNode and secondNode children instead the old single child.
     */
    class SplitResult<K : Comparable<K>?> {
        var firstNode: Node<K>? = null
            private set
        var secondNode: Node<K>? = null
            private set
        var key: K? = null
            private set
        var sizeChanged = false
        fun fill(first: Node<K>?, key: K?, second: Node<K>?): SplitResult<K> {
            firstNode = first
            this.key = key
            secondNode = second
            return this
        }

        fun fill(node: Node<K>): SplitResult<K> {
            return fill(node, null, null)
        }

        fun setSizeChanged(): SplitResult<K> {
            sizeChanged = true
            return this
        }
    }

    companion object {
        fun <K : Comparable<K>?> createNode(firstChild: Node<K>?, key: K, secondChild: Node<K>?): Node<K> {
            return if (firstChild == null && secondChild == null) {
                BinaryNode(key)
            } else {
                InternalBinaryNode(firstChild, key, secondChild)
            }
        }

        fun <K : Comparable<K>?> createRootNode(
            firstChild: Node<K>?, key: K,
            secondChild: Node<K>?, size: Int
        ): RootNode<K> {
            return if (firstChild == null && secondChild == null) {
                RootBinaryNode(key, size)
            } else {
                RootInternalBinaryNode(firstChild, key, secondChild, size)
            }
        }

        fun <K : Comparable<K>?> createNode(
            firstChild: Node<K>?, firstKey: K,
            secondChild: Node<K>?, secondKey: K,
            thirdChild: Node<K>?
        ): Node<K> {
            return if (firstChild == null && secondChild == null && thirdChild == null) {
                TernaryNode(firstKey, secondKey)
            } else {
                InternalTernaryNode(firstChild, firstKey, secondChild, secondKey, thirdChild)
            }
        }

        fun <K : Comparable<K>?> createRootNode(
            firstChild: Node<K>?, firstKey: K,
            secondChild: Node<K>?, secondKey: K,
            thirdChild: Node<K>?, size: Int
        ): RootNode<K> {
            return if (firstChild == null && secondChild == null && thirdChild == null) {
                RootTernaryNode(firstKey, secondKey, size)
            } else {
                RootInternalTernaryNode(firstChild, firstKey, secondChild, secondKey, thirdChild, size)
            }
        }

        /**
         * checks the subtree of the node for correctness: for tests only
         *
         * @throws RuntimeException if the tree structure is incorrect
         */
        fun <K : Comparable<K>?> checkNode(node: Node<K?>) {
            node.checkNode(null, null)
        }

        fun <K : Comparable<K>?> getLess(node: Node<K>?, stack: Stack<TreePos<K>>): Boolean {
            stack.push(TreePos(node))
            return false
        }

        fun <K : Comparable<K>?> getLess(node: Node<K>, key: K): K? {
            val stack = Stack<TreePos<K>>()
            if (!node.getLess(key, stack)) {
                return null
            }
            val treePos = stack.peek()!!
            return if (treePos.pos == 1) treePos.node!!.firstKey else treePos.node!!.secondKey
        }
    }
}
