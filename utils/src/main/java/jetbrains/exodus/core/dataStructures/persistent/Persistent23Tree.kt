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

class Persistent23Tree<K : Comparable<K>?> internal constructor(
    /**
     * The root of the last version of the tree.
     */
    @field:Volatile override var root: RootNode<K>?
) : AbstractPersistent23Tree<K>() {
    constructor() : this(null)

    fun beginRead(): ImmutableTree<K> {
        return ImmutableTree(root)
    }

    val clone: Persistent23Tree<K>
        get() = Persistent23Tree(root)

    fun beginWrite(): MutableTree<K> {
        return MutableTree(this)
    }

    fun endWrite(tree: MutableTree<K>): Boolean {
        if (root !== tree.getStartingRoot()) {
            return false
        }
        root = tree.root
        return true
    }

    open class ImmutableTree<K : Comparable<K>?> internal constructor(root: RootNode<K?>?) :
        AbstractPersistent23Tree<K>() {
        override val root: RootNode<K>?

        init {
            this.root = root
        }
    }

    open class MutableTree<K : Comparable<K>?> internal constructor(tree: Persistent23Tree<K?>) :
        AbstractPersistent23Tree<K?>() {
        private val baseTree: Persistent23Tree<K?>
        private var startingRoot: RootNode<K?>?
        override var root: RootNode<K?>?

        init {
            startingRoot = tree.root
            root = startingRoot
            baseTree = tree
        }

        fun add(key: K) {
            if (root == null) {
                root = RootBinaryNode(key, 1)
            } else {
                val splitResult = root!!.insert(key)
                // splitResult.firstNode != null
                val size = if (splitResult.sizeChanged) root.getSize() + 1 else root.getSize()
                root = if (splitResult.getSecondNode() == null) {
                    splitResult.getFirstNode().asRoot(size)
                } else {
                    RootInternalBinaryNode(
                        splitResult.getFirstNode(),
                        splitResult.getKey(),
                        splitResult.getSecondNode(),
                        size
                    )
                }
            }
        }

        fun exclude(key: K): Boolean {
            return if (root == null) {
                false
            } else {
                val removeResult = root!!.remove(key, true) ?: return false
                var result = removeResult.getFirst()
                if (result is RemovedNode<*>) {
                    result = result.getFirstChild()
                }
                root = result?.asRoot(root.getSize() - 1)
                true
            }
        }

        fun addAll(keys: Iterable<K>, size: Int) {
            addAll(keys.iterator(), size)
        }

        fun addAll(keys: Iterator<K>, size: Int) {
            if (!isEmpty) {
                throw UnsupportedOperationException()
            }
            root = makeRootNode(keys, size, 1)
        }

        /**
         * Try to merge changes into the base tree.
         *
         * @return true if merging succeeded
         */
        fun endWrite(): Boolean {
            if (baseTree.endWrite(this)) {
                startingRoot = root
                return true
            }
            return false
        }

        /**
         * Create tree of next `size` elements from `iterator` of depth at least `depth`
         *
         * @param iterator sorted sequence of keys
         * @param size     amount of keys to take from `iterator`
         * @param toDepth  minimal depth of the desired tree
         * @return root of the constructed tree
         */
        private fun makeNode(iterator: Iterator<K>, size: Int, toDepth: Int): Node<K?>? {
            if (size <= 0) {
                return null
            }
            var left = size
            var node: Node<K?>? = null
            var depth = 1
            var minSize = 0
            var maxSize = 0
            while (left > 0) {
                if (depth >= toDepth) {
                    if (left <= maxSize + 1) {
                        return AbstractPersistent23Tree.Companion.createNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, left - 1, depth - 1)
                        )
                    } else if (left <= 2 * maxSize + 2) {
                        val third = Math.max(left - 2 - maxSize, minSize)
                        return AbstractPersistent23Tree.Companion.createNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, left - 2 - third, depth - 1),
                            iterator.next(),
                            makeNode(iterator, third, depth - 1)
                        )
                    }
                }
                val minUp = (1 shl Math.max(toDepth, depth + 1)) - (1 shl depth) - 1
                var up = Math.max(left - 3 - 2 * maxSize, minUp)
                val third = Math.max(left - up - 3 - maxSize, minSize)
                var second = left - 3 - third - up
                if (second >= minSize) {
                    node = AbstractPersistent23Tree.Companion.createNode<K?>(
                        node,
                        iterator.next(),
                        makeNode(iterator, second, depth - 1),
                        iterator.next(),
                        makeNode(iterator, third, depth - 1)
                    )
                    left -= second + third + 2
                } else {
                    up = Math.max(left - 2 - maxSize, minUp)
                    second = left - 2 - up
                    node = AbstractPersistent23Tree.Companion.createNode<K?>(
                        node,
                        iterator.next(),
                        makeNode(iterator, second, depth - 1)
                    )
                    left -= second + 1
                }
                maxSize = maxSize * 3 + 2
                minSize = minSize * 2 + 1
                depth++
            }
            return node
        }

        // TODO: this copy-paste is used to prevent root nodes at non-top positions, needs polishing
        private fun makeRootNode(iterator: Iterator<K>, size: Int, toDepth: Int): RootNode<K?>? {
            if (size <= 0) {
                return null
            }
            var left = size
            var node: Node<K?>? = null
            var depth = 1
            var minSize = 0
            var maxSize = 0
            while (true) {
                if (depth >= toDepth) {
                    if (left <= maxSize + 1) {
                        return AbstractPersistent23Tree.Companion.createRootNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, left - 1, depth - 1),
                            size
                        )
                    } else if (left <= 2 * maxSize + 2) {
                        val third = Math.max(left - 2 - maxSize, minSize)
                        return AbstractPersistent23Tree.Companion.createRootNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, left - 2 - third, depth - 1),
                            iterator.next(),
                            makeNode(iterator, third, depth - 1),
                            size
                        )
                    }
                }
                val minUp = (1 shl Math.max(toDepth, depth + 1)) - (1 shl depth) - 1
                var up = Math.max(left - 3 - 2 * maxSize, minUp)
                val third = Math.max(left - up - 3 - maxSize, minSize)
                var second = left - 3 - third - up
                if (second >= minSize) {
                    left -= second + third + 2
                    node = if (left <= 0) {
                        return AbstractPersistent23Tree.Companion.createRootNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, second, depth - 1),
                            iterator.next(),
                            makeNode(iterator, third, depth - 1),
                            size
                        )
                    } else {
                        AbstractPersistent23Tree.Companion.createNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, second, depth - 1),
                            iterator.next(),
                            makeNode(iterator, third, depth - 1)
                        )
                    }
                } else {
                    up = Math.max(left - 2 - maxSize, minUp)
                    second = left - 2 - up
                    left -= second + 1
                    node = if (left <= 0) {
                        return AbstractPersistent23Tree.Companion.createRootNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, second, depth - 1),
                            size
                        )
                    } else {
                        AbstractPersistent23Tree.Companion.createNode<K?>(
                            node,
                            iterator.next(),
                            makeNode(iterator, second, depth - 1)
                        )
                    }
                }
                maxSize = maxSize * 3 + 2
                minSize = minSize * 2 + 1
                depth++
            }
        }

        fun getStartingRoot(): Node<K?>? {
            return startingRoot
        }

        fun testConsistency() {
            if (root != null) {
                AbstractPersistent23Tree.Companion.checkNode<K?>(root!!)
            }
        }
    }
}
