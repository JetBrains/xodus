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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.*
import jetbrains.exodus.tree.*
import java.util.*

open class BTreeTraverser : TreeTraverser {
    @JvmField
    protected var stack = arrayOfNulls<TreePos>(8)

    @JvmField
    var top = 0

    @JvmField
    var currentNode: BasePage

    @JvmField
    var node: ILeafNode = ILeafNode.EMPTY

    @JvmField
    var currentPos = 0

    constructor(currentNode: BasePage) {
        this.currentNode = currentNode
    }

    // for tests only
    private constructor(source: BTreeTraverser) {
        stack = source.stack // tricky
        currentNode = source.currentNode
        currentPos = source.currentPos
    }

    override fun init(left: Boolean) {
        val size = currentNode.size
        currentPos = if (left) 0 else size - 1
        if (!canMoveDown()) {
            currentPos = if (left) -1 else size
        }
    }

    override fun isNotEmpty(): Boolean = currentNode.size > 0
    override fun getKey(): ByteIterable = node.getKey()
    override fun getValue(): ByteIterable = node.getValue() ?: throw NullPointerException()

    override fun hasValue(): Boolean {
        return node.hasValue()
    }

    override fun moveDown(): INode {
        return pushChild(TreePos(currentNode, currentPos), getChildForMoveDown()!!, 0).also { node = it!! }!!
    }

    override fun moveDownToLast(): INode {
        val child = getChildForMoveDown()
        return pushChild(TreePos(currentNode, currentPos), child!!, child.size - 1).also { node = it!! }!!
    }

    protected open fun getChildForMoveDown(): BasePage? = currentNode.getChild(currentPos)

    protected open fun pushChild(topPos: TreePos, child: BasePage, pos: Int): ILeafNode? {
        setAt(top, topPos)
        currentNode = child
        currentPos = pos
        ++top
        return if (child.isBottom()) {
            handleLeaf(child.getKey(pos))
        } else {
            ILeafNode.EMPTY
        }
    }

    protected open fun handleLeaf(leaf: BaseLeafNode): ILeafNode? {
        return leaf
    }

    fun setAt(pos: Int, treePos: TreePos) {
        val length = stack.size
        if (pos >= length) { // ensure capacity
            stack = stack.copyOf(length shl 1)
        }
        stack[pos] = treePos
    }

    override fun moveUp() {
        --top
        val topPos = stack[top]
        currentNode = topPos!!.node
        currentPos = topPos.pos
        node = ILeafNode.EMPTY
        stack[top] = null // help gc
    }

    fun getNextSibling(key: ByteIterable): Int {
        return currentNode.binarySearch(key, currentPos)
    }

    fun getNextSibling(key: ByteIterable, expectedAddress: Long): Int {
        return currentNode.binarySearch(key, currentPos, expectedAddress)
    }

    override fun compareCurrent(key: ByteIterable): Int {
        return currentNode.getKey(currentPos).compareKeyTo(key)
    }

    fun moveTo(index: Int) {
        currentPos = index
    }

    fun canMoveTo(index: Int): Boolean {
        return index < currentNode.size
    }

    override fun canMoveRight(): Boolean {
        return currentPos + 1 < currentNode.size
    }

    override fun moveRight(): INode {
        ++currentPos
        return if (currentNode.isBottom()) {
            handleLeafR(currentNode.getKey(currentPos)).also { node = it!! }!!
        } else {
            ILeafNode.EMPTY.also { node = it }
        }
    }

    protected open fun handleLeafR(leaf: BaseLeafNode): ILeafNode? {
        return leaf
    }

    protected open fun handleLeafL(leaf: BaseLeafNode): ILeafNode? {
        return leaf
    }

    override fun canMoveLeft(): Boolean {
        return currentPos > 0
    }

    override fun moveLeft(): INode {
        --currentPos
        return if (currentNode.isBottom()) {
            handleLeafL(currentNode.getKey(currentPos)).also { node = it!! }!!
        } else {
            ILeafNode.EMPTY.also { node = it }
        }
    }

    override fun getCurrentAddress(): Long = currentNode.getChildAddress(currentPos)

    override fun canMoveUp(): Boolean {
        return top != 0
    }

    override fun canMoveDown(): Boolean {
        return !currentNode.isBottom()
    }

    override fun reset(root: MutableTreeRoot) {
        top = 0
        node = ILeafNode.EMPTY
        currentNode = root as BasePage
        currentPos = 0
    }

    override fun moveTo(key: ByteIterable, value: ByteIterable?): Boolean {
        return doMoveTo(key, value, false)
    }

    override fun moveToRange(key: ByteIterable, value: ByteIterable?): Boolean {
        return doMoveTo(key, value, true)
    }

    private fun doMoveTo(key: ByteIterable, value: ByteIterable?, rangeSearch: Boolean): Boolean {
        var result: ILeafNode? = null
        if (top == 0 || currentNode.isInPageRange(key, value)) {
            result = currentNode.find(this, top, key, value, rangeSearch)
        } else {
            var rangeFound = false
            for (i in top - 1 downTo 1) {
                val node = stack[i]!!.node
                if (node.isInPageRange(key, value)) {
                    result = node.find(this, i, key, value, rangeSearch)
                    rangeFound = true
                    break
                }
            }
            if (!rangeFound) {
                result = stack[0]!!.node.find(this, 0, key, value, rangeSearch)
            }
        }
        if (result == null) {
            return false
        }
        node = if (result.isDupLeaf()) LeafNodeKV(result.getValue()!!, result.getKey()) else result
        return true
    }

    override fun getTree(): BTreeBase = (if (top == 0) currentNode else stack[0]!!.node).tree
    open fun isDup(): Boolean = false

    operator fun iterator(): PageIterator { // for testing purposes
        return object : PageIterator {
            private var index = 0
            private var pos = 0
            private var currentIteratorNode: BasePage? = null

            override fun getPos() = pos

            override fun hasNext(): Boolean {
                return index <= top // equality means we should return current
            }

            override fun next(): BasePage {
                val next: BasePage
                if (index < top) {
                    val treePos = stack[index]
                    next = treePos!!.node
                    pos = treePos.pos
                } else {
                    if (index > top) {
                        throw NoSuchElementException("No more pages in stack")
                    } else {
                        next = currentNode
                        pos = currentPos
                    }
                }
                currentIteratorNode = next
                index++
                return next
            }

            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    interface PageIterator : MutableIterator<BasePage?> {
        fun getPos(): Int
    }

    companion object {
        // for tests only
        fun isInDupMode(addressIterator: AddressIterator): Boolean {
            // hasNext() updates 'inDupTree'
            return addressIterator.hasNext() && (addressIterator.traverser as BTreeTraverserDup).inDupTree
        }

        // for tests only
        fun getTraverserNoDup(addressIterator: AddressIterator): BTreeTraverser {
            return BTreeTraverser(addressIterator.traverser as BTreeTraverser)
        }
    }
}
