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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterableBase
import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.MutableTreeRoot
import jetbrains.exodus.tree.TreeTraverser
import jetbrains.exodus.util.LightOutputStream

internal open class PatriciaTraverser(private val tree: PatriciaTreeBase, currentNode: NodeBase) : TreeTraverser {
    @JvmField
    var stack: Array<NodeChildrenIterator?>

    @JvmField
    var top: Int

    private var currentValue: ByteIterable? = null

    @JvmField
    var currentChild: ChildReference? = null

    @JvmField
    var currentIterator: NodeChildrenIterator? = null

    @JvmField
    var currentNode: NodeBase
    override fun getTree(): ITree? = tree

    init {
        this.currentNode = currentNode
        currentValue = currentNode.getValue()

        stack = arrayOfNulls(INITIAL_STACK_CAPACITY)
        top = 0
    }

    fun updateCurrentNode(currentNode: NodeBase) {
        this.currentNode = currentNode
        currentValue = currentNode.getValue()
    }

    override fun init(left: Boolean) {
        if (left) {
            intiItr()
        } else {
            currentIterator = currentNode.getChildrenLast()
            currentChild = currentIterator!!.getNode()
        }
    }

    override fun isNotEmpty(): Boolean = currentNode.getChildrenCount() > 0

    override fun moveDown(): INode {
        stack = pushIterator(stack, currentIterator, top)
        updateCurrentNode(currentChild!!.getNode(tree))
        intiItr()
        ++top
        return currentNode
    }

    override fun moveDownToLast(): INode {
        stack = pushIterator(stack, currentIterator, top)
        updateCurrentNode(currentChild!!.getNode(tree))

        if (currentNode.getChildrenCount() > 0) {
            val itr = currentNode.getChildrenLast()
            currentIterator = itr
            currentChild = itr.getNode()
        } else {
            currentIterator = null
            currentChild = null
        }
        ++top
        return currentNode
    }

    override fun getKey(): ByteIterable {
        if (top == 0) {
            return if (currentNode.hasValue()) currentNode.getKey() else ByteIterable.EMPTY
        }
        val output = LightOutputStream(7)
        for (i in 0 until top) {
            ByteIterableBase.fillBytes(stack[i]!!.getKey()!!, output)
            output.write(stack[i]!!.getNode()!!.firstByte.toInt()) // seems that firstByte isn't mutated
        }
        ByteIterableBase.fillBytes(currentNode.getKey(), output)
        return output.asArrayByteIterable()
    }

    override fun getValue(): ByteIterable = currentValue ?: ByteIterable.EMPTY

    override fun hasValue(): Boolean {
        return currentValue != null
    }

    override fun moveUp() {
        --top
        val topItr = stack[top]
        updateCurrentNode(topItr!!.getParentNode()!!)
        currentIterator = topItr
        currentChild = topItr.getNode()
        stack[top] = null // help gc
    }

    override fun compareCurrent(key: ByteIterable): Int {
        throw UnsupportedOperationException()
    }

    override fun canMoveRight(): Boolean {
        return currentIterator != null && currentIterator!!.hasNext()
    }

    fun isValidPos(): Boolean = currentIterator != null

    override fun moveRight(): INode {
        if (currentIterator!!.hasNext()) {
            if (currentIterator!!.isMutable()) {
                currentChild = currentIterator!!.next()
            } else {
                currentIterator!!.nextInPlace()
            }
        } else {
            currentIterator = null
            currentChild = null
        }
        return currentNode
    }

    override fun canMoveLeft(): Boolean {
        return if (currentIterator == null) {
            currentNode.getChildrenCount() > 0
        } else currentIterator!!.hasPrev()
    }

    override fun moveLeft(): INode {
        return if (currentIterator!!.hasPrev()) {
            if (currentIterator!!.isMutable() || currentChild == null) {
                currentChild = currentIterator!!.prev()
            } else {
                currentIterator!!.prevInPlace()
            }
            currentNode
        } else {
            throw IllegalStateException()
        }
    }

    override fun getCurrentAddress(): Long = currentNode.getAddress()

    override fun canMoveUp(): Boolean {
        return top != 0
    }

    override fun canMoveDown(): Boolean {
        return currentChild != null
    }

    override fun reset(root: MutableTreeRoot) {
        top = 0
        updateCurrentNode(root as NodeBase)
        intiItr()
    }

    override fun moveTo(key: ByteIterable, value: ByteIterable?): Boolean {
        val it = key.iterator()
        var node =
            if (top == 0) currentNode else stack[0]!!.getParentNode()!! // the most bottom node, ignoring lower bound
        var depth = 0
        var tmp = arrayOfNulls<NodeChildrenIterator>(INITIAL_STACK_CAPACITY)
        // go down and search
        while (true) {
            if (NodeBase.MatchResult.getMatchingLength(node.matchesKeySequence(it)) < 0) {
                return false
            }
            if (!it.hasNext()) {
                break
            }
            val itr = node.getChildren(it.next())
            val ref = itr.getNode() ?: return false
            tmp = pushIterator(tmp, itr, depth++)
            node = ref.getNode(tree)
        }
        // key match
        if (node.hasValue() && (value == null || value.compareTo(node.getValue()) == 0)) {
            updateCurrentNode(node)
            intiItr()
            stack = tmp
            top = depth
            return true
        }
        return false
    }

    override fun moveToRange(key: ByteIterable, value: ByteIterable?): Boolean {
        val it = key.iterator()
        var node =
            if (top == 0) currentNode else stack[0]!!.getParentNode()!! // the most bottom node, ignoring lower bound
        var depth = 0
        var tmp = arrayOfNulls<NodeChildrenIterator>(INITIAL_STACK_CAPACITY)
        // go down and search
        val dive: Boolean
        var smaller = false
        while (true) {
            val hasNext = it.hasNext()
            val matchResult = node.matchesKeySequence(it)
            if (NodeBase.MatchResult.getMatchingLength(matchResult) < 0) {
                if (value == null) {
                    smaller = NodeBase.MatchResult.hasNext(matchResult) && (!hasNext ||
                            NodeBase.MatchResult.getKeyByte(matchResult) < NodeBase.MatchResult.getNextByte(matchResult))
                    dive = !smaller
                    break
                }
                return false
            }
            if (!it.hasNext()) {
                // key match
                if (!node.hasValue()) {
                    dive = true
                    break
                }
                if (value == null || value <= node.getValue()) {
                    updateCurrentNode(node)
                    intiItr()
                    stack = tmp
                    top = depth
                    return true
                }
                return false
            }
            val nextByte = it.next()
            var itr = node.getChildren(nextByte)
            var ref = itr.getNode()
            if (ref == null) {
                itr = node.getChildrenRange(nextByte)
                ref = itr.getNode()
                if (ref != null) {
                    tmp = pushIterator(tmp, itr, depth++)
                    node = ref.getNode(tree)
                    dive = true
                    break
                }
                smaller = true
                dive = false
                break
            }
            tmp = pushIterator(tmp, itr, depth++)
            node = ref.getNode(tree)
        }
        if (smaller || !node.hasValue()) {
            if (dive && node.getChildrenCount() > 0) {
                val itr = node.getChildren().iterator()
                tmp = pushIterator(tmp, itr, depth)
                node = itr.next().getNode(tree)
            } else {
                // go up and try range search
                var itr: NodeChildrenIterator?
                do {
                    itr = if (depth > 0) {
                        tmp[--depth]
                    } else {
                        return false // search already gave us the max
                    }
                } while (!itr!!.hasNext())
                node = itr.next().getNode(tree)
                // trick: tmp[depth] was already in stack
            }
            ++depth
            while (!node.hasValue()) {
                val itr = node.getChildren().iterator()
                check(itr.hasNext()) { "Can't dive into tree branch" }
                val ref = itr.next()
                tmp = pushIterator(tmp, itr, depth++)
                node = ref.getNode(tree)
            }
        }
        updateCurrentNode(node)
        intiItr()
        stack = tmp
        top = depth
        return true
    }

    fun intiItr() {
        if (currentNode.getChildrenCount() > 0) {
            val itr = currentNode.getChildren().iterator()
            currentIterator = itr
            currentChild = itr.next()
        } else {
            currentIterator = null
            currentChild = null
        }
    }

    companion object {
        private const val INITIAL_STACK_CAPACITY = 8
        private fun pushIterator(
            tmp: Array<NodeChildrenIterator?>,
            itr: NodeChildrenIterator?,
            depth: Int
        ): Array<NodeChildrenIterator?> {
            var input = tmp
            val length = input.size
            if (depth >= length) { // ensure capacity
                val newCapacity = length shl 1
                val newStack = arrayOfNulls<NodeChildrenIterator>(newCapacity)
                System.arraycopy(input, 0, newStack, 0, length)
                input = newStack
            }
            input[depth] = itr
            return input
        }
    }
}
