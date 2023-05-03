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

open class BTreeTraverserDup(currentNode: BasePage) : BTreeTraverser(currentNode) {
    var inDupTree = false
    override fun canMoveDown(): Boolean {
        val currentPos = currentPos
        val currentNode = currentNode
        return currentPos < currentNode.size && (!currentNode.isBottom || currentPos >= 0 && currentNode.isDupKey(
            currentPos
        ))
    }

    override val childForMoveDown: BasePage?
        get() = if (currentNode.isBottom) {
            val leaf = currentNode.getKey(currentPos)
            inDupTree = true
            leaf.tree.root
        } else {
            super.childForMoveDown
        }

    override fun moveUp() {
        super.moveUp()
        if (currentNode.isBottom) {
            // we moved up and we are at bottom: this means we were in a dup tree
            inDupTree = false
        }
    }

    override fun handleLeaf(leaf: BaseLeafNode): ILeafNode? {
        return if (leaf.isDupLeaf) {
            LeafNodeKV(leaf.value!!, leaf.key)
        } else {
            super.handleLeaf(leaf)
        }
    }

    override fun handleLeafR(leaf: BaseLeafNode): ILeafNode? {
        return if (leaf.isDupLeaf) {
            LeafNodeKV(leaf.value!!, leaf.key)
        } else if (leaf.isDup) {
            inDupTree = true
            pushChild(TreePos(currentNode, currentPos), leaf.tree.root, 0)
        } else {
            super.handleLeaf(leaf)
        }
    }

    override fun handleLeafL(leaf: BaseLeafNode): ILeafNode? {
        return if (leaf.isDupLeaf) {
            LeafNodeKV(leaf.value!!, leaf.key)
        } else if (leaf.isDup) {
            inDupTree = true
            val root = leaf.tree.root
            pushChild(TreePos(currentNode, currentPos), root, root.size - 1)
        } else {
            super.handleLeaf(leaf)
        }
    }

    fun popUntilDupRight() {
        var bottom = top
        while (true) {
            --bottom
            val current = stack[bottom]!!
            stack[bottom] = null // gc
            if (current.node.isBottom) {
                currentNode = current.node
                currentPos = current.pos
                top = bottom
                inDupTree = false
                return
            }
        }
    }

    fun popUntilDupLeft() {
        /*if (false) {
            final int bottom = 0;
            final TreePos current = stack[bottom];
            currentNode = current.node;
            currentPos = current.pos;
            top = bottom;
            while (top > bottom) {
                --top;
                stack[top] = null;
            }
        }*/
    }

    override val isDup: Boolean
        get() = true
}
