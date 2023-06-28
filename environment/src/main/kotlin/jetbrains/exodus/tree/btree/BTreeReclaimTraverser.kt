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

import jetbrains.exodus.log.RandomAccessLoggable

class BTreeReclaimTraverser(@JvmField val mainTree: BTreeMutable) : BTreeTraverser(mainTree.getRoot()) {
    @JvmField
    var wasReclaim = false

    @JvmField
    val dupLeafsLo: MutableList<RandomAccessLoggable?> = ArrayList()

    @JvmField
    val dupLeafsHi: MutableList<RandomAccessLoggable?> = ArrayList()
    fun setPage(node: BasePage) {
        currentNode = node
    }

    fun pushChild(index: Int) {
        node = pushChild(TreePos(currentNode, index), currentNode.getChild(index), 0)!!
    }

    fun popAndMutate() {
        val node = currentNode
        moveUp()
        if (node.isMutable()) {
            val nodeMutable = currentNode.getMutableCopy(mainTree)
            nodeMutable.setMutableChild(currentPos, node as BasePageMutable)
            currentNode = nodeMutable
        }
    }
}