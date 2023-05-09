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

internal class PatriciaReclaimActualTraverser(@JvmField val mainTree: PatriciaTreeMutable) :
    PatriciaTraverser(mainTree, mainTree.root) {
    @JvmField
    var wasReclaim = false

    init {
        init(true)
    }

    fun popAndMutate() {
        --top
        val topItr = stack[top]!!
        val parentNode = topItr.getParentNode()
        if (currentNode.isMutable()) {
            val pos = topItr.getIndex()
            val parentNodeMutable = parentNode!!.getMutableCopy(mainTree)
            parentNodeMutable.setChild(pos, (currentNode as MutableNode))
            updateCurrentNode(parentNodeMutable)
            currentChild = parentNodeMutable.getRef(pos)
            // mutate iterator to boost performance
            currentIterator = if (parentNode.isMutable()) topItr else parentNodeMutable.getChildren(pos)
            // currentIterator = topItr;
        } else {
            updateCurrentNode(parentNode!!)
            currentIterator = topItr
            currentChild = topItr.getNode()
        }
        stack[top] = null // help gc
    }
}
