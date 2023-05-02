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

import jetbrains.exodus.tree.INode

internal class PatriciaReclaimSourceTraverser(
    tree: PatriciaTreeBase,
    currentNode: NodeBase,
    private val minAddress: Long
) : PatriciaTraverser(tree, currentNode) {
    init {
        init(true)
    }

    override fun moveRight(): INode {
        var result: INode
        do {
            result = super.moveRight()
        } while (isValidPos && !isAddressReclaimable(currentChild!!.suffixAddress))
        return result
    }

    fun moveToNextReclaimable() {
        while (isValidPos && !isAddressReclaimable(currentChild!!.suffixAddress)) {
            super.moveRight()
        }
    }

    fun isAddressReclaimable(address: Long): Boolean {
        return minAddress <= address
    }
}
