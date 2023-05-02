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

import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.MutableTreeRoot
import jetbrains.exodus.tree.TreeCursorMutable

class BTreeCursorDupMutable(
    tree: ITreeMutable?, // hack to avoid casts
    override val traverser: BTreeTraverserDup
) : TreeCursorMutable(tree!!, traverser) {
    override fun getNextDup(): Boolean {
        moveIfNecessary()
        // move to next dup if in -1 position or dupCursor has next element
        return if (traverser.node === ILeafNode.EMPTY) {
            if (wasDelete) {
                val root = traverser.currentNode // traverser was reset to root after delete
                val key = nextAfterRemovedKey!!
                val value = nextAfterRemovedValue!!
                if (next) {
                    if (traverser.inDupTree) {
                        return true
                    }
                    // not really a dup, rollback
                    reset(root as MutableTreeRoot) // also restores state
                    wasDelete = true
                    nextAfterRemovedKey = key
                    nextAfterRemovedValue = value
                }
                return false
            }
            next
        } else {
            hasNext() && traverser.inDupTree && next && traverser.inDupTree
        }
    }

    override fun getNextNoDup(): Boolean {
        moveIfNecessary()
        if (wasDelete) {
            if (next) {
                /* we managed to re-navigate to key which is next
                   after removed, check if it is a duplicate */
                if (!traverser.inDupTree) {
                    return true
                }
            } else {
                return false
            }
        }
        if (traverser.inDupTree) {
            traverser.popUntilDupRight()
            canGoDown = false
        }
        return next
    }

    override fun getPrevDup(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun getPrevNoDup(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun count(): Int {
        moveIfNecessary()
        return if (traverser.inDupTree) traverser.currentNode.tree.size.toInt() else super.count()
    }
}
