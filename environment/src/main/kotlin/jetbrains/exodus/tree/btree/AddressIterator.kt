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

import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.LongIterator
import jetbrains.exodus.tree.TreeTraverser

/*
 *Iterator over tree addresses
 */
class AddressIterator(private var root: ITree?, private var alreadyIn: Boolean, @JvmField val traverser: TreeTraverser) :
    LongIterator {
    private var canGoDown = true
    override fun hasNext(): Boolean {
        return traverser.canMoveRight() || advance() || root != null
    }

    override fun next(): Long {
        if (alreadyIn) {
            alreadyIn = false
            return traverser.getCurrentAddress()
        }
        if (canGoDown) {
            if (traverser.canMoveDown()) {
                traverser.moveDown()
                return traverser.getCurrentAddress()
            }
        } else {
            canGoDown = true
        }
        if (traverser.canMoveRight()) {
            traverser.moveRight()
            val result = traverser.getCurrentAddress()
            if (traverser.canMoveDown()) {
                traverser.moveDown()
                alreadyIn = true
            }
            return result
        }
        if (traverser.canMoveUp()) {
            traverser.moveUp()
            canGoDown = false
            return traverser.getCurrentAddress()
        }

        val root = this.root
        if (root != null) {
            val result = root.getRootAddress()

            this.root = null
            return result
        }

        return Loggable.NULL_ADDRESS
    }

    private fun advance(): Boolean {
        while (traverser.canMoveUp()) {
            canGoDown = if (traverser.canMoveRight()) {
                return true
            } else {
                traverser.moveUp()
                false
            }
        }
        return traverser.canMoveRight()
    }
}
