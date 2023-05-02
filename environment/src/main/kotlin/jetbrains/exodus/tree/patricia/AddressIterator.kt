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

import jetbrains.exodus.tree.LongIterator
import jetbrains.exodus.tree.TreeTraverser

class AddressIterator(private val traverser: TreeTraverser) : LongIterator {
    private var finished = false

    /*
     * Used for trees with returning rootAddress
     */
    init {
        traverser.init(true)
    }

    override fun hasNext(): Boolean {
        return !finished
    }

    override fun next(): Long {
        val result = traverser.currentAddress
        if (traverser.canMoveDown()) {
            traverser.moveDown()
            return result
        }
        if (traverser.canMoveRight()) {
            traverser.moveRight()
            traverser.moveDown()
            return result
        }
        while (true) {
            if (traverser.canMoveUp()) {
                if (traverser.canMoveRight()) {
                    traverser.moveRight()
                    traverser.moveDown()
                    return result
                } else {
                    traverser.moveUp()
                }
            } else if (traverser.canMoveRight()) {
                traverser.moveRight()
                traverser.moveDown()
                return result
            } else {
                finished = true
                break
            }
        }
        return result
    }

}
