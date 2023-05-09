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
package jetbrains.exodus.tree

import jetbrains.exodus.ByteIterable

/**
 *
 */
open class TreeCursor @JvmOverloads constructor(
    @JvmField protected val traverser: TreeTraverser,
    @JvmField var alreadyIn: Boolean = false
) :
    ITreeCursor {
    @JvmField
    protected var canGoDown = true

    @JvmField
    var inited = false
    protected open operator fun hasNext(): Boolean {
        return if (inited) {
            traverser.canMoveRight() || advance()
        } else traverser.isNotEmpty()
    }


    protected open fun hasPrev(): Boolean {
        return if (inited) {
            traverser.canMoveLeft() || retreat()
        } else traverser.isNotEmpty()
    }

    override fun getNext(): Boolean {
        if (!inited) {
            traverser.init(true)
            inited = true
        }
        if (alreadyIn) {
            alreadyIn = false
            return true
        }
        val result = moveToNext()
        if (!result) {
            traverser.init(false)
            canGoDown = true
            moveToPrev()
        }
        return result
    }

    override fun getPrev(): Boolean {
        if (!inited) {
            traverser.init(false)
            inited = true
        }
        if (alreadyIn) {
            alreadyIn = false
            return true
        }
        val result = moveToPrev()
        if (!result) {
            traverser.init(true)
            canGoDown = true
            moveToNext()
        }
        return result
    }

    override fun getLast(): Boolean {
        // move up to root
        while (traverser.canMoveUp()) {
            traverser.moveUp()
        }
        traverser.init(false)
        inited = true
        alreadyIn = !traverser.isNotEmpty() && traverser.hasValue()
        return prev
    }

    override fun getKey(): ByteIterable {
        return traverser.getKey()
    }

    override fun getValue(): ByteIterable {
        return traverser.getValue()
    }

    override fun getNextDup(): Boolean {
        // tree without duplicates can has next dup only in -1 position
        return traverser.getKey() === ByteIterable.EMPTY && next
    }

    override fun getNextNoDup(): Boolean {
        return next
    }

    override fun getPrevDup(): Boolean {
        return prev
    }

    override fun getPrevNoDup(): Boolean {
        return prev
    }

    override fun getSearchKey(key: ByteIterable): ByteIterable? {
        return moveTo(key, null, false)
    }

    override fun getSearchKeyRange(key: ByteIterable): ByteIterable? {
        return moveTo(key, null, true)
    }

    override fun getSearchBoth(key: ByteIterable, value: ByteIterable): Boolean {
        return moveTo(key, value, false) != null
    }

    override fun getSearchBothRange(key: ByteIterable, value: ByteIterable): ByteIterable? {
        return moveTo(key, value, true)
    }

    override fun count(): Int {
        return 1
    }

    override fun close() {}
    override fun deleteCurrent(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun isMutable(): Boolean {
        return false
    }

    override fun getTree(): ITree? = traverser.getTree()

    protected open fun moveTo(key: ByteIterable, value: ByteIterable?, rangeSearch: Boolean): ByteIterable? {
        if (if (rangeSearch) traverser.moveToRange(key, value) else traverser.moveTo(key, value)) {
            canGoDown = true
            alreadyIn = false
            inited = true
            return traverser.getValue()
        }
        return null
    }

    private fun moveToNext(): Boolean {
        while (true) {
            if (canGoDown) {
                if (traverser.canMoveDown()) {
                    if (traverser.moveDown().hasValue()) {
                        return true
                    }
                    continue
                }
            } else {
                canGoDown = true
            }
            if (traverser.canMoveRight()) {
                val node = traverser.moveRight()
                if (!traverser.canMoveDown() && node.hasValue()) {
                    return true
                }
            } else if (!advance()) {
                break
            }
        }
        return false
    }

    private fun moveToPrev(): Boolean {
        while (true) {
            if (canGoDown) {
                if (traverser.canMoveDown()) {
                    if (traverser.moveDownToLast().hasValue()) {
                        return true
                    }
                    continue
                }
            } else {
                canGoDown = true
            }
            if (traverser.canMoveLeft()) {
                val node = traverser.moveLeft()
                if (!traverser.canMoveDown() && node.hasValue()) {
                    return true
                }
            } else if (!retreat()) {
                break
            }
        }
        return false
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

    private fun retreat(): Boolean {
        while (traverser.canMoveUp()) {
            canGoDown = if (traverser.canMoveLeft()) {
                return true
            } else {
                traverser.moveUp()
                false
            }
        }
        return traverser.canMoveLeft()
    }
}
