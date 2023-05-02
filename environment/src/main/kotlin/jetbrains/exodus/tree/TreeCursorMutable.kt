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
import jetbrains.exodus.ExodusException

/**
 * Cursor with deleteCurrent support.
 * May be used for trees with duplicates or without ones.
 */
open class TreeCursorMutable : TreeCursor, ITreeCursorMutable {
    final override val tree: ITreeMutable
    @JvmField
    protected var wasDelete = false
    @JvmField
    protected var nextAfterRemovedKey: ByteIterable? = null
    @JvmField
    protected var nextAfterRemovedValue: ByteIterable? = null
    private var moveToKey: ByteIterable? = null
    private var moveToValue: ByteIterable? = null

    constructor(tree: ITreeMutable, traverser: TreeTraverser) : super(traverser) {
        this.tree = tree
    }

    constructor(tree: ITreeMutable, traverser: TreeTraverser, alreadyIn: Boolean) : super(traverser, alreadyIn) {
        this.tree = tree
    }

    override fun getNext(): Boolean {
        moveIfNecessary()
        return if (wasDelete) {
            wasDelete = false
            // move to remembered next
            val key = nextAfterRemovedKey
            if (key != null) {
                if (traverser.moveTo(key, if (tree.isAllowingDuplicates) nextAfterRemovedValue else null)) {
                    inited = true
                    return true
                }
                false
            } else {
                false
            }
        } else {
            super.getNext()
        }
    }

    protected fun reset(newRoot: MutableTreeRoot?) {
        traverser.reset(newRoot!!)
        canGoDown = true
        alreadyIn = false
        inited = false
    }

    override fun hasNext(): Boolean {
        moveIfNecessary()
        return if (wasDelete) nextAfterRemovedKey != null else super.hasNext()
    }

    override fun getPrev(): Boolean {
        moveIfNecessary()
        return if (wasDelete) {
            throw UnsupportedOperationException()
        } else {
            super.getPrev()
        }
    }

    override fun hasPrev(): Boolean {
        moveIfNecessary()
        return if (wasDelete) {
            throw UnsupportedOperationException()
        } else {
            super.hasPrev()
        }
    }

    override fun getKey(): ByteIterable {
        moveIfNecessary()
        return super.getKey()
    }

    override fun getValue(): ByteIterable {
        moveIfNecessary()
        return super.getValue()
    }

    override fun deleteCurrent(): Boolean {
        moveIfNecessary()
        if (wasDelete) {
            return false
        }

        // delete and remember next
        val key = key
        val value = value
        if (next) {
            nextAfterRemovedKey = traverser.key
            nextAfterRemovedValue = traverser.value
        } else {
            nextAfterRemovedKey = null
            nextAfterRemovedValue = null
        }

        // don't call back treeChanged() for current cursor
        if (tree.isAllowingDuplicates) {
            tree.delete(key, value, this)
        } else {
            tree.delete(key, null, this)
        }
        wasDelete = true

        // root may be changed by tree.delete, so reset cursor with new root
        reset(tree.root)
        return true
    }

    override fun isMutable(): Boolean {
        return true
    }

    override fun treeChanged() {
        if (moveToKey == null) {
            val key = key
            val value = value
            moveToKey = key
            moveToValue = value
        }
    }

    override fun close() {
        tree.cursorClosed(this)
    }

    override fun moveTo(key: ByteIterable, value: ByteIterable?, rangeSearch: Boolean): ByteIterable? {
        val result = super.moveTo(key, value, rangeSearch)
        if (result != null) {
            wasDelete = false
        }
        return result
    }

    protected fun moveIfNecessary() {
        if (moveToKey != null) {
            if (moveToValue == null) {
                throw ExodusException("Can't move Cursor to null value")
            }
            moveToPair(moveToKey!!, moveToValue!!)
            moveToValue = null
            moveToKey = null
        }
    }

    private fun moveToPair(key: ByteIterable, value: ByteIterable) {
        reset(tree.root)
        // move to current
        val withDuplicates = tree.isAllowingDuplicates
        if (!traverser.moveTo(key, if (withDuplicates) value else null)) {
            wasDelete = true
            // null means current was removed
            // try to move to next key/value
            if (withDuplicates) {
                if (!(traverser.moveToRange(key, value) || traverser.moveToRange(key, null))) {
                    // null means key/value was removed, move to next key
                    return
                }
            } else if (!traverser.moveToRange(key, null)) {
                return
            }
            nextAfterRemovedKey = traverser.key
            nextAfterRemovedValue = traverser.value
        } else {
            inited = true
        }
    }

    companion object {
        @JvmStatic
        fun notifyCursors(tree: ITreeMutable) {
            val openCursors = tree.openCursors
            if (openCursors != null) {
                for (cursor in openCursors) {
                    cursor.treeChanged()
                }
            }
        }

        @JvmStatic
        fun notifyCursors(tree: ITreeMutable, cursorToSkip: ITreeCursorMutable?) {
            val openCursors = tree.openCursors
            if (openCursors != null) {
                for(cursor in openCursors) {
                    if (cursor !== cursorToSkip) {
                        cursor.treeChanged()
                    }
                }
            }
        }
    }
}
