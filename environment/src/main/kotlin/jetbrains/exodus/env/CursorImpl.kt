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
package jetbrains.exodus.env

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.tree.ITreeCursor

internal class CursorImpl(private val store: StoreImpl, private val txn: TransactionBase) : Cursor {
    private val snapshotId: Long = txn.snapshotId
    private var treeCursor: ITreeCursor? = null
    private var isClosed = false

    override fun getNext(): Boolean {
        checkTreeCursor()
        return treeCursor!!.next
    }

    override fun getNextDup(): Boolean {
        checkTreeCursor()
        return treeCursor!!.nextDup
    }

    override fun getNextNoDup(): Boolean {
        checkTreeCursor()
        return treeCursor!!.nextNoDup
    }

    override fun getPrev(): Boolean {
        checkTreeCursor()
        return treeCursor!!.prev
    }

    override fun getPrevDup(): Boolean {
        checkTreeCursor()
        return treeCursor!!.prevDup
    }

    override fun getPrevNoDup(): Boolean {
        checkTreeCursor()
        return treeCursor!!.prevNoDup
    }

    override fun getLast(): Boolean {
        checkTreeCursor()
        return treeCursor!!.last
    }

    override fun getKey(): ByteIterable {
        checkTreeCursor()
        return treeCursor!!.key
    }

    override fun getValue(): ByteIterable {
        checkTreeCursor()
        return treeCursor!!.value
    }

    override fun getSearchKey(key: ByteIterable): ByteIterable? {
        checkTreeCursor()
        return treeCursor!!.getSearchKey(key)
    }

    override fun getSearchKeyRange(key: ByteIterable): ByteIterable? {
        checkTreeCursor()
        return treeCursor!!.getSearchKeyRange(key)
    }

    override fun getSearchBoth(key: ByteIterable, value: ByteIterable): Boolean {
        checkTreeCursor()
        return treeCursor!!.getSearchBoth(key, value)
    }

    override fun getSearchBothRange(key: ByteIterable, value: ByteIterable): ByteIterable? {
        checkTreeCursor()
        return treeCursor!!.getSearchBothRange(key, value)
    }

    override fun count(): Int {
        checkTreeCursor()
        return treeCursor!!.count()
    }

    override fun isMutable(): Boolean {
        checkTreeCursor()
        return treeCursor!!.isMutable
    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            if (treeCursor != null) {
                treeCursor!!.close()
            }
        }
    }

    override fun deleteCurrent(): Boolean {
        val txn: ReadWriteTransaction = EnvironmentImpl.throwIfReadonly(
            txn,
            "Can't delete a key/value pair of cursor in read-only transaction"
        )
        if (treeCursor == null) {
            treeCursor = txn.getMutableTree(store).openCursor()
        } else {
            if (!treeCursor!!.isMutable) {
                val key = treeCursor!!.key
                val value = treeCursor!!.value
                val newCursor = txn.getMutableTree(store).openCursor()
                treeCursor = if (newCursor.getSearchBoth(key, value)) {
                    newCursor // navigated to same pair, ready to delete
                } else {
                    throw ConcurrentModificationException(CANT_DELETE_MODIFIED_MSG)
                }
            }
        }
        return treeCursor!!.deleteCurrent()
    }

    private fun checkTreeCursor() {
        if (treeCursor == null) {
            treeCursor = txn.getTree(store).openCursor()
        }
        if (snapshotId != txn.snapshotId) {
            throw ExodusException("Cursor holds an obsolete database snapshot. Check if txn.flush() or txn.commit() is called.")
        }
    }

    companion object {
        private const val CANT_DELETE_MODIFIED_MSG = "Can't delete (pair not found in mutable tree)"
    }
}
