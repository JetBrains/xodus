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

import jetbrains.exodus.*
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.NullLoggable.isNullLoggable
import jetbrains.exodus.log.TooBigLoggableException
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.tree.ITreeCursorMutable
import jetbrains.exodus.tree.TreeCursor
import jetbrains.exodus.util.LightOutputStream

internal class BTreeDupMutable(dupTree: BTreeBase, key: ByteIterable) : BTreeMutable(dupTree, null) {
    @JvmField
    var mainTree: BTreeMutable? = null

    @JvmField
    var key: ByteIterable

    @JvmField
    var address = Loggable.NULL_ADDRESS

    init {
        size = dupTree.size
        this.key = key
    }

    override fun addExpiredLoggable(loggable: Loggable) {
        mainTree!!.addExpiredLoggable(loggable)
    }

    override fun addExpiredLoggable(address: Long) {
        mainTree!!.addExpiredLoggable(address)
    }

    override fun decrementSize(delta: Long) {
        super.decrementSize(delta)
        mainTree!!.decrementSize(delta)
    }

    override fun incrementSize() {
        super.incrementSize()
        mainTree!!.incrementSize()
    }

    override fun save(): Long {
        check(address == Loggable.NULL_ADDRESS) { "Duplicates sub-tree already saved" }
        val rootPage: BasePageMutable = getRoot()
        val type: Byte =
            if (rootPage.isBottom()) LEAF_DUP_BOTTOM_ROOT else LEAF_DUP_INTERNAL_ROOT
        val keyIterable = getIterable(key.length.toLong())
        var sizeIterable: ByteIterable
        var startAddress = log.writtenHighAddress // remember high address before saving the data
        val rootDataIterable = rootPage.getData()
        var iterables: Array<ByteIterable?>
        var result: Long
        val canRetry: Boolean
        val expired = getExpiredLoggables()
        if (log.isLastWrittenFileAddress(startAddress)) {
            sizeIterable = getIterable(size shl 1)
            iterables = arrayOf(keyIterable, key, sizeIterable, rootDataIterable)
            result = log.tryWrite(type, structureId, CompoundByteIterable(iterables), expired)
            if (result >= 0) {
                address = result
                return result
            } else {
                canRetry = false
            }
        } else {
            canRetry = true
        }
        if (!log.isLastWrittenFileAddress(startAddress)) {
            val writtenType = log.getWrittenLoggableType(startAddress, DUP_LEAF)
            if (isNullLoggable(writtenType)) {
                val lengthBound = log.fileLengthBound
                val alignment = startAddress % lengthBound
                startAddress += lengthBound - alignment
                check(log.writtenHighAddress >= startAddress) { "Address alignment underflow: start address $startAddress, alignment $alignment" }
            }
        }
        sizeIterable = getIterable((size shl 1) + 1)
        val offsetIterable = getIterable(log.writtenHighAddress - startAddress)
        iterables = arrayOf(keyIterable, key, sizeIterable, offsetIterable, rootDataIterable)
        val data: ByteIterable = CompoundByteIterable(iterables)
        result = if (canRetry) log.tryWrite(type, structureId, data, expired) else log.writeContinuously(
            type,
            structureId,
            data,
            expired
        )
        if (result < 0) {
            if (canRetry) {
                iterables[3] = getIterable(log.writtenHighAddress - startAddress)
                result = log.writeContinuously(type, structureId, CompoundByteIterable(iterables), expired)
                if (result < 0) {
                    throw TooBigLoggableException()
                }
            } else {
                throw TooBigLoggableException()
            }
        }
        address = result
        return result
    }

    override fun getLeafStream(): LightOutputStream? = mainTree!!.getLeafStream()
    override fun getExpiredLoggables(): ExpiredLoggableCollection = mainTree!!.getExpiredLoggables()
    override fun getOpenCursors(): Iterable<ITreeCursorMutable>? = throwCantOpenCursor<Iterable<ITreeCursorMutable>>()

    override fun openCursor(): TreeCursor {
        return throwCantOpenCursor<TreeCursor>()!!
    }

    override fun cursorClosed(cursor: ITreeCursorMutable) {
        throwCantOpenCursor<Any>()
    }

    override fun getBottomPageType(): Byte = DUP_BOTTOM
    override fun getInternalPageType(): Byte = DUP_INTERNAL
    override fun getLeafType(): Byte = DUP_LEAF
    override fun isDup(): Boolean = true

    override fun loadLeaf(address: Long): LeafNode {
        val loggable = getLoggable(address)
        return if (loggable.getType() == DUP_LEAF) {
            object : LeafNode(log, loggable) {
                override fun getValue(): ByteIterable = this@BTreeDupMutable.key
                override fun isDupLeaf(): Boolean = true
                override fun toString(): String {
                    return "DLN {key:" + key + "} @ " + this.getAddress()
                }
            }
        } else {
            throw IllegalArgumentException(
                "Unexpected loggable type " + loggable.getType() +
                        " at address " + loggable.getAddress()
            )
        }
    }

    override fun isDupKey(address: Long): Boolean {
        return false
    }

    override fun createMutableLeaf(key: ByteIterable, value: ByteIterable): BaseLeafNodeMutable {
        return DupLeafNodeMutable(key, this)
    }

    companion object {
        private fun <T> throwCantOpenCursor(): T? {
            throw ExodusException("Can't open cursor on BTreeDupMutable")
        }
    }
}
