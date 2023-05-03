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
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getCompressedSize
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.TreeCursor

internal class BTreeDup(mainTree: BTreeBase, leafNodeDup: LeafNodeDup) :
    BTreeBase(mainTree.log, mainTree.balancePolicy, false, mainTree.structureId) {
    private val leafNodeDup: LeafNodeDup

    // get key from tree
    private val leafNodeDupKey: ByteIterable
    var startAddress: Long = 0
    private var dataOffset: Byte = 0

    init {
        dataIterator = mainTree.getDataIterator(Loggable.NULL_ADDRESS)
        this.leafNodeDup = leafNodeDup
        leafNodeDupKey = leafNodeDup.key
        val iterator = leafNodeDup.getRawValue(0).iterator()
        val l = iterator.compressedUnsignedLong
        size = l shr 1
        if (l and 1L == 1L) {
            val offset = iterator.compressedUnsignedLong
            startAddress = leafNodeDup.address - offset
            dataOffset = (getCompressedSize(l)
                    + getCompressedSize(offset)).toByte()
        } else {
            startAddress = Loggable.NULL_ADDRESS
            dataOffset = getCompressedSize(l).toByte()
        }
    }

    override val rootAddress: Long
        get() {
            throw UnsupportedOperationException("BTreeDup has no root in 'Loggable' terms")
        }
    override val mutableCopy: BTreeDupMutable
        get() = BTreeDupMutable(this, leafNodeDupKey)

    override fun openCursor(): TreeCursor {
        return TreeCursor(BTreeTraverser(this.root))
    }

    override val root: BasePage
        get() = loadPage(
            leafNodeDup.type, leafNodeDup.getRawValue(dataOffset.toInt()),
            leafNodeDup.insideSinglePage
        )

    override fun loadLeaf(address: Long): LeafNode {
        val loggable = getLoggable(address)
        return if (loggable.type == DUP_LEAF) {
            object : LeafNode(log, loggable) {
                override val isDupLeaf: Boolean
                    get() = true

                override val value: ByteIterable
                    get() = leafNodeDupKey

                override fun toString(): String {
                    return "DLN {key:" + key + "} @ " + this.address
                }
            }
        } else {
            throw IllegalArgumentException("Unexpected loggable type " + loggable.type + " at address " + loggable.address)
        }
    }

    override fun isDupKey(address: Long): Boolean {
        return false
    }
}
