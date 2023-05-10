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

    @JvmField
    var startAddress: Long = 0
    private var dataOffset: Byte = 0

    init {
        dataIterator = mainTree.getDataIterator(Loggable.NULL_ADDRESS)
        this.leafNodeDup = leafNodeDup
        leafNodeDupKey = leafNodeDup.getKey()
        val iterator = leafNodeDup.getRawValue(0).iterator()
        val l = iterator.getCompressedUnsignedLong()
        size = l shr 1
        if (l and 1L == 1L) {
            val offset = iterator.getCompressedUnsignedLong()
            startAddress = leafNodeDup.getAddress() - offset
            dataOffset = (getCompressedSize(l)
                    + getCompressedSize(offset)).toByte()
        } else {
            startAddress = Loggable.NULL_ADDRESS
            dataOffset = getCompressedSize(l).toByte()
        }
    }

    override fun getRootAddress(): Long {
        throw UnsupportedOperationException("BTreeDup has no root in 'Loggable' terms")
    }

    override fun getMutableCopy(): BTreeDupMutable = BTreeDupMutable(this, leafNodeDupKey)

    override fun openCursor(): TreeCursor {
        return TreeCursor(BTreeTraverser(this.getRoot()))
    }

    override fun getRoot(): BasePage = loadPage(
        leafNodeDup.getType(), leafNodeDup.getRawValue(dataOffset.toInt()),
        leafNodeDup.insideSinglePage
    )

    override fun loadLeaf(address: Long): LeafNode {
        val loggable = getLoggable(address)
        return if (loggable.getType() == DUP_LEAF) {
            object : LeafNode(log, loggable) {
                override fun isDupLeaf(): Boolean = true
                override fun getValue(): ByteIterable = leafNodeDupKey

                override fun toString(): String {
                    return "DLN {key:" + getKey() + "} @ " + this.getAddress()
                }
            }
        } else {
            throw IllegalArgumentException("Unexpected loggable type " + loggable.getType() + " at address " + loggable.getAddress())
        }
    }

    override fun isDupKey(address: Long): Boolean {
        return false
    }
}
