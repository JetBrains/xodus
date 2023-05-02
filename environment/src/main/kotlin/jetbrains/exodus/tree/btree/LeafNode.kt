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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.ByteIterableWithAddress
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getCompressedSize
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.RandomAccessLoggable

/**
 * Stateless leaf node for immutable btree
 */
open class LeafNode(protected val log: Log, val loggable: RandomAccessLoggable) : BaseLeafNode() {
    private val keyLength: Int
    val insideSinglePage: Boolean

    init {
        val data = loggable.data
        val keyLength = data.compressedUnsignedInt
        val keyRecordSize = getCompressedSize(keyLength.toLong())
        this.keyLength = (keyLength shl 3) + keyRecordSize
        insideSinglePage = loggable.isDataInsideSinglePage
    }

    override val address: Long
        get() = loggable.address
    val type: Int
        get() = loggable.type.toInt()

    override fun compareKeyTo(iterable: ByteIterable): Int {
        return loggable.data.compareTo(
            keyRecordSize, getKeyLength(), iterable,
            0, iterable.length
        )
    }

    override fun compareValueTo(iterable: ByteIterable): Int {
        return loggable.data.compareTo(
            keyRecordSize + getKeyLength(),
            valueLength, iterable, 0, iterable.length
        )
    }

    override val key: ByteIterable
        get() = loggable.data.subIterable(keyRecordSize, getKeyLength())
    override val value: ByteIterable
        get() {
            val valueLength = valueLength
            return if (valueLength == 0) {
                ArrayByteIterable.EMPTY
            } else loggable.data.subIterable(keyRecordSize + getKeyLength(), valueLength)
        }
    override val isMutable: Boolean
        get() = false

    fun getLoggable(): Loggable {
        return loggable
    }

    fun getRawValue(offset: Int): ByteIterableWithAddress {
        val data = loggable.data
        return data.cloneWithOffset(keyRecordSize + getKeyLength() + offset)
    }

    private fun getKeyLength(): Int {
        return keyLength ushr 3
    }

    private val keyRecordSize: Int
        get() = keyLength and 7
    private val valueLength: Int
        get() = loggable.dataLength - keyRecordSize - getKeyLength()

    protected open fun doReclaim(context: BTreeReclaimTraverser, leafIndex: Int) {
        val keyAddress = context.currentNode.getKeyAddress(leafIndex)
        if (keyAddress == loggable.address) {
            val tree = context.mainTree
            tree.addExpiredLoggable(keyAddress)
            val node = context.currentNode.getMutableCopy(tree)
            node[leafIndex, tree.createMutableLeaf(key, value)] = null
            context.wasReclaim = true
            context.setPage(node)
        }
    }

    open fun reclaim(context: BTreeReclaimTraverser) {
        val keyIterable = key
        if (!context.canMoveDown() && context.canMoveRight()) {
            val leafIndex: Int
            val cmp = context.compareCurrent(keyIterable)
            if (cmp > 0) {
                return
            }
            leafIndex = if (cmp == 0) {
                context.currentPos
            } else {
                context.moveRight()
                context.getNextSibling(keyIterable, loggable.address)
            }
            if (leafIndex >= 0) {
                doReclaim(context, leafIndex)
                context.moveTo(leafIndex + 1)
                return
            } else if (context.canMoveTo(-leafIndex - 1)) {
                return
            }
        }
        // go up
        if (context.canMoveUp()) {
            while (true) {
                context.popAndMutate()
                context.moveRight()
                val index = context.getNextSibling(keyIterable)
                if (index < 0) {
                    if (context.canMoveTo(-index - 1) || !context.canMoveUp()) {
                        context.moveTo(Math.max(-index - 2, 0))
                        break
                    }
                } else {
                    context.pushChild(index) // node is always internal
                    break
                }
            }
        }
        // go down
        while (context.canMoveDown()) {
            var index = context.getNextSibling(keyIterable)
            if (index < 0) {
                index = Math.max(-index - 2, 0)
            }
            context.pushChild(index)
        }
        val leafIndex = context.getNextSibling(keyIterable)
        if (leafIndex >= 0) {
            doReclaim(context, leafIndex)
            context.moveTo(leafIndex + 1)
        } else {
            context.moveTo(-leafIndex - 1)
        }
    }
}
