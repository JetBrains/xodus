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
import jetbrains.exodus.log.*
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getCompressedSize
import jetbrains.exodus.log.DataCorruptionException.Companion.raise
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeCursor
import jetbrains.exodus.tree.TreeCursor
import java.io.PrintStream

/**
 * Base BTree implementation
 */
abstract class BTreeBase internal constructor(
    override val log: Log,
    val balancePolicy: BTreeBalancePolicy,
    val allowsDuplicates: Boolean,
    override val structureId: Int
) : ITree {
    protected var dataIterator: DataIterator? = null
    final override var size: Long = -1

    abstract override val mutableCopy: BTreeMutable

    /**
     * Returns root page of the tree
     *
     * @return tree root
     */
    abstract val root: BasePage
    override val isEmpty: Boolean
        get() = this.root.size == 0

    override fun getDataIterator(address: Long): DataIterator {
        if (dataIterator == null) {
            dataIterator = DataIterator(log, address)
        } else {
            if (address >= 0L) {
                dataIterator!!.checkPage(address)
            }
        }
        return dataIterator!!
    }

    override fun addressIterator(): AddressIterator {
        val traverser: BTreeTraverser =
            if (allowsDuplicates) BTreeMutatingTraverserDup.create(this) else BTreeMutatingTraverser.create(
                this
            )
        return AddressIterator(if (isEmpty) null else this, traverser.currentPos >= 0 && !isEmpty, traverser)
    }

    override fun openCursor(): ITreeCursor {
        return if (allowsDuplicates) BTreeCursorDup(BTreeTraverserDup(this.root)) else TreeCursor(BTreeTraverser(this.root))
    }

    fun getLoggable(address: Long): RandomAccessLoggable {
        return log.readNotNull(getDataIterator(address), address)
    }

    fun loadPage(address: Long): BasePageImmutable {
        val loggable = getLoggable(address)
        return loadPage(loggable.type.toInt(), loggable.data, loggable.isDataInsideSinglePage)
    }

    protected fun loadPage(
        type: Int, data: ByteIterableWithAddress,
        loggableInsideSinglePage: Boolean
    ): BasePageImmutable {
        val result: BasePageImmutable = when (type.toByte()) {
            LEAF_DUP_BOTTOM_ROOT, BOTTOM_ROOT, BOTTOM, DUP_BOTTOM -> BottomPage(
                this,
                data,
                loggableInsideSinglePage
            )

            LEAF_DUP_INTERNAL_ROOT, INTERNAL_ROOT, INTERNAL, DUP_INTERNAL -> InternalPage(
                this,
                data,
                loggableInsideSinglePage
            )

            else -> throw IllegalArgumentException("Unknown loggable type [$type]")
        }
        return result
    }

    open fun loadLeaf(address: Long): LeafNode {
        val loggable = getLoggable(address)
        return when (val type = loggable.type) {
            LEAF, DUP_LEAF -> LeafNode(
                log, loggable
            )

            LEAF_DUP_BOTTOM_ROOT, LEAF_DUP_INTERNAL_ROOT -> if (allowsDuplicates) {
                LeafNodeDup(this, loggable)
            } else {
                throw ExodusException(
                    "Try to load leaf with duplicates, but tree is not configured " +
                            "to support duplicates."
                )
            }

            else -> {
                raise("Unexpected loggable type: $type", log, address)
                // dummy unreachable statement
                throw RuntimeException()
            }
        }
    }

    open fun isDupKey(address: Long): Boolean {
        val type = getLoggable(address).type
        return type == LEAF_DUP_BOTTOM_ROOT || type == LEAF_DUP_INTERNAL_ROOT
    }

    fun compareLeafToKey(address: Long, key: ByteIterable): Int {
        val loggable = getLoggable(address)
        val data = loggable.data
        val keyLength = data.compressedUnsignedInt
        val keyRecordSize = getCompressedSize(keyLength.toLong())
        return data.compareTo(keyRecordSize, keyLength, key, 0, key.length)
    }

    override fun get(key: ByteIterable): ByteIterable? {
        val leaf = this.root[key]
        return leaf?.value
    }

    override fun hasKey(key: ByteIterable): Boolean {
        return this.root.keyExists(key)
    }

    override fun hasPair(key: ByteIterable, value: ByteIterable): Boolean {
        return this.root.exists(key, value)
    }

    override fun dump(out: PrintStream) {
        this.root.dump(out, 0, null)
    }

    override fun dump(out: PrintStream, renderer: Dumpable.ToString?) {
        this.root.dump(out, 0, renderer)
    }

    companion object {
        const val BOTTOM_ROOT: Byte = 2
        const val INTERNAL_ROOT: Byte = 3
        const val BOTTOM: Byte = 4
        const val INTERNAL: Byte = 5
        const val LEAF: Byte = 6
        const val LEAF_DUP_BOTTOM_ROOT: Byte = 7
        const val LEAF_DUP_INTERNAL_ROOT: Byte = 8
        const val DUP_BOTTOM: Byte = 9
        const val DUP_INTERNAL: Byte = 10
        const val DUP_LEAF: Byte = 11
    }
}
