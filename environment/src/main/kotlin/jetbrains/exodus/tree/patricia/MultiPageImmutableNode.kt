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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.log.*
import java.util.*

internal val Byte.unsigned: Int get() = this.toInt() and 0xff

const val CHILDREN_BITSET_SIZE_LONGS = 4
const val CHILDREN_BITSET_SIZE_BYTES = CHILDREN_BITSET_SIZE_LONGS * Long.SIZE_BYTES

/**
 * In V2 database format, a Patricia node with children can be encoded in 3 different ways depending
 * on number of children. Each of the ways is optimal for the number of children it represents.
 */
enum class V2ChildrenFormat {
    Complete, // the node has all 256 children
    Sparse,   // the number of children is in the range 1..32
    Bitset    // the number of children is in the range 33..255
}

internal class MultiPageImmutableNode : NodeBase, ImmutableNode {
    private val log: Log
    private val data: ByteIterableWithAddress
    private val childAddressLength: Byte
    private val baseAddress: Long // if it is not equal to NULL_ADDRESS then the node is saved in the v2 format
    internal val loggable: RandomAccessLoggable
    private val childrenCount: Int
    private val v2Format: Boolean

    override fun isMutable(): Boolean = false

    override fun getLoggable(): RandomAccessLoggable = loggable

    override fun getChildrenCount(): Int = childrenCount
    override fun getAddress(): Long = loggable.getAddress()

    override fun getChildren(): NodeChildren = object : NodeChildren {
        override fun iterator(): NodeChildrenIterator {
            val childrenCount = childrenCount
            return if (childrenCount == 0)
                EmptyNodeChildrenIterator()
            else {
                if (v2Format) {
                    when (childrenCount) {
                        256 -> ImmutableNodeCompleteChildrenV2Iterator(-1, null)
                        in 1..32 -> ImmutableNodeSparseChildrenV2Iterator(-1, null)
                        else -> ImmutableNodeBitsetChildrenV2Iterator(-1, null)
                    }
                } else {
                    ImmutableNodeChildrenIterator(-1, null)
                }
            }
        }
    }

    override fun getChildrenLast(): NodeChildrenIterator {
        val childrenCount = childrenCount
        return if (childrenCount == 0)
            EmptyNodeChildrenIterator()
        else {
            if (v2Format) {
                when (childrenCount) {
                    256 -> ImmutableNodeCompleteChildrenV2Iterator(childrenCount, null)
                    in 1..32 -> ImmutableNodeSparseChildrenV2Iterator(childrenCount, null)
                    else -> ImmutableNodeBitsetChildrenV2Iterator(childrenCount, null)
                }
            } else {
                ImmutableNodeChildrenIterator(childrenCount, null)
            }
        }
    }

    constructor(log: Log, loggable: RandomAccessLoggable, data: ByteIterableWithAddress) :
            this(log, loggable.getType(), loggable, data, data.iterator())

    private constructor(
        log: Log,
        type: Byte,
        loggable: RandomAccessLoggable,
        data: ByteIterableWithAddress,
        it: ByteIteratorWithAddress
    ) : super(type, data, it) {
        this.log = log
        this.loggable = loggable

        var baseAddress = Loggable.NULL_ADDRESS
        if (PatriciaTreeBase.nodeHasChildren(type)) {
            val i = it.compressedUnsignedInt
            val childrenCount = i ushr 3
            if (childrenCount < VERSION2_CHILDREN_COUNT_BOUND) {
                this.childrenCount = childrenCount
            } else {
                this.childrenCount = childrenCount - VERSION2_CHILDREN_COUNT_BOUND
                baseAddress = it.compressedUnsignedLong
            }
            checkAddressLength(((i and 7) + 1).also { len -> childAddressLength = len.toByte() })
        } else {
            childrenCount = 0
            childAddressLength = 0.toByte()
        }
        this.baseAddress = baseAddress
        this.v2Format = baseAddress != Loggable.NULL_ADDRESS
        this.data = data.cloneWithAddressAndLength(it.address, it.available())
    }

    override fun asNodeBase(): NodeBase {
        return this
    }

    override fun getMutableCopy(mutableTree: PatriciaTreeMutable): MutableNode {
        return mutableTree.mutateNode(this)
    }

    override fun getChild(tree: PatriciaTreeBase, b: Byte): NodeBase? {
        if (v2Format) {
            getV2Child(b)?.let { searchResult ->
                return tree.loadNode(addressByOffsetV2(searchResult.offset))
            }
        } else {
            val key = b.unsigned
            var low = 0
            var high = childrenCount - 1
            while (low <= high) {
                val mid = low + high ushr 1
                val offset = mid * (childAddressLength + 1)
                val cmp = byteAt(offset).unsigned - key
                when {
                    cmp < 0 -> low = mid + 1
                    cmp > 0 -> high = mid - 1
                    else -> {
                        return tree.loadNode(nextLong(offset + 1))
                    }
                }
            }
        }
        return null
    }

    override fun getChildren(b: Byte): NodeChildrenIterator {
        if (v2Format) {
            getV2Child(b)?.let { searchResult ->
                val index = searchResult.index
                val node = ChildReference(b, addressByOffsetV2(searchResult.offset))
                return when (searchResult.childrenFormat) {
                    V2ChildrenFormat.Complete -> ImmutableNodeCompleteChildrenV2Iterator(index, node)
                    V2ChildrenFormat.Sparse -> ImmutableNodeSparseChildrenV2Iterator(index, node)
                    V2ChildrenFormat.Bitset -> ImmutableNodeBitsetChildrenV2Iterator(index, node)
                }
            }
        } else {
            val key = b.unsigned
            var low = 0
            var high = childrenCount - 1
            while (low <= high) {
                val mid = low + high ushr 1
                val offset = mid * (childAddressLength + 1)
                val cmp = byteAt(offset).unsigned - key
                when {
                    cmp < 0 -> low = mid + 1
                    cmp > 0 -> high = mid - 1
                    else -> {
                        val suffixAddress = nextLong(offset + 1)
                        return ImmutableNodeChildrenIterator(mid, ChildReference(b, suffixAddress))
                    }
                }
            }
        }
        return EmptyNodeChildrenIterator()
    }

    override fun getChildrenRange(b: Byte): NodeChildrenIterator {
        val ub = b.unsigned
        if (v2Format) {
            when (val childrenCount = childrenCount) {
                0 -> return EmptyNodeChildrenIterator()
                256 -> return ImmutableNodeCompleteChildrenV2Iterator(
                    ub, ChildReference(b, addressByOffsetV2(ub * childAddressLength))
                )

                in 1..32 -> {
                    for (i in 0 until childrenCount) {
                        val nextByte = byteAt(i)
                        val next = nextByte.unsigned
                        if (ub <= next) {
                            return ImmutableNodeSparseChildrenV2Iterator(
                                i,
                                ChildReference(nextByte, addressByOffsetV2(childrenCount + i * childAddressLength))
                            )
                        }
                    }
                }

                else -> {
                    val bitsetIdx = ub / Long.SIZE_BITS
                    val bitset = data.nextLong(bitsetIdx * Long.SIZE_BYTES, Long.SIZE_BYTES)

                    val bit = ub % Long.SIZE_BITS
                    val bitmask = 1L shl bit
                    var index = (bitset and (bitmask - 1L)).countOneBits()

                    if (bitsetIdx > 0) {
                        for (i in 0 until bitsetIdx) {
                            index += data.nextLong(i * Long.SIZE_BYTES, Long.SIZE_BYTES).countOneBits()
                        }
                    }


                    val offset = CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength
                    if ((bitset and bitmask) != 0L) {
                        return ImmutableNodeBitsetChildrenV2Iterator(
                            index, ChildReference(b, addressByOffsetV2(offset))
                        )
                    }
                    if (index < childrenCount) {
                        return ImmutableNodeBitsetChildrenV2Iterator(
                            index - 1, ChildReference(b, addressByOffsetV2(offset))
                        ).apply { next() }
                    }
                }
            }
        } else {
            var low = -1
            var high = childrenCount
            var offset = -1
            var resultByte = 0.toByte()
            while (high - low > 1) {
                val mid = low + high + 1 ushr 1
                val off = mid * (childAddressLength + 1)
                val actual = byteAt(off)
                if (actual.unsigned >= ub) {
                    offset = off
                    resultByte = actual
                    high = mid
                } else {
                    low = mid
                }
            }
            if (offset >= 0) {
                val suffixAddress = nextLong(offset + 1)
                return ImmutableNodeChildrenIterator(high, ChildReference(resultByte, suffixAddress))
            }
        }
        return EmptyNodeChildrenIterator()
    }


    // if specified byte is found the returns index of child, offset of suffixAddress and type of children format
    private fun getV2Child(b: Byte): SearchResult? {
        val ub = b.unsigned
        when (val childrenCount = childrenCount) {
            0 -> return null
            256 -> return SearchResult(
                index = ub,
                offset = ub * childAddressLength,
                childrenFormat = V2ChildrenFormat.Complete
            )

            in 1..32 -> {
                for (i in 0 until childrenCount) {
                    val next = byteAt(i).unsigned
                    if (ub < next) break
                    if (ub == next) {
                        return SearchResult(
                            index = i,
                            offset = childrenCount + i * childAddressLength,
                            childrenFormat = V2ChildrenFormat.Sparse
                        )
                    }
                }
            }

            else -> {
                val bitsetIdx = ub / Long.SIZE_BITS
                val bitset = data.nextLong(bitsetIdx * Long.SIZE_BYTES, Long.SIZE_BYTES)

                val bit = ub % Long.SIZE_BITS
                val bitmask = 1L shl bit
                if ((bitset and bitmask) == 0L) return null
                var index = (bitset and (bitmask - 1L)).countOneBits()

                if (bitsetIdx > 0) {
                    for (i in 0 until bitsetIdx) {
                        index += data.nextLong(i * Long.SIZE_BYTES, Long.SIZE_BYTES).countOneBits()
                    }
                }

                return SearchResult(
                    index = index,
                    offset = CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength,
                    childrenFormat = V2ChildrenFormat.Bitset
                )
            }
        }
        return null
    }

    private fun byteAt(offset: Int): Byte {
        return data.byteAt(offset)
    }

    private fun nextLong(offset: Int): Long {
        return data.nextLong(offset, childAddressLength.toInt())
    }

    private fun addressByOffsetV2(offset: Int) = nextLong(offset) + baseAddress

    private fun childReferenceV1(index: Int) = (index * (childAddressLength + 1)).let { offset ->
        ChildReference(byteAt(offset), nextLong(offset + 1))
    }

    // get child reference in case of v2 format with complete children (the node has all 256 children)
    private fun childReferenceCompleteV2(index: Int) =
        ChildReference(index.toByte(), addressByOffsetV2(index * childAddressLength))

    // get child reference in case of v2 format with sparse children (the number of children is in the range 1..32)
    private fun childReferenceSparseV2(index: Int) =
        ChildReference(byteAt(index), addressByOffsetV2(childrenCount + index * childAddressLength))

    // get child reference in case of v2 format with bitset children representation
    // (the number of children is in the range 33..255)
    private fun childReferenceBitsetV2(index: Int, bit: Int) =
        ChildReference(bit.toByte(), addressByOffsetV2(CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength))

    private fun fillChildReferenceV1(index: Int, node: ChildReference) =
        (index * (childAddressLength + 1)).let { offset ->
            node.firstByte = byteAt(offset)
            node.suffixAddress = nextLong(offset + 1)
        }

    private fun fillChildReferenceCompleteV2(index: Int, node: ChildReference) {
        node.firstByte = index.toByte()
        node.suffixAddress = addressByOffsetV2(index * childAddressLength)
    }

    private fun fillChildReferenceSparseV2(index: Int, node: ChildReference) {
        node.firstByte = byteAt(index)
        node.suffixAddress = addressByOffsetV2(childrenCount + index * childAddressLength)
    }

    private fun fillChildReferenceBitsetV2(index: Int, bit: Int, node: ChildReference) {
        node.firstByte = bit.toByte()
        node.suffixAddress = addressByOffsetV2(CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength)
    }

    private abstract inner class NodeChildrenIteratorBase(
        @JvmField protected var index: Int,
        @JvmField protected var node: ChildReference?
    ) :
        NodeChildrenIterator {

        override fun getIndex(): Int = index

        override fun getNode(): ChildReference? = node

        override fun isMutable(): Boolean = false

        override fun hasNext() = index < childrenCount - 1

        override fun hasPrev() = index > 0

        override fun remove() =
            throw ExodusException("Can't remove manually Patricia node child, use Store.delete() instead")

        override fun getParentNode(): NodeBase = this@MultiPageImmutableNode

        override fun getKey(): ByteIterable = this@MultiPageImmutableNode.key
    }

    private inner class ImmutableNodeChildrenIterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {

        override fun next(): ChildReference = childReferenceV1(++index).also { node = it }

        override fun prev(): ChildReference = childReferenceV1(--index).also { node = it }

        override fun nextInPlace() = fillChildReferenceV1(++index, node.notNull)

        override fun prevInPlace() = fillChildReferenceV1(--index, node.notNull)
    }

    // v2 format complete children (the node has all 256 children)
    private inner class ImmutableNodeCompleteChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {

        override fun next() = childReferenceCompleteV2(++index).also { node = it }

        override fun prev() = childReferenceCompleteV2(--index).also { node = it }

        override fun nextInPlace() = fillChildReferenceCompleteV2(++index, node.notNull)

        override fun prevInPlace() = fillChildReferenceCompleteV2(--index, node.notNull)
    }

    // v2 format sparse children (the number of children is in the range 1..32)
    private inner class ImmutableNodeSparseChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {

        override fun next() = childReferenceSparseV2(++index).also { node = it }

        override fun prev() = childReferenceSparseV2(--index).also { node = it }

        override fun nextInPlace() = fillChildReferenceSparseV2(++index, node.notNull)

        override fun prevInPlace() = fillChildReferenceSparseV2(--index, node.notNull)
    }

    // v2 format bitset children (the number of children is in the range 33..255)
    private inner class ImmutableNodeBitsetChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {

        private val bitset = LongArray(CHILDREN_BITSET_SIZE_LONGS).let { array ->

            array.indices.forEach { i ->
                array[i] = data.nextLong(i * Long.SIZE_BYTES, Long.SIZE_BYTES)
            }
            BitSet.valueOf(array)
        }
        private var bit = -1

        override fun next(): ChildReference {
            incIndex()
            return childReferenceBitsetV2(index, bit).also { node = it }
        }

        override fun prev(): ChildReference {
            decIndex()
            return childReferenceBitsetV2(index, bit).also { node = it }
        }

        override fun nextInPlace() {
            incIndex()
            fillChildReferenceBitsetV2(index, bit, node.notNull)
        }

        override fun prevInPlace() {
            decIndex()
            fillChildReferenceBitsetV2(index, bit, node.notNull)
        }

        private fun incIndex() {
            ++index
            if (bit < 0) {
                for (i in 0..index) {
                    bit = bitset.nextSetBit(bit + 1)
                }
            } else {
                bit = bitset.nextSetBit(bit + 1)
            }
            if (bit < 0) {
                throw ExodusException("Inconsistent children bitset in Patricia node")
            }
        }

        private fun decIndex() {
            --index
            if (bit < 0) {
                bit = 256
                for (i in index until childrenCount) {
                    bit = bitset.previousSetBit(bit - 1)
                }
            } else {
                if (bit == 0) {
                    throw ExodusException("Inconsistent children bitset in Patricia node")
                }
                bit = bitset.previousSetBit(bit - 1)
            }
            if (bit < 0) {
                throw ExodusException("Inconsistent children bitset in Patricia node")
            }
        }
    }
}

data class SearchResult(val index: Int, val offset: Int, val childrenFormat: V2ChildrenFormat)

private fun checkAddressLength(addressLen: Int) {
    if (addressLen < 0 || addressLen > 8) {
        throw ExodusException("Invalid length of address: $addressLen")
    }
}
