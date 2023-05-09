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

import jetbrains.exodus.*
import jetbrains.exodus.log.ByteIterableWithAddress
import jetbrains.exodus.log.ByteIteratorWithAddress
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.NullLoggable.create
import jetbrains.exodus.log.RandomAccessLoggable
import java.util.*

internal class SinglePageImmutableNode : NodeBase, ImmutableNode {
    @JvmField
    internal val loggable: RandomAccessLoggable
    private val data: ByteArray
    private var dataStart: Int
    private val dataEnd: Int
    private val childrenCount: Int
    private val childAddressLength: Byte
    private val v2Format: Boolean
    private val baseAddress // if it is not equal to NULL_ADDRESS then the node is saved in the v2 format
            : Long

    constructor(
        loggable: RandomAccessLoggable,
        data: ByteIterableWithAddress
    ) : this(loggable.getType(), loggable, data, data.iterator())

    constructor() : super(ByteIterable.EMPTY, null) {
        dataStart = 0
        dataEnd = 0
        data = ArrayByteIterable.EMPTY_BYTES
        loggable = create()
        childrenCount = 0
        childAddressLength = 0
        baseAddress = Loggable.NULL_ADDRESS
        v2Format = true
    }

    private constructor(
        type: Byte,
        loggable: RandomAccessLoggable,
        data: ByteIterableWithAddress, it: ByteIteratorWithAddress
    ) : super(type, data, it) {
        dataStart = it.offset
        dataEnd = dataStart + data.length
        this.data = data.baseBytes
        this.loggable = loggable
        var baseAddress = Loggable.NULL_ADDRESS
        if (PatriciaTreeBase.nodeHasChildren(type)) {
            val i = getCompressedUnsignedInt()
            val childrenCount = i ushr 3
            if (childrenCount < VERSION2_CHILDREN_COUNT_BOUND) {
                this.childrenCount = childrenCount
            } else {
                this.childrenCount = childrenCount - VERSION2_CHILDREN_COUNT_BOUND
                baseAddress = getCompressedUnsignedLong()
            }
            val addressLen = (i and 7) + 1
            checkAddressLength(addressLen)
            childAddressLength = addressLen.toByte()
        } else {
            childrenCount = 0
            childAddressLength = 0
        }
        this.baseAddress = baseAddress
        v2Format = baseAddress != Loggable.NULL_ADDRESS
    }

    override fun getChildrenCount(): Int = childrenCount

    override fun getLoggable(): RandomAccessLoggable = loggable

    private fun checkAddressLength(addressLen: Int) {
        if (addressLen < 0 || addressLen > 8) {
            throw ExodusException("Invalid length of address: \$addressLen")
        }
    }

    private fun getCompressedUnsignedInt(): Int {
        var result = 0
        var shift = 0
        do {
            val b = data[dataStart++]
            result += b.toInt() and 0x7f shl shift
            if (b.toInt() and 0x80 != 0) {
                return result
            }
            shift += 7
        } while (dataStart < dataEnd)
        throw ExodusException("Bad compressed number")
    }

    private fun getCompressedUnsignedLong(): Long {
        var result: Long = 0
        var shift = 0
        do {
            val b = data[dataStart++]
            result += (b.toInt() and 0x7f).toLong() shl shift
            if (b.toInt() and 0x80 != 0) {
                return result
            }
            shift += 7
        } while (dataStart < dataEnd)
        throw ExodusException("Bad compressed number")
    }

    override fun getAddress(): Long = loggable.getAddress()

    override fun asNodeBase(): NodeBase {
        return this
    }

    override fun isMutable(): Boolean = false

    override fun getMutableCopy(mutableTree: PatriciaTreeMutable): MutableNode {
        return mutableTree.mutateNode(this)
    }

    override fun getChild(tree: PatriciaTreeBase, b: Byte): NodeBase? {
        if (v2Format) {
            val result = getV2Child(b)
            if (result != null) {
                return tree.loadNode(addressByOffsetV2(result.offset))
            }
        } else {
            val key = java.lang.Byte.toUnsignedInt(b)
            var low = 0
            var high = childrenCount - 1
            while (low <= high) {
                val mid = low + high ushr 1
                val offset = mid * (childAddressLength + 1)
                val cmp = java.lang.Byte.toUnsignedInt(byteAt(offset)) - key
                if (cmp < 0) {
                    low = mid + 1
                } else if (cmp > 0) {
                    high = mid - 1
                } else {
                    return tree.loadNode(nextLong(offset + 1))
                }
            }
        }
        return null
    }

    override fun getChildren(): NodeChildren = object : NodeChildren {
        override fun iterator(): NodeChildrenIterator {
            val childrenCount = (this@SinglePageImmutableNode).childrenCount
            if (childrenCount == 0) {
                return EmptyNodeChildrenIterator()
            }
            if (v2Format) {
                if (childrenCount == 256) {
                    return ImmutableNodeCompleteChildrenV2Iterator(-1, null)
                }
                if (childrenCount in 1..32) {
                    return ImmutableNodeSparseChildrenV2Iterator(-1, null)
                }
                return ImmutableNodeBitsetChildrenV2Iterator(-1, null)
            } else {
                return ImmutableNodeChildrenIterator(-1, null)
            }
        }
    }

    override fun getChildrenRange(b: Byte): NodeChildrenIterator {
        val ub = java.lang.Byte.toUnsignedInt(b)
        if (v2Format) {
            val childrenCount = childrenCount
            if (childrenCount == 0) {
                return EmptyNodeChildrenIterator()
            }
            if (childrenCount == 256) {
                return ImmutableNodeCompleteChildrenV2Iterator(
                    ub, ChildReference(b, addressByOffsetV2(ub * childAddressLength))
                )
            }
            if (childrenCount in 1..32) {
                for (i in 0 until childrenCount) {
                    val nextByte = byteAt(i)
                    val next = java.lang.Byte.toUnsignedInt(nextByte)
                    if (ub <= next) {
                        return ImmutableNodeSparseChildrenV2Iterator(
                            i,
                            ChildReference(
                                nextByte,
                                addressByOffsetV2(childrenCount + i * childAddressLength)
                            )
                        )
                    }
                }
            } else {
                val bitsetIdx = ub / java.lang.Long.SIZE
                val bitset = nextLong(bitsetIdx * java.lang.Long.BYTES, java.lang.Long.BYTES)
                val bit = ub % java.lang.Long.SIZE
                val bitmask = 1L shl bit
                var index = java.lang.Long.bitCount(bitset and bitmask - 1L)
                if (bitsetIdx > 0) {
                    for (i in 0 until bitsetIdx) {
                        index += java.lang.Long.bitCount(nextLong(i * java.lang.Long.BYTES, java.lang.Long.BYTES))
                    }
                }
                val offset = CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength
                if (bitset and bitmask != 0L) {
                    return ImmutableNodeBitsetChildrenV2Iterator(
                        index, ChildReference(b, addressByOffsetV2(offset))
                    )
                }
                if (index < childrenCount) {
                    val iterator = ImmutableNodeBitsetChildrenV2Iterator(
                        index - 1, ChildReference(b, addressByOffsetV2(offset))
                    )
                    iterator.next()
                    return iterator
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
                if (java.lang.Byte.toUnsignedInt(actual) >= ub) {
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

    override fun getChildren(b: Byte): NodeChildrenIterator {
        if (v2Format) {
            val searchResult = getV2Child(b)
            if (searchResult != null) {
                val index = searchResult.index
                val node = ChildReference(b, addressByOffsetV2(searchResult.offset))
                return when (searchResult.childrenFormat) {
                    V2ChildrenFormat.Complete -> ImmutableNodeCompleteChildrenV2Iterator(
                        index,
                        node
                    )

                    V2ChildrenFormat.Sparse -> ImmutableNodeSparseChildrenV2Iterator(
                        index,
                        node
                    )

                    V2ChildrenFormat.Bitset -> ImmutableNodeBitsetChildrenV2Iterator(
                        index,
                        node
                    )
                }
            }
        } else {
            val key = java.lang.Byte.toUnsignedInt(b)
            var low = 0
            var high = childrenCount - 1
            while (low <= high) {
                val mid = low + high ushr 1
                val offset = mid * (childAddressLength + 1)
                val cmp = java.lang.Byte.toUnsignedInt(byteAt(offset)) - key
                if (cmp < 0) {
                    low = mid + 1
                } else if (cmp > 0) {
                    high = mid - 1
                } else {
                    val suffixAddress = nextLong(offset + 1)
                    return ImmutableNodeChildrenIterator(mid, ChildReference(b, suffixAddress))
                }
            }
        }
        return EmptyNodeChildrenIterator()
    }

    override fun getChildrenLast(): NodeChildrenIterator {
        val childrenCount = childrenCount
        if (childrenCount == 0) {
            return EmptyNodeChildrenIterator()
        }
        return if (v2Format) {
            if (childrenCount == 256) {
                return ImmutableNodeCompleteChildrenV2Iterator(
                    childrenCount,
                    null
                )
            }
            if (childrenCount in 2..32) {
                ImmutableNodeSparseChildrenV2Iterator(
                    childrenCount,
                    null
                )
            } else ImmutableNodeBitsetChildrenV2Iterator(
                childrenCount,
                null
            )
        } else {
            ImmutableNodeChildrenIterator(childrenCount, null)
        }
    }

    private fun addressByOffsetV2(offset: Int): Long {
        return nextLong(offset) + baseAddress
    }

    private fun childReferenceV1(index: Int): ChildReference {
        val offset = index * (childAddressLength + 1)
        return ChildReference(byteAt(offset), nextLong(offset + 1))
    }

    /**
     * Get child reference in case of v2 format with complete children (the node has all 256 children)
     */
    private fun childReferenceCompleteV2(index: Int): ChildReference {
        return ChildReference(index.toByte(), addressByOffsetV2(index * childAddressLength))
    }

    /**
     * get child reference in case of v2 format with sparse children
     * (the number of children is in the range 1..32)
     */
    private fun childReferenceSparseV2(index: Int): ChildReference {
        return ChildReference(
            byteAt(index), addressByOffsetV2(
                childrenCount +
                        index * childAddressLength
            )
        )
    }

    /**
     * get child reference in case of v2 format with bitset children representation
     * / * (the number of children is in the range 33..255)
     */
    private fun childReferenceBitsetV2(index: Int, bit: Int): ChildReference {
        return ChildReference(
            bit.toByte(),
            addressByOffsetV2(CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength)
        )
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

    private fun fillChildReferenceV1(index: Int, node: ChildReference) {
        val offset = index * (childAddressLength + 1)
        node.firstByte = byteAt(offset)
        node.suffixAddress = nextLong(offset + 1)
    }

    private fun getV2Child(b: Byte): SearchResult? {
        val ub = java.lang.Byte.toUnsignedInt(b)
        val childrenCount = childrenCount
        if (childrenCount == 0) {
            return null
        }
        if (childrenCount == 256) {
            return SearchResult(ub, ub * childAddressLength, V2ChildrenFormat.Complete)
        }
        if (childrenCount in 1..32) {
            for (i in 0 until childrenCount) {
                val next = java.lang.Byte.toUnsignedInt(byteAt(i))
                if (ub < next) {
                    break
                }
                if (ub == next) {
                    return SearchResult(
                        i,
                        childrenCount + i * childAddressLength,
                        V2ChildrenFormat.Sparse
                    )
                }
            }
        } else {
            val bitsetIdx = ub / java.lang.Long.SIZE
            val bitset = nextLong(bitsetIdx * java.lang.Long.BYTES, java.lang.Long.BYTES)
            val bit = ub % java.lang.Long.SIZE
            val bitmask = 1L shl bit
            if (bitset and bitmask == 0L) {
                return null
            }
            var index = java.lang.Long.bitCount(bitset and bitmask - 1L)
            if (bitsetIdx > 0) {
                for (i in 0 until bitsetIdx) {
                    index += java.lang.Long.bitCount(nextLong(i * java.lang.Long.BYTES, java.lang.Long.BYTES))
                }
            }
            return SearchResult(
                index,
                CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength,
                V2ChildrenFormat.Bitset
            )
        }
        return null
    }

    private fun byteAt(offset: Int): Byte {
        return data[dataStart + offset]
    }

    private fun nextLong(offset: Int, length: Int = childAddressLength.toInt()): Long {
        val start = dataStart + offset
        val end = start + length
        var result: Long = 0
        for (i in start until end) {
            result = (result shl 8) + (data[i].toInt() and 0xff)
        }
        return result
    }

    private abstract inner class NodeChildrenIteratorBase protected constructor(
        @JvmField var index: Int,
        @JvmField var node: ChildReference?
    ) : NodeChildrenIterator {

        override fun getIndex(): Int = index

        override fun getNode(): ChildReference? = node

        override fun getKey(): ByteIterable? = this@SinglePageImmutableNode.key
        override fun isMutable(): Boolean = false

        override fun hasNext(): Boolean {
            return index < childrenCount - 1
        }

        override fun hasPrev(): Boolean {
            return index > 0
        }

        override fun remove() {
            throw ExodusException(
                "Can't remove manually Patricia node child, use Store.delete() instead"
            )
        }

        override fun getParentNode(): NodeBase? = this@SinglePageImmutableNode
    }

    private inner class ImmutableNodeChildrenIterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {
        override fun next(): ChildReference {
            node = childReferenceV1(++index)
            return node!!
        }

        override fun prev(): ChildReference {
            node = childReferenceV1(--index)
            return node!!
        }

        override fun nextInPlace() {
            fillChildReferenceV1(++index, node!!)
        }

        override fun prevInPlace() {
            fillChildReferenceV1(--index, node!!)
        }
    }

    /**
     * v2 format complete children (the node has all 256 children)
     */
    private inner class ImmutableNodeCompleteChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {
        override fun next(): ChildReference {
            node = childReferenceCompleteV2(++index)
            return node!!
        }

        override fun prev(): ChildReference {
            node = childReferenceCompleteV2(--index)
            return node!!
        }

        override fun nextInPlace() {
            fillChildReferenceCompleteV2(++index, node!!)
        }

        override fun prevInPlace() {
            fillChildReferenceCompleteV2(--index, node!!)
        }
    }

    /**
     * V2 format sparse children (the number of children is in the range 1..32)
     */
    private inner class ImmutableNodeSparseChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {
        override fun next(): ChildReference {
            node = childReferenceSparseV2(++index)
            return node!!
        }

        override fun prev(): ChildReference {
            node = childReferenceSparseV2(--index)
            return node!!
        }

        override fun nextInPlace() {
            fillChildReferenceSparseV2(++index, node!!)
        }

        override fun prevInPlace() {
            fillChildReferenceSparseV2(--index, node!!)
        }
    }

    // v2 format bitset children (the number of children is in the range 33..255)
    private inner class ImmutableNodeBitsetChildrenV2Iterator(index: Int, node: ChildReference?) :
        NodeChildrenIteratorBase(index, node) {
        private val bitset: BitSet
        private var bit = -1

        init {
            val bits = LongArray(CHILDREN_BITSET_SIZE_LONGS)
            for (i in bits.indices) {
                bits[i] = nextLong(i * java.lang.Long.BYTES, java.lang.Long.BYTES)
            }
            bitset = BitSet.valueOf(bits)
        }

        override fun next(): ChildReference {
            incIndex()
            node = childReferenceBitsetV2(index, bit)
            return node!!
        }

        override fun prev(): ChildReference {
            decIndex()
            node = childReferenceBitsetV2(index, bit)
            return node!!
        }

        override fun nextInPlace() {
            incIndex()
            fillChildReferenceBitsetV2(index, bit, node!!)
        }

        override fun prevInPlace() {
            decIndex()
            fillChildReferenceBitsetV2(index, bit, node!!)
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
