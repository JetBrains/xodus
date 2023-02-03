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
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.fillBytes
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.getCompressedSize
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.SingleByteIterable
import jetbrains.exodus.log.TooBigLoggableException
import jetbrains.exodus.tree.patricia.ChildReferenceSet.ChildReferenceIterator
import jetbrains.exodus.util.LightOutputStream

const val VERSION2_CHILDREN_COUNT_BOUND = 257

internal open class MutableNode : NodeBase {

    internal val children: ChildReferenceSet

    constructor(origin: ImmutableNode) : super(origin.keySequence, origin.value) {
        children = ChildReferenceSet()
        copyChildrenFrom(origin)
    }

    constructor(keySequence: ByteIterable) : this(keySequence, null, ChildReferenceSet())

    constructor(keySequence: ByteIterable, value: ByteIterable?, children: ChildReferenceSet)
        : super(keySequence, value) {
        this.children = children
    }

    open fun setKeySequence(keySequence: ByteIterable) {
        this.keySequence = keySequence
    }

    open fun setValue(value: ByteIterable?) {
        this.value = value
    }

    public override fun getAddress() = Loggable.NULL_ADDRESS

    public override fun isMutable() = true

    public override fun getMutableCopy(mutableTree: PatriciaTreeMutable) = this

    fun getRef(pos: Int): ChildReference = children.referenceAt(pos)

    public override fun getChild(tree: PatriciaTreeBase, b: Byte): NodeBase? {
        val ref = children[b]
        return ref?.getNode(tree)
    }

    public override fun getChildren(): NodeChildren {
        return object : NodeChildren {
            override fun iterator(): NodeChildrenIterator {
                return if (children.isEmpty) EmptyNodeChildrenIterator()
                else MutableNodeChildrenIterator(this@MutableNode, children)
            }
        }
    }

    public override fun getChildren(b: Byte): NodeChildrenIterator {
        val index = children.searchFor(b)
        if (index < 0) return EmptyNodeChildrenIterator()
        return MutableNodeChildrenIterator(this, children.iterator(index))
    }

    public override fun getChildrenLast() = getChildren(children.size())

    fun getChildren(pos: Int): NodeChildrenIterator = MutableNodeChildrenIterator(this, children.iterator(pos))

    public override fun getChildrenRange(b: Byte): NodeChildrenIterator {
        if (children.isEmpty) {
            return EmptyNodeChildrenIterator()
        }
        var index = children.searchFor(b)
        if (index < 0) {
            index = -index - 1
        }
        return MutableNodeChildrenIterator(this, children.iterator(index))
    }

    public override fun getChildrenCount() = children.size()

    fun hasChildren() = !children.isEmpty

    fun setChild(b: Byte, child: MutableNode) {
        val index = children.searchFor(b)
        if (index < 0) {
            children.insertAt(-index - 1, ChildReferenceMutable(b, child))
        } else {
            val ref = children.referenceAt(index)
            if (ref.isMutable) {
                (ref as ChildReferenceMutable).child = child
            } else {
                children.setAt(index, ChildReferenceMutable(b, child))
            }
        }
    }

    fun setChild(index: Int, child: MutableNode) {
        val ref = children.referenceAt(index)
        if (ref.isMutable) {
            (ref as ChildReferenceMutable).child = child
        } else {
            children.setAt(index, ChildReferenceMutable(ref.firstByte, child))
        }
    }

    fun getRightChild(tree: PatriciaTreeBase, b: Byte): NodeBase? {
        val ref = children.right ?: return null
        val firstByte = ref.firstByte.unsigned
        val rightByte = b.unsigned
        require(rightByte >= firstByte)
        return if (rightByte > firstByte) null else ref.getNode(tree)
    }

    /**
     * Adds child which is greater (i.e. its next byte greater) than any other.
     *
     * @param b     next byte of child suffix.
     * @param child child node.
     */
    fun addRightChild(b: Byte, child: MutableNode) {
        val right = children.right
        require(right == null || right.firstByte.unsigned < b.unsigned)
        children.putRight(ChildReferenceMutable(b, child))
    }

    /**
     * Sets in-place the right child with the same first byte.
     *
     * @param b     next byte of child suffix.
     * @param child child node.
     */
    fun setRightChild(b: Byte, child: MutableNode) {
        val right = children.right
        require(right != null && right.firstByte.unsigned == b.unsigned)
        children.setAt(children.size() - 1, ChildReferenceMutable(b, child))
    }

    fun removeChild(b: Byte) = children.remove(b)

    /**
     * Splits current node onto two ones: prefix defined by prefix length and suffix linked with suffix via nextByte.
     *
     * @param prefixLength length of the prefix.
     * @param nextByte     next byte after prefix linking it with suffix.
     * @return the prefix node.
     */
    fun splitKey(prefixLength: Int, nextByte: Byte): MutableNode {
        val keyBytes = keySequence.bytesUnsafe
        val prefixKey = when (prefixLength) {
            0 -> ByteIterable.EMPTY
            1 -> SingleByteIterable.getIterable(keyBytes[0])
            else -> ArrayByteIterable(keyBytes, prefixLength)
        }
        val prefix = MutableNode(prefixKey)
        val suffixKey = when (val suffixLength = keySequence.length - prefixLength - 1) {
            0 -> ByteIterable.EMPTY
            1 -> SingleByteIterable.getIterable(keyBytes[prefixLength + 1])
            else -> keySequence.subIterable(prefixLength + 1, suffixLength)
        }
        val suffix = MutableNode(
            suffixKey, value,  // copy children of this node to the suffix one
            children
        )
        prefix.setChild(nextByte, suffix)
        return prefix
    }

    fun splitKey(prefixLength: Int, nextByte: Int) = splitKey(prefixLength, nextByte.toByte())

    fun mergeWithSingleChild(tree: PatriciaTreeMutable) {
        val ref = getChildren().iterator().next() ?: throw IllegalStateException("A child is expected")
        val child = ref.getNode(tree)
        value = child.value
        keySequence = CompoundByteIterable(
            arrayOf(keySequence, SingleByteIterable.getIterable(ref.firstByte), child.keySequence)
        )
        copyChildrenFrom(child)
    }

    fun hang(firstByte: Byte, tail: ByteIterator): MutableNode {
        val result = MutableNode(ArrayByteIterable(tail))
        setChild(firstByte, result)
        return result
    }

    fun hang(firstByte: Int, tail: ByteIterator) = hang(firstByte.toByte(), tail)

    fun hangRight(firstByte: Byte, tail: ByteIterator): MutableNode {
        val result = MutableNode(ArrayByteIterable(tail))
        addRightChild(firstByte, result)
        return result
    }

    fun hangRight(firstByte: Int, tail: ByteIterator) = hangRight(firstByte.toByte(), tail)

    fun save(tree: PatriciaTreeMutable, context: MutableNodeSaveContext): Long {
        val nodeStream = context.newNodeStream()
        // save key and value
        if (hasKey()) {
            fillBytes(keySequence.length.toLong(), nodeStream)
            ByteIterableBase.fillBytes(keySequence, nodeStream)
        }
        value?.let { value ->
            fillBytes(value.length.toLong(), nodeStream)
            ByteIterableBase.fillBytes(value, nodeStream)
        }
        val childrenCount = childrenCount
        if (childrenCount > 0) {
            // save references to children
            if (tree.useV1Format || childrenCount < 2 /* for single child, v2 format definitely wouldn't benefit */) {
                saveChildrenV1(childrenCount, nodeStream)
            } else {
                saveChildrenV2(childrenCount, nodeStream)
            }
        }
        // finally, write loggable
        val log = tree.getLog()
        var type = loggableType
        val structureId = tree.getStructureId()
        val mainIterable: ByteIterable = nodeStream.asArrayByteIterable()
        val startAddress = context.startAddress
        var result: Long
        if (!isRoot) {
            result = log.write(type, structureId, mainIterable)
            // save address of the first saved loggable
            if (startAddress == Loggable.NULL_ADDRESS) {
                context.startAddress = result
            }
            return result
        }
        val iterables = arrayOfNulls<ByteIterable>(3)
        iterables[0] = context.preliminaryRootData
        // Save root without back reference if the tree consists of root only.
        // In that case, startAddress is undefined, i.e. no other child is saved before root.
        if (startAddress == Loggable.NULL_ADDRESS) {
            iterables[1] = mainIterable
            result = log.write(type, structureId, CompoundByteIterable(iterables, 2))
            return result
        }
        // Tree is saved with several loggables. Is it saved in a single file?
        val singleFile = log.isLastWrittenFileAddress(startAddress)
        val pos: Int // where the offset info will be inserted
        if (!singleFile) {
            pos = 1
            iterables[2] = mainIterable
        } else {
            iterables[1] = mainIterable
            result = log.tryWrite(type, structureId, CompoundByteIterable(iterables, 2))
            if (result >= 0) {
                return result
            }
            pos = 1
            iterables[2] = mainIterable
        }
        type = type plus PatriciaTreeBase.ROOT_BIT_WITH_BACKREF
        iterables[pos] = CompressedUnsignedLongByteIterable.getIterable(log.writtenHighAddress - startAddress)
        val data: ByteIterable = CompoundByteIterable(iterables, pos + 2)
        result =
            if (singleFile) log.writeContinuously(type, structureId, data) else log.tryWrite(type, structureId, data)
        if (result < 0) {
            if (!singleFile) {
                iterables[pos] = CompressedUnsignedLongByteIterable.getIterable(log.writtenHighAddress - startAddress)
                result = log.writeContinuously(type, structureId, CompoundByteIterable(iterables, pos + 2))
                if (result >= 0) {
                    return result
                }
            }
            throw TooBigLoggableException()
        }
        return result
    }

    protected open val isRoot: Boolean get() = false

    private fun copyChildrenFrom(node: NodeBase) {
        val childrenCount = node.childrenCount
        children.clear(childrenCount)
        if (childrenCount > 0) {
            for ((i, child) in node.children.withIndex()) {
                children.setAt(i, child.notNull)
            }
        }
    }

    private val loggableType: Byte
        get() {
            var result = PatriciaTreeBase.NODE_WO_KEY_WO_VALUE_WO_CHILDREN
            if (hasKey()) {
                result = result plus PatriciaTreeBase.HAS_KEY_BIT
            }
            if (hasValue()) {
                result = result plus PatriciaTreeBase.HAS_VALUE_BIT
            }
            if (hasChildren()) {
                result = result plus PatriciaTreeBase.HAS_CHILDREN_BIT
            }
            if (isRoot) {
                result = result plus PatriciaTreeBase.ROOT_BIT
            }
            return result
        }

    private fun saveChildrenV1(childrenCount: Int, nodeStream: LightOutputStream) {
        val bytesPerAddress = children.maxOf { r -> CompressedUnsignedLongArrayByteIterable.logarithm(r.suffixAddress) }
        fillBytes(((childrenCount shl 3) + bytesPerAddress - 1).toLong(), nodeStream)
        for (ref in children) {
            nodeStream.write(ref.firstByte.toInt())
            LongBinding.writeUnsignedLong(ref.suffixAddress, bytesPerAddress, nodeStream)
        }
    }

    private fun saveChildrenV2(childrenCount: Int, nodeStream: LightOutputStream) {
        val baseAddress = children.minOf { r -> r.suffixAddress }
        val bytesPerAddress = children.maxOf { r ->
            CompressedUnsignedLongArrayByteIterable.logarithm(r.suffixAddress - baseAddress)
        }
        // if this is a sparse node make sure v2 format would result in a more compact saving
        if (childrenCount <= 32) {
            val bytesPerAddressV1 =
                children.maxOf { r -> CompressedUnsignedLongArrayByteIterable.logarithm(r.suffixAddress) }
            if (bytesPerAddress == bytesPerAddressV1 ||
                (bytesPerAddressV1 - bytesPerAddress) * childrenCount <= getCompressedSize(baseAddress) + 1
            ) {
                saveChildrenV1(childrenCount, nodeStream)
                return
            }
        }
        fillBytes((((childrenCount + VERSION2_CHILDREN_COUNT_BOUND) shl 3) + bytesPerAddress - 1).toLong(), nodeStream)
        fillBytes(baseAddress, nodeStream)
        if (childrenCount < 256) {
            if (childrenCount <= 32) {
                for (ref in children) {
                    nodeStream.write(ref.firstByte.toInt())
                }
            } else {
                val bitset = longArrayOf(0L, 0L, 0L, 0L)
                for (ref in children) {
                    val b = ref.firstByte.unsigned
                    bitset[b / Long.SIZE_BITS] += 1L shl (b % Long.SIZE_BITS)
                }
                bitset.forEach { l ->
                    LongBinding.writeUnsignedLong(l, Long.SIZE_BYTES, nodeStream)
                }
            }
        }
        for (ref in children) {
            LongBinding.writeUnsignedLong(ref.suffixAddress - baseAddress, bytesPerAddress, nodeStream)
        }
    }

    private class MutableNodeChildrenIterator : NodeChildrenIterator {

        private val _node: MutableNode
        private val refs: ChildReferenceIterator
        override val key: ByteIterable
        private var ref: ChildReference? = null

        constructor(node: MutableNode, refs: ChildReferenceSet) {
            this._node = node
            this.refs = refs.iterator()
            key = node.keySequence
        }

        constructor(node: MutableNode, refs: ChildReferenceIterator) {
            this._node = node
            this.refs = refs
            this.ref = refs.currentRef()
            key = node.keySequence
        }

        override fun hasNext() = refs.hasNext()

        override fun next(): ChildReference = refs.next().also { this.ref = it }

        override fun hasPrev() = refs.index > 0

        override fun prev(): ChildReference = refs.prev().also { this.ref = it }

        override val isMutable = true

        override fun nextInPlace() = throw UnsupportedOperationException()

        override fun prevInPlace() = throw UnsupportedOperationException()

        override fun remove() {
            _node.removeChild(ref.notNull.firstByte)
        }

        override val node: ChildReference? get() = ref

        override val parentNode: MutableNode get() = _node

        override val index: Int get() = refs.index
    }
}

private infix fun Byte.plus(b: Byte): Byte = (this + b).toByte()