/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

import static jetbrains.exodus.tree.patricia.MultiPageImmutableNodeKt.CHILDREN_BITSET_SIZE_BYTES;
import static jetbrains.exodus.tree.patricia.MultiPageImmutableNodeKt.CHILDREN_BITSET_SIZE_LONGS;
import static jetbrains.exodus.tree.patricia.MutableNodeKt.VERSION2_CHILDREN_COUNT_BOUND;

final class SinglePageImmutableNode extends NodeBase implements ImmutableNode {
    final RandomAccessLoggable loggable;
    private final byte[] data;
    private int dataStart;

    private final int dataEnd;

    private final short childrenCount;
    private final byte childAddressLength;

    private final boolean v2Format;

    private final long baseAddress; // if it is not equal to NULL_ADDRESS then the node is saved in the v2 format

    public SinglePageImmutableNode(final RandomAccessLoggable loggable,
                                   final ByteIterableWithAddress data) {
        this(loggable.getType(), loggable, data, data.iterator());
    }

    SinglePageImmutableNode() {
        super(ByteIterable.EMPTY, null);

        this.dataStart = 0;
        this.dataEnd = 0;

        this.data = ArrayByteIterable.EMPTY_BYTES;
        this.loggable = NullLoggable.create();
        childrenCount = 0;
        childAddressLength = 0;
        baseAddress = Loggable.NULL_ADDRESS;
        v2Format = true;
    }

    private SinglePageImmutableNode(final byte type,
                                    final RandomAccessLoggable loggable,
                                    final ByteIterableWithAddress data, ByteIteratorWithAddress it) {
        super(type, data, it);

        this.dataStart = it.getOffset();
        this.dataEnd = dataStart + data.getLength();

        this.data = data.getBaseBytes();
        this.loggable = loggable;

        var baseAddress = Loggable.NULL_ADDRESS;

        if (PatriciaTreeBase.nodeHasChildren(type)) {
            var i = getCompressedUnsignedInt();
            var childrenCount = i >>> 3;
            if (childrenCount < VERSION2_CHILDREN_COUNT_BOUND) {
                this.childrenCount = (short) childrenCount;
            } else {
                this.childrenCount = (short) (childrenCount - VERSION2_CHILDREN_COUNT_BOUND);
                baseAddress = getCompressedUnsignedLong();
            }

            final int addressLen = (i & 7) + 1;
            checkAddressLength(addressLen);
            childAddressLength = (byte) addressLen;
        } else {
            childrenCount = 0;
            childAddressLength = 0;
        }

        this.baseAddress = baseAddress;
        this.v2Format = baseAddress != Loggable.NULL_ADDRESS;
    }

    private void checkAddressLength(int addressLen) {
        if (addressLen < 0 || addressLen > 8) {
            throw new ExodusException("Invalid length of address: $addressLen");
        }
    }

    private int getCompressedUnsignedInt() {
        int result = 0;
        int shift = 0;
        do {
            final byte b = data[dataStart++];
            result += (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        } while (dataStart < dataEnd);

        throw new ExodusException("Bad compressed number");
    }

    private long getCompressedUnsignedLong() {
        long result = 0;
        int shift = 0;
        do {
            final byte b = data[dataStart++];
            result += (long) (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        } while (dataStart < dataEnd);

        throw new ExodusException("Bad compressed number");
    }

    @Override
    public RandomAccessLoggable getLoggable() {
        return loggable;
    }

    @Override
    public long getAddress() {
        return loggable.getAddress();
    }

    @Override
    public NodeBase asNodeBase() {
        return this;
    }

    @Override
    boolean isMutable() {
        return false;
    }

    @Override
    MutableNode getMutableCopy(@NotNull PatriciaTreeMutable mutableTree) {
        return mutableTree.mutateNode(this);
    }

    @Override
    @Nullable NodeBase getChild(@NotNull PatriciaTreeBase tree, byte b) {
        if (v2Format) {
            var result = getV2Child(b);
            if (result != null) {
                return tree.loadNode(addressByOffsetV2(result.getOffset()));
            }
        } else {
            var key = Byte.toUnsignedInt(b);
            var low = 0;
            var high = childrenCount - 1;
            while (low <= high) {
                var mid = low + high >>> 1;
                var offset = mid * (childAddressLength + 1);
                var cmp = Byte.toUnsignedInt(byteAt(offset)) - key;

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return tree.loadNode(nextLong(offset + 1));
                }
            }
        }
        return null;
    }

    @Override
    @NotNull NodeChildren getChildren() {
        return () -> {
            var childrenCount = getChildrenCount();

            if (childrenCount == 0) {
                return new EmptyNodeChildrenIterator();
            }

            if (v2Format) {
                if (childrenCount == 256) {
                    return new ImmutableNodeCompleteChildrenV2Iterator(-1, null);
                }
                if (childrenCount >= 1 && childrenCount <= 32) {
                    return new ImmutableNodeSparseChildrenV2Iterator(-1, null);
                }

                return new ImmutableNodeBitsetChildrenV2Iterator(-1, null);
            } else {
                return new ImmutableNodeChildrenIterator(-1, null);
            }
        };
    }

    @Override
    @NotNull NodeChildrenIterator getChildrenRange(byte b) {
        var ub = Byte.toUnsignedInt(b);
        if (v2Format) {
            var childrenCount = getChildrenCount();
            if (childrenCount == 0) {
                return new EmptyNodeChildrenIterator();
            }
            if (childrenCount == 256) {
                return new ImmutableNodeCompleteChildrenV2Iterator(
                        ub, new ChildReference(b, addressByOffsetV2(ub * childAddressLength)));
            }

            if (childrenCount >= 1 && childrenCount <= 32) {
                for (int i = 0; i < childrenCount; i++) {
                    var nextByte = byteAt(i);
                    var next = Byte.toUnsignedInt(nextByte);

                    if (ub <= next) {
                        return new ImmutableNodeSparseChildrenV2Iterator(
                                i,
                                new ChildReference(nextByte,
                                        addressByOffsetV2(childrenCount + i * childAddressLength))
                        );
                    }
                }
            } else {
                var bitsetIdx = ub / Long.SIZE;
                var bitset = nextLong(bitsetIdx * Long.BYTES, Long.BYTES);

                var bit = ub % Long.SIZE;
                var bitmask = 1L << bit;
                var index = Long.bitCount(bitset & (bitmask - 1L));

                if (bitsetIdx > 0) {
                    for (int i = 0; i < bitsetIdx; i++) {
                        index += Long.bitCount(nextLong(i * Long.BYTES, Long.BYTES));
                    }
                }


                var offset = CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength;
                if ((bitset & bitmask) != 0L) {
                    return new ImmutableNodeBitsetChildrenV2Iterator(
                            index, new ChildReference(b, addressByOffsetV2(offset))
                    );
                }
                if (index < childrenCount) {
                    var iterator = new ImmutableNodeBitsetChildrenV2Iterator(
                            index - 1, new ChildReference(b, addressByOffsetV2(offset))
                    );
                    iterator.next();
                    return iterator;
                }
            }
        } else {
            var low = -1;
            var high = getChildrenCount();
            var offset = -1;
            var resultByte = (byte) 0;
            while (high - low > 1) {
                var mid = low + high + 1 >>> 1;
                var off = mid * (childAddressLength + 1);
                var actual = byteAt(off);

                if (Byte.toUnsignedInt(actual) >= ub) {
                    offset = off;
                    resultByte = actual;
                    high = mid;
                } else {
                    low = mid;
                }
            }
            if (offset >= 0) {
                var suffixAddress = nextLong(offset + 1);
                return new ImmutableNodeChildrenIterator(high, new ChildReference(resultByte, suffixAddress));
            }
        }

        return new EmptyNodeChildrenIterator();
    }

    @Override
    @NotNull NodeChildrenIterator getChildren(byte b) {
        if (v2Format) {
            var searchResult = getV2Child(b);
            if (searchResult != null) {
                var index = searchResult.getIndex();
                var node = new ChildReference(b, addressByOffsetV2(searchResult.getOffset()));

                switch (searchResult.getChildrenFormat()) {
                    case Complete:
                        return new ImmutableNodeCompleteChildrenV2Iterator(index, node);
                    case Sparse:
                        return new ImmutableNodeSparseChildrenV2Iterator(index, node);
                    case Bitset:
                        return new ImmutableNodeBitsetChildrenV2Iterator(index, node);
                }
            }
        } else {
            var key = Byte.toUnsignedInt(b);
            var low = 0;
            var high = childrenCount - 1;
            while (low <= high) {
                var mid = (low + high) >>> 1;
                var offset = mid * (childAddressLength + 1);
                var cmp = Byte.toUnsignedInt(byteAt(offset)) - key;
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    var suffixAddress = nextLong(offset + 1);
                    return new ImmutableNodeChildrenIterator(mid, new ChildReference(b, suffixAddress));
                }
            }
        }
        return new EmptyNodeChildrenIterator();
    }

    @Override
    int getChildrenCount() {
        return childrenCount;
    }

    @Override
    @NotNull NodeChildrenIterator getChildrenLast() {
        var childrenCount = getChildrenCount();
        if (childrenCount == 0) {
            return new EmptyNodeChildrenIterator();
        }

        if (v2Format) {
            if (childrenCount == 256) {
                return new ImmutableNodeCompleteChildrenV2Iterator(childrenCount, null);
            }

            if (childrenCount > 1 && childrenCount <= 32) {
                return new ImmutableNodeSparseChildrenV2Iterator(childrenCount, null);
            }

            return new ImmutableNodeBitsetChildrenV2Iterator(childrenCount, null);
        } else {
            return new ImmutableNodeChildrenIterator(childrenCount, null);
        }

    }

    private long addressByOffsetV2(int offset) {
        return nextLong(offset) + baseAddress;
    }

    private ChildReference childReferenceV1(int index) {
        var offset = index * (childAddressLength + 1);
        return new ChildReference(byteAt(offset), nextLong(offset + 1));
    }

    /**
     * Get child reference in case of v2 format with complete children (the node has all 256 children)
     */
    private ChildReference childReferenceCompleteV2(int index) {
        return new ChildReference((byte) index, addressByOffsetV2(index * childAddressLength));
    }

    /**
     * get child reference in case of v2 format with sparse children
     * (the number of children is in the range 1..32)
     */
    private ChildReference childReferenceSparseV2(int index) {
        return new ChildReference(byteAt(index), addressByOffsetV2(getChildrenCount() +
                index * childAddressLength));
    }

    /**
     * get child reference in case of v2 format with bitset children representation
     * /* (the number of children is in the range 33..255)
     */
    private ChildReference childReferenceBitsetV2(int index, int bit) {
        return new ChildReference((byte) bit,
                addressByOffsetV2(CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength));
    }

    private void fillChildReferenceCompleteV2(int index, ChildReference node) {
        node.firstByte = (byte) index;
        node.suffixAddress = addressByOffsetV2(index * childAddressLength);
    }

    private void fillChildReferenceSparseV2(int index, ChildReference node) {
        node.firstByte = byteAt(index);
        node.suffixAddress = addressByOffsetV2(getChildrenCount() + index * childAddressLength);
    }

    private void fillChildReferenceBitsetV2(int index, int bit, ChildReference node) {
        node.firstByte = (byte) bit;
        node.suffixAddress = addressByOffsetV2(CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength);
    }

    private void fillChildReferenceV1(int index, ChildReference node) {
        var offset = (index * (childAddressLength + 1));
        node.firstByte = byteAt(offset);
        node.suffixAddress = nextLong(offset + 1);
    }


    private SearchResult getV2Child(byte b) {
        var ub = Byte.toUnsignedInt(b);
        var childrenCount = this.childrenCount;

        if (childrenCount == 0) {
            return null;
        }
        if (childrenCount == 256) {
            return new SearchResult(ub, ub * childAddressLength, V2ChildrenFormat.Complete);
        }
        if (childrenCount >= 1 && childrenCount <= 32) {
            for (int i = 0; i < childrenCount; i++) {
                var next = Byte.toUnsignedInt(byteAt(i));
                if (ub < next) {
                    break;
                }

                if (ub == next) {
                    return new SearchResult(
                            i,
                            childrenCount + i * childAddressLength,
                            V2ChildrenFormat.Sparse
                    );
                }
            }
        } else {
            var bitsetIdx = ub / Long.SIZE;
            var bitset = nextLong(bitsetIdx * Long.BYTES, Long.BYTES);

            var bit = ub % Long.SIZE;
            var bitmask = 1L << bit;
            if ((bitset & bitmask) == 0L) {
                return null;
            }
            var index = Long.bitCount(bitset & (bitmask - 1L));

            if (bitsetIdx > 0) {
                for (int i = 0; i < bitsetIdx; i++) {
                    index += Long.bitCount(nextLong(i * Long.BYTES, Long.BYTES));
                }
            }

            return new SearchResult(
                    index,
                    CHILDREN_BITSET_SIZE_BYTES + index * childAddressLength,
                    V2ChildrenFormat.Bitset
            );
        }

        return null;
    }

    private byte byteAt(int offset) {
        return data[dataStart + offset];
    }

    private long nextLong(int offset) {
        return nextLong(offset, childAddressLength);
    }

    private long nextLong(int offset, int length) {
        final int start = dataStart + offset;
        final int end = start + length;

        long result = 0;
        for (int i = start; i < end; ++i) {
            result = (result << 8) + ((int) data[i] & 0xff);
        }

        return result;
    }

    private abstract class NodeChildrenIteratorBase implements NodeChildrenIterator {
        protected int index;
        protected ChildReference node;

        protected NodeChildrenIteratorBase(int index, ChildReference node) {
            this.index = index;
            this.node = node;
        }

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public boolean hasNext() {
            return index < childrenCount - 1;
        }

        @Override
        public boolean hasPrev() {
            return index > 0;
        }

        @Override
        public void remove() {
            throw new ExodusException(
                    "Can't remove manually Patricia node child, use Store.delete() instead");
        }

        @Nullable
        @Override
        public NodeBase getParentNode() {
            return SinglePageImmutableNode.this;
        }

        @Nullable
        @Override
        public ByteIterable getKey() {
            return keySequence;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Nullable
        @Override
        public ChildReference getNode() {
            return node;
        }
    }

    private final class ImmutableNodeChildrenIterator extends NodeChildrenIteratorBase {
        private ImmutableNodeChildrenIterator(int index, ChildReference node) {
            super(index, node);
        }

        @Override
        public ChildReference next() {
            node = childReferenceV1(++index);
            return node;
        }

        @Override
        public ChildReference prev() {
            node = childReferenceV1(--index);
            return node;
        }

        @Override
        public void nextInPlace() {
            fillChildReferenceV1(++index, node);
        }

        @Override
        public void prevInPlace() {
            fillChildReferenceV1(--index, node);
        }
    }

    /**
     * v2 format complete children (the node has all 256 children)
     */
    private final class ImmutableNodeCompleteChildrenV2Iterator extends NodeChildrenIteratorBase {
        private ImmutableNodeCompleteChildrenV2Iterator(int index, ChildReference node) {
            super(index, node);
        }

        @Override
        public ChildReference next() {
            node = childReferenceCompleteV2(++index);
            return node;
        }

        @Override
        public ChildReference prev() {
            node = childReferenceCompleteV2(--index);
            return node;
        }

        @Override
        public void nextInPlace() {
            fillChildReferenceCompleteV2(++index, node);
        }

        @Override
        public void prevInPlace() {
            fillChildReferenceCompleteV2(--index, node);
        }
    }

    /**
     * V2 format sparse children (the number of children is in the range 1..32)
     */
    private final class ImmutableNodeSparseChildrenV2Iterator extends NodeChildrenIteratorBase {
        private ImmutableNodeSparseChildrenV2Iterator(int index, ChildReference node) {
            super(index, node);
        }

        @Override
        public ChildReference next() {
            node = childReferenceSparseV2(++index);
            return node;
        }

        @Override
        public ChildReference prev() {
            node = childReferenceSparseV2(--index);
            return node;
        }

        @Override
        public void nextInPlace() {
            fillChildReferenceSparseV2(++index, node);
        }

        @Override
        public void prevInPlace() {
            fillChildReferenceSparseV2(--index, node);
        }
    }

    // v2 format bitset children (the number of children is in the range 33..255)
    private final class ImmutableNodeBitsetChildrenV2Iterator extends NodeChildrenIteratorBase {
        private final BitSet bitset;
        private int bit = -1;

        private ImmutableNodeBitsetChildrenV2Iterator(int index, ChildReference node) {
            super(index, node);
            final long[] bits = new long[CHILDREN_BITSET_SIZE_LONGS];
            for (int i = 0; i < bits.length; i++) {
                bits[i] = nextLong(i * Long.BYTES, Long.BYTES);
            }

            bitset = BitSet.valueOf(bits);
        }

        @Override
        public ChildReference next() {
            incIndex();
            node = childReferenceBitsetV2(index, bit);
            return node;
        }

        @Override
        public ChildReference prev() {
            decIndex();
            node = childReferenceBitsetV2(index, bit);
            return node;
        }

        @Override
        public void nextInPlace() {
            incIndex();
            fillChildReferenceBitsetV2(index, bit, node);
        }

        @Override
        public void prevInPlace() {
            decIndex();
            fillChildReferenceBitsetV2(index, bit, node);
        }

        private void incIndex() {
            ++index;
            if (bit < 0) {
                for (int i = 0; i <= index; i++) {
                    bit = bitset.nextSetBit(bit + 1);
                }
            } else {
                bit = bitset.nextSetBit(bit + 1);
            }
            if (bit < 0) {
                throw new ExodusException("Inconsistent children bitset in Patricia node");
            }
        }

        private void decIndex() {
            --index;
            if (bit < 0) {
                bit = 256;
                for (int i = index; i < getChildrenCount(); i++) {
                    bit = bitset.previousSetBit(bit - 1);
                }
            } else {
                if (bit == 0) {
                    throw new ExodusException("Inconsistent children bitset in Patricia node");
                }
                bit = bitset.previousSetBit(bit - 1);
            }
            if (bit < 0) {
                throw new ExodusException("Inconsistent children bitset in Patricia node");
            }
        }
    }
}
