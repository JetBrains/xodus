/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.ByteIterableWithAddress;
import jetbrains.exodus.log.ByteIteratorWithAddress;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class ImmutableNode extends NodeBase {

    private static final int CHILDREN_COUNT_TO_TRIGGER_BINARY_SEARCH = 8;
    private static final int LAZY_KEY_VALUE_ITERABLE_MIN_LENGTH = 16;

    private final long address;
    @NotNull
    private final ByteIterableWithAddress data;
    private final int dataOffset;
    private final short childrenCount;
    private final byte childAddressLength;

    ImmutableNode(final long address, final byte type, @NotNull final ByteIterableWithAddress data) {
        this(address, type, data, data.iterator());
    }

    private ImmutableNode(final long address,
                          final byte type,
                          @NotNull final ByteIterableWithAddress data,
                          @NotNull final ByteIteratorWithAddress it) {
        super(extractKey(type, data, it), extractValue(type, data, it));
        this.address = address;
        this.data = data;
        if (PatriciaTreeBase.nodeHasChildren(type)) {
            final int i = CompressedUnsignedLongByteIterable.getInt(it);
            childrenCount = (short) (i >> 3);
            checkAddressLength(childAddressLength = (byte) ((i & 7) + 1));
        } else {
            childrenCount = (short) 0;
            childAddressLength = (byte) 0;
        }
        dataOffset = (int) (it.getAddress() - data.getDataAddress());
    }

    /**
     * Creates empty node for an empty tree.
     */
    ImmutableNode() {
        super(ByteIterable.EMPTY, null);
        address = Loggable.NULL_ADDRESS;
        data = ByteIterableWithAddress.EMPTY;
        dataOffset = 0;
        childrenCount = (short) 0;
        childAddressLength = (byte) 0;
    }

    @Override
    long getAddress() {
        return address;
    }

    @Override
    boolean isMutable() {
        return false;
    }

    @Override
    MutableNode getMutableCopy(@NotNull final PatriciaTreeMutable mutableTree) {
        return mutableTree.mutateNode(this);
    }

    @Override
    @Nullable
    NodeBase getChild(@NotNull final PatriciaTreeBase tree, final byte b) {
        final int key = b & 0xff;
        int low = 0;
        int high = childrenCount - 1;
        while (low + CHILDREN_COUNT_TO_TRIGGER_BINARY_SEARCH - 1 <= high) {
            final int mid = (low + high) >>> 1;
            final ByteIterator it = getDataIterator(mid * (childAddressLength + 1));
            int cmp = (it.next() & 0xff) - key;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return tree.loadNode(it.nextLong(childAddressLength));
            }
        }
        return getChildUsingLinearSearch(tree, key, low, high + 1);
    }

    @Override
    @NotNull
    NodeChildren getChildren() {
        return new NodeChildren() {
            @Override
            public NodeChildrenIterator iterator() {
                return childrenCount == (short) 0 ?
                    new EmptyNodeChildrenIterator() :
                    new ImmutableNodeChildrenIterator(getDataIterator(0), -1, null);
            }
        };
    }

    @Override
    @NotNull
    NodeChildrenIterator getChildren(final byte b) {
        final int key = b & 0xff;
        int low = 0;
        int high = childrenCount - 1;
        while (low + CHILDREN_COUNT_TO_TRIGGER_BINARY_SEARCH - 1 <= high) {
            final int mid = (low + high) >>> 1;
            final ByteIterator it = getDataIterator(mid * (childAddressLength + 1));
            int cmp = (it.next() & 0xff) - key;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                final long suffixAddress = it.nextLong(childAddressLength);
                return new ImmutableNodeChildrenIterator(it, mid, new ChildReference(b, suffixAddress));
            }
        }
        return getChildrenUsingLinearSearch(b, low, high + 1);
    }

    @Override
    @NotNull
    NodeChildrenIterator getChildrenRange(final byte b) {
        final int key = b & 0xff;
        if (childrenCount < CHILDREN_COUNT_TO_TRIGGER_BINARY_SEARCH) {
            // linear search
            final ByteIterator it = getDataIterator(0);
            for (int i = 0; i < childrenCount; ++i) {
                byte actual = it.next();
                int cmp = (actual & 0xff) - key;
                if (cmp > 0) {
                    final long suffixAddress = it.nextLong(childAddressLength);
                    return new ImmutableNodeChildrenIterator(it, i, new ChildReference(actual, suffixAddress));
                }
                it.skip(childAddressLength);
            }
        } else {
            // binary search
            final int childRecordLength = childAddressLength + 1;
            int low = -1;
            int high = childrenCount;
            ByteIterator result = null;
            byte resultByte = (byte) 0;
            while (high - low > 1) {
                int mid = (low + high + 1) >>> 1;
                final ByteIterator it = getDataIterator(mid * childRecordLength);
                byte actual = it.next();
                int cmp = (actual & 0xff) - key;
                if (cmp > 0) {
                    result = it;
                    resultByte = actual;
                    high = mid;
                } else {
                    low = mid;
                }
            }
            if (result != null) {
                final long suffixAddress = result.nextLong(childAddressLength);
                return new ImmutableNodeChildrenIterator(result, high, new ChildReference(resultByte, suffixAddress));
            }
        }
        return new EmptyNodeChildrenIterator();
    }


    @Override
    int getChildrenCount() {
        return childrenCount;
    }

    @NotNull
    @Override
    NodeChildrenIterator getChildrenLast() {
        return new ImmutableNodeChildrenIterator(null, childrenCount, null);
    }

    private ByteIterator getDataIterator(final int offset) {
        return address == Loggable.NULL_ADDRESS ? ByteIterable.EMPTY_ITERATOR : data.iterator(dataOffset + offset);
    }

    @Nullable
    private NodeBase getChildUsingLinearSearch(@NotNull final PatriciaTreeBase tree,
                                               final int key,
                                               final int startIndex, /* inclusively */
                                               final int endIndex    /* exclusively */) {
        final ByteIterator it = getDataIterator(startIndex * (childAddressLength + 1));
        for (int i = startIndex; i < endIndex; ++i) {
            final int cmp = (it.next() & 0xff) - key;
            if (cmp < 0) {
                it.skip(childAddressLength);
                continue;
            }
            if (cmp == 0) {
                return tree.loadNode(it.nextLong(childAddressLength));
            }
            break;
        }
        return null;
    }

    @NotNull
    private NodeChildrenIterator getChildrenUsingLinearSearch(final byte b,
                                                              final int startIndex, /* inclusively */
                                                              final int endIndex    /* exclusively */) {
        final int key = b & 0xff;
        final ByteIterator it = getDataIterator(startIndex * (childAddressLength + 1));
        for (int i = startIndex; i < endIndex; ++i) {
            int cmp = (it.next() & 0xff) - key;
            if (cmp < 0) {
                it.skip(childAddressLength);
                continue;
            }
            if (cmp == 0) {
                final long suffixAddress = it.nextLong(childAddressLength);
                return new ImmutableNodeChildrenIterator(it, i, new ChildReference(b, suffixAddress));
            }
            break;
        }
        return new EmptyNodeChildrenIterator();
    }

    @NotNull
    private static ByteIterable extractKey(final byte type,
                                           @NotNull final ByteIterableWithAddress data,
                                           @NotNull final ByteIteratorWithAddress it) {
        if (!PatriciaTreeBase.nodeHasKey(type)) {
            return ByteIterable.EMPTY;
        }
        return extractLazyIterable(data, it);
    }

    @Nullable
    private static ByteIterable extractValue(final byte type,
                                             @NotNull final ByteIterableWithAddress data,
                                             @NotNull final ByteIteratorWithAddress it) {
        if (!PatriciaTreeBase.nodeHasValue(type)) {
            return null;
        }
        return extractLazyIterable(data, it);
    }

    private static ByteIterable extractLazyIterable(@NotNull final ByteIterableWithAddress data,
                                                    @NotNull final ByteIteratorWithAddress it) {
        final int length = CompressedUnsignedLongByteIterable.getInt(it);
        if (length == 1) {
            return ArrayByteIterable.fromByte(it.next());
        }
        if (length < LAZY_KEY_VALUE_ITERABLE_MIN_LENGTH) {
            return new ArrayByteIterable(it, length);
        }
        final ByteIterable result = data.clone((int) (it.getAddress() - data.getDataAddress())).subIterable(0, length);
        it.skip(length);
        return result;
    }

    private static void checkAddressLength(long addressLen) {
        if (addressLen < 0 || addressLen > 8) {
            throw new ExodusException("Invalid length of address: " + addressLen);
        }
    }

    private final class ImmutableNodeChildrenIterator implements NodeChildrenIterator {

        private ByteIterator itr;
        private int index = 0;
        private ChildReference node;

        private ImmutableNodeChildrenIterator(ByteIterator itr, int index, ChildReference node) {
            this.itr = itr;
            this.index = index;
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return index < childrenCount - 1;
        }

        @Override
        public ChildReference next() {
            ++index;
            return node = new ChildReference(itr.next(), itr.nextLong(childAddressLength));
        }

        @Override
        public boolean hasPrev() {
            return index > 0;
        }

        @Override
        public ChildReference prev() {
            --index;
            itr = getDataIterator(index * (childAddressLength + 1));
            return node = new ChildReference(itr.next(), itr.nextLong(childAddressLength));
        }

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public void nextInPlace() {
            ++index;
            final ChildReference node = this.node;
            node.firstByte = itr.next();
            node.suffixAddress = itr.nextLong(childAddressLength);
        }

        @Override
        public void prevInPlace() {
            --index;
            final ChildReference node = this.node;
            itr = getDataIterator(index * (childAddressLength + 1));
            node.firstByte = itr.next();
            node.suffixAddress = itr.nextLong(childAddressLength);
        }

        @Override
        public ChildReference getNode() {
            return node;
        }

        @Override
        public void remove() {
            throw new ExodusException("Can't remove manually Patricia node child, use Store.delete() instead");
        }

        @Override
        public NodeBase getParentNode() {
            return ImmutableNode.this;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public ByteIterable getKey() {
            return keySequence;
        }
    }
}