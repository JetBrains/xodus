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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.CompoundByteIteratorBase;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;

abstract class BasePageImmutable extends BasePage {

    @NotNull
    protected final ByteIterableWithAddress data;
    private long dataAddress;
    byte keyAddressLen;

    /**
     * Create empty page
     *
     * @param tree tree which the page belongs to
     */
    BasePageImmutable(@NotNull BTreeBase tree) {
        super(tree);
        data = ByteIterableWithAddress.EMPTY;
        size = 0;
        dataAddress = Loggable.NULL_ADDRESS;
    }

    /**
     * Create page and load size and key address length
     *
     * @param tree tree which the page belongs to
     * @param data binary data to load the page from.
     */
    BasePageImmutable(@NotNull BTreeBase tree, @NotNull final ByteIterableWithAddress data) {
        super(tree);
        this.data = data;
        final ByteIteratorWithAddress it = data.iterator();
        size = CompressedUnsignedLongByteIterable.getInt(it) >> 1;
        init(it);
    }

    /**
     * Create page of specified size and load key address length
     *
     * @param tree tree which the page belongs to
     * @param data source iterator
     * @param size computed size
     */
    BasePageImmutable(@NotNull BTreeBase tree, @NotNull final ByteIterableWithAddress data, int size) {
        super(tree);
        this.data = data;
        this.size = size;
        init(data.iterator());
    }

    private void init(@NotNull final ByteIteratorWithAddress itr) {
        if (size > 0) {
            final int next = itr.next();
            dataAddress = itr.getAddress();
            loadAddressLengths(next);
        } else {
            dataAddress = itr.getAddress();
        }
    }

    @Override
    protected long getDataAddress() {
        return dataAddress;
    }

    ByteIterator getDataIterator(final int offset) {
        return dataAddress == Loggable.NULL_ADDRESS ?
            ByteIterable.EMPTY_ITERATOR : data.iterator((int) (dataAddress - data.getDataAddress() + offset));
    }

    protected void loadAddressLengths(final int length) {
        checkAddressLength(keyAddressLen = (byte) length);
    }

    static void checkAddressLength(byte addressLen) {
        if (addressLen < 0 || addressLen > 8) {
            throw new ExodusException("Invalid length of address: " + addressLen);
        }
    }

    @Override
    protected long getKeyAddress(final int index) {
        return getDataIterator(index * keyAddressLen).nextLong(keyAddressLen);
    }

    @Override
    @NotNull
    public BaseLeafNode getKey(final int index) {
        return getTree().loadLeaf(getKeyAddress(index));
    }

    @Override
    protected boolean isMutable() {
        return false;
    }

    @Override
    protected SearchRes binarySearch(final ByteIterable key) {
        return binarySearch(key, 0);
    }

    @Override
    protected SearchRes binarySearch(final ByteIterable key, final int low) {
        if (dataAddress == Loggable.NULL_ADDRESS) {
            return SearchRes.NOT_FOUND;
        }
        final BTreeBase tree = getTree();
        final SearchRes result = tree.getSearchRes();
        binarySearch(tree, result, key, low, size - 1, keyAddressLen, dataAddress);
        return result;
    }

    static void doReclaim(BTreeReclaimTraverser context) {
        final BasePageMutable node = context.currentNode.getMutableCopy(context.mainTree);
        context.wasReclaim = true;
        context.setPage(node);
        context.popAndMutate();
    }

    private static void binarySearch(@NotNull final BTreeBase tree,
                                     @NotNull final SearchRes result,
                                     @NotNull final ByteIterable key,
                                     int low, int high,
                                     final int bytesPerLong,
                                     long startAddress) {
        final Log log = tree.log;
        final int pageSize = log.getCachePageSize();
        final int mask = pageSize - 1; // bytes size is always a power of 2
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;
        final BinarySearchIterator it = new BinarySearchIterator(pageSize);

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midAddress = startAddress + (mid * bytesPerLong);
            it.offset = ((int) midAddress) & mask;
            it.address = midAddress - it.offset;
            boolean loaded = false;
            if (it.address == leftAddress) {
                it.page = leftPage;
            } else if (it.address == rightAddress) {
                it.page = rightPage;
            } else {
                it.page = log.getCachedPage(it.address);
                loaded = true;
            }

            final long address = it.address;
            final byte[] page = it.page;
            final int cmp;

            if (pageSize - it.offset < bytesPerLong) {
                final long nextAddress = address + pageSize;
                if (rightAddress == nextAddress) {
                    it.nextPage = rightPage;
                } else {
                    it.nextPage = log.getCachedPage(nextAddress);
                    loaded = true;
                }
                cmp = (result.key = tree.loadLeaf(it.asCompound().nextLong(bytesPerLong))).compareKeyTo(key);
            } else {
                cmp = (result.key = tree.loadLeaf(it.nextLong(bytesPerLong))).compareKeyTo(key);
            }

            if (cmp < 0) {
                low = mid + 1;
                if (loaded) {
                    leftAddress = address;
                    leftPage = page;
                }
            } else if (cmp > 0) {
                high = mid - 1;
                if (loaded) {
                    rightAddress = address;
                    rightPage = page;
                }
            } else {
                // key found
                result.index = mid;
                return;
            }
        }
        // key not found
        result.index = -(low + 1);
        result.key = null;
    }

    private static class BinarySearchIterator extends ByteIterator {

        private byte[] page;
        private byte[] nextPage;
        private int offset;
        private long address;
        private final int pageSize;

        private BinarySearchIterator(int pageSize) {
            this.pageSize = pageSize;
        }

        private CompoundByteIteratorBase asCompound() {
            return new CompoundByteIteratorBase(this) {
                @Override
                protected ByteIterator nextIterator() {
                    page = nextPage;
                    address += pageSize;
                    offset = 0;
                    return BinarySearchIterator.this;
                }
            };
        }

        @Override
        public boolean hasNext() {
            return offset < pageSize;
        }

        @Override
        public byte next() {
            return page[offset++];
        }

        @Override
        public long skip(long bytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long nextLong(final int length) {
            final long result = LongBinding.entryToUnsignedLong(page, offset, length);
            offset += length;
            return result;
        }
    }
}
