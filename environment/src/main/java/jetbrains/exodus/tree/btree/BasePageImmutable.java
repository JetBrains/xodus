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
    long dataAddress;
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
        return dataAddress == Loggable.NULL_ADDRESS ? Loggable.NULL_ADDRESS :
            data.nextLong((int) (dataAddress - data.getDataAddress() + index * keyAddressLen), keyAddressLen);
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
    protected int binarySearch(final ByteIterable key) {
        return binarySearch(key, 0);
    }

    @Override
    protected int binarySearch(final ByteIterable key, int low) {
        if (dataAddress == Loggable.NULL_ADDRESS) {
            return -1;
        }

        final Log log = tree.log;
        final int cachePageSize = log.getCachePageSize();
        final int bytesPerAddress = keyAddressLen;
        int high = size - 1;
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;
        final BinarySearchIterator it = new BinarySearchIterator();

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midAddress = dataAddress + (mid * bytesPerAddress);
            final int offset;
            it.offset = offset = ((int) midAddress) & (cachePageSize - 1); // cache page size is always a power of 2
            final long pageAddress = midAddress - offset;
            if (pageAddress == leftAddress) {
                it.page = leftPage;
            } else if (pageAddress == rightAddress) {
                it.page = rightPage;
            } else {
                it.page = leftPage = log.getCachedPage(pageAddress);
                leftAddress = pageAddress;
            }

            final long leafAddress;
            if (cachePageSize - offset < bytesPerAddress) {
                final long nextPageAddress = pageAddress + cachePageSize;
                if (rightAddress == nextPageAddress) {
                    it.nextPage = rightPage;
                } else {
                    it.nextPage = rightPage = log.getCachedPage(nextPageAddress);
                    rightAddress = nextPageAddress;
                }
                leafAddress = it.asCompound().nextLong(bytesPerAddress);
            } else {
                leafAddress = it.nextLong(bytesPerAddress);
            }

            final int cmp = tree.compareLeafToKey(leafAddress, key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                // key found
                return mid;
            }
        }
        // key not found
        return -(low + 1);
    }

    private static class BinarySearchIterator extends ByteIterator {

        private byte[] page;
        private byte[] nextPage;
        private int offset;

        private CompoundByteIteratorBase asCompound() {
            return new CompoundByteIteratorBase(this) {
                @Override
                protected ByteIterator nextIterator() {
                    page = nextPage;
                    offset = 0;
                    return BinarySearchIterator.this;
                }
            };
        }

        @Override
        public boolean hasNext() {
            return offset < page.length;
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
            return LongBinding.entryToUnsignedLong(page, offset, length);
        }
    }

    static void doReclaim(BTreeReclaimTraverser context) {
        final BasePageMutable node = context.currentNode.getMutableCopy(context.mainTree);
        context.wasReclaim = true;
        context.setPage(node);
        context.popAndMutate();
    }
}
