/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;

abstract class BasePageImmutable extends BasePage {

    @NotNull
    protected final ByteIterableWithAddress data;
    byte keyAddressLen;
    private ILeafNode minKey = null;
    private ILeafNode maxKey = null;
    protected final Log log;

    protected final byte[] page;
    private final int dataOffset;

    private final boolean formatWithHashCodeIsUsed;


    /**
     * Create empty page
     *
     * @param tree tree which the page belongs to
     */
    BasePageImmutable(@NotNull BTreeBase tree) {
        super(tree);
        data = ByteIterableWithAddress.EMPTY;

        size = 0;
        log = tree.log;
        formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();

        dataOffset = 0;
        page = ArrayByteIterable.EMPTY_BYTES;
    }

    /**
     * Create page and load size and key address length
     *
     * @param tree tree which the page belongs to
     * @param data binary data to load the page from.
     */
    BasePageImmutable(@NotNull BTreeBase tree, @NotNull final ByteIterableWithAddress data,
                      final boolean loggableInsideSinglePage) {
        super(tree);
        log = tree.log;

        final ByteIteratorWithAddress it = data.iterator();
        size = it.getCompressedUnsignedInt() >> 1;
        this.data = init(data, it);

        formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();

        if (loggableInsideSinglePage) {
            page = this.data.getBaseBytes();
            dataOffset = this.data.baseOffset();
        } else {
            page = null;
            dataOffset = -1;
        }
    }

    /**
     * Create page of specified size and load key address length
     *
     * @param tree tree which the page belongs to
     * @param data source iterator
     * @param size computed size
     */
    BasePageImmutable(@NotNull BTreeBase tree,
                      @NotNull final ByteIterableWithAddress data, int size,
                      final boolean loggableInsideSinglePage) {
        super(tree);
        log = tree.log;

        this.size = size;
        var it = data.iterator();

        this.data = init(data, it);

        formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();

        if (loggableInsideSinglePage) {
            page = this.data.getBaseBytes();
            dataOffset = this.data.baseOffset();
        } else {
            page = null;
            dataOffset = -1;
        }
    }

    private ByteIterableWithAddress init(final ByteIterableWithAddress data, @NotNull final ByteIteratorWithAddress itr) {
        ByteIterableWithAddress result;
        if (size > 0) {
            final int next = itr.next();
            result = data.cloneWithAddressAndLength(itr.getAddress(), itr.available());
            loadAddressLengths(next, itr);
        } else {
            result = data.cloneWithAddressAndLength(itr.getAddress(), itr.available());
        }

        return result;
    }

    @Override
    @NotNull ILeafNode getMinKey() {
        if (minKey != null) return minKey;
        return minKey = super.getMinKey();
    }

    @Override
    @NotNull ILeafNode getMaxKey() {
        if (maxKey != null) return maxKey;
        return maxKey = super.getMaxKey();
    }

    @Override
    protected long getDataAddress() {
        return data.getDataAddress();
    }

    ByteIterator getDataIterator() {
        return data.getDataAddress() == Loggable.NULL_ADDRESS ?
                ByteIterable.EMPTY_ITERATOR : data.iterator();
    }

    protected void loadAddressLengths(final int length, final ByteIterator it) {
        checkAddressLength(keyAddressLen = (byte) length);
    }

    static void checkAddressLength(byte addressLen) {
        if (addressLen < 0 || addressLen > 8) {
            throw new ExodusException("Invalid length of address: " + addressLen);
        }
    }

    @Override
    protected long getKeyAddress(final int index) {
        if (getDataAddress() == Loggable.NULL_ADDRESS) {
            return Loggable.NULL_ADDRESS;
        }

        if (page != null) {
            return getLong(index * keyAddressLen, keyAddressLen);
        }

        return data.nextLong(index * keyAddressLen, keyAddressLen);
    }

    protected final long getLong(int offset, int length) {
        offset += dataOffset;

        long result = 0;
        for (int i = 0; i < length; ++i) {
            result = (result << 8) + ((int) page[offset + i] & 0xff);
        }

        return result;
    }

    @Override
    @NotNull
    public BaseLeafNode getKey(final int index) {
        return getTree().loadLeaf(getKeyAddress(index));
    }

    @Override
    protected boolean isDupKey(int index) {
        return getTree().isDupKey(getKeyAddress(index));
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
    protected int binarySearch(final ByteIterable key, final int low, final long expectedAddress) {
        return binarySearch(key, low);
    }

    @Override
    protected int binarySearch(final ByteIterable key, int low) {
        if (getDataAddress() == Loggable.NULL_ADDRESS) {
            return -1;
        }

        if (page != null) {
            return singePageBinarySearch(key, low);
        }

        if (formatWithHashCodeIsUsed) {
            return multiPageBinarySearch(key, low);
        } else {
            return compatibleBinarySearch(key, low);
        }
    }

    private int multiPageBinarySearch(final ByteIterable key, int low) {
        final int cachePageSize = log.getCachePageSize();
        final int bytesPerAddress = keyAddressLen;

        final long dataAddress = getDataAddress();
        int high = size - 1;
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;

        final int adjustedPageSize = log.getCachePageSize() - BufferedDataWriter.LOGGABLE_DATA;
        final BinarySearchIterator it = new BinarySearchIterator(adjustedPageSize);

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midAddress = log.adjustLoggableAddress(dataAddress, (((long) mid) * bytesPerAddress));

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
            if (adjustedPageSize - offset < bytesPerAddress) {
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

    private int singePageBinarySearch(final ByteIterable key, int low) {
        int high = size - 1;
        final int bytesPerAddress = keyAddressLen;

        while (low <= high) {
            final int mid = (low + high) >>> 1;

            final int offset = mid * bytesPerAddress;
            final long leafAddress = getLong(offset, bytesPerAddress);

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

    private int compatibleBinarySearch(final ByteIterable key, int low) {
        final int cachePageSize = log.getCachePageSize();
        final int bytesPerAddress = keyAddressLen;

        final long dataAddress = this.getDataAddress();
        int high = size - 1;
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;

        while (low <= high) {
            final int mid = (low + high) >>> 1;

            final long midAddress =
                    log.adjustLoggableAddress(dataAddress, (((long) mid) * bytesPerAddress));


            final int offset;

            final int adjustedPageSize;
            if (formatWithHashCodeIsUsed) {
                adjustedPageSize = cachePageSize - BufferedDataWriter.LOGGABLE_DATA;
            } else {
                adjustedPageSize = cachePageSize;
            }

            final BinarySearchIterator it = new BinarySearchIterator(adjustedPageSize);
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
            if (adjustedPageSize - offset < bytesPerAddress) {
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

    private static class BinarySearchIterator implements ByteIterator {

        private byte[] page;
        private byte[] nextPage;
        private int offset;
        private final int adjustedPageSize;

        private BinarySearchIterator(final int adjustedPageSize) {
            this.adjustedPageSize = adjustedPageSize;
        }

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
            return offset < adjustedPageSize;
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
