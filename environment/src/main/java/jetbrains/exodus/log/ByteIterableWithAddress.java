/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.log;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

public abstract class ByteIterableWithAddress implements ByteIterable {

    public static final ByteIterableWithAddress EMPTY = getEmpty(Loggable.NULL_ADDRESS);

    private final long address;

    protected ByteIterableWithAddress(final long address) {
        this.address = address;
    }

    public final long getDataAddress() {
        return address;
    }

    @Override
    public abstract ByteIteratorWithAddress iterator();

    public abstract ByteIteratorWithAddress iterator(final int offset);

    public abstract int compareTo(final int offset, final int len, @NotNull final ByteIterable right);

    public abstract ByteIterableWithAddress clone(final int offset);

    @Override
    public byte[] getBytesUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        return new LogAwareFixedLengthByteIterable(this, offset, length);
    }

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        // can't compare
        throw new UnsupportedOperationException();
    }

    public static ByteIterableWithAddress getEmpty(final long address) {
        return new ArrayByteIterableWithAddress(address, ByteIterable.EMPTY_BYTES, 0, 0);
    }

    public static int binarySearch(@NotNull final IByteIterableComparator comparator,
                                   final ByteIterable key,
                                   int low, int high,
                                   final int bytesPerLong,
                                   Log log, long startAddress) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        final int mask = pageSize - 1; // page size is always a power of 2
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;
        final BinarySearchIterator it = new BinarySearchIterator(pageSize);

        while (low <= high) {
            final int mid = (low + high + 1) >>> 1;
            final long midAddress = startAddress + (mid * bytesPerLong);
            it.offset = ((int) midAddress) & mask;
            it.address = midAddress - it.offset;
            boolean loaded = false;
            if (it.address == leftAddress) {
                it.page = leftPage;
            } else if (it.address == rightAddress) {
                it.page = rightPage;
            } else {
                it.page = cache.getPage(log, it.address).getBytesUnsafe();
                loaded = true;
            }

            final int cmp;
            final long address;
            final byte[] page;

            if (pageSize - it.offset < bytesPerLong) {
                final long nextAddress = (address = it.address) + pageSize;
                if (rightAddress == nextAddress) {
                    it.nextPage = rightPage;
                } else {
                    it.nextPage = cache.getPage(log, nextAddress).getBytesUnsafe();
                    loaded = true;
                }
                page = it.page;
                cmp = comparator.compare(LongBinding.entryToUnsignedLong(it.asCompound(), bytesPerLong), key);
            } else {
                cmp = comparator.compare(LongBinding.entryToUnsignedLong(it, bytesPerLong), key);
                page = it.page;
                address = it.address;
            }

            if (cmp < 0) {
                //left < right
                low = mid + 1;
                if (loaded) {
                    leftAddress = it.address;
                    leftPage = it.page;
                }
            } else if (cmp > 0) {
                //left > right
                high = mid - 1;
                if (loaded) {
                    rightAddress = address;
                    rightPage = page;
                }
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    private static class BinarySearchIterator implements ByteIterator {
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
        public long skip(long length) {
            throw new UnsupportedOperationException();
        }
    }
}
