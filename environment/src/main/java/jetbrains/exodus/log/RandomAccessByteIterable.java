/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.log.iterate.CompoundByteIteratorBase;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"PublicMethodNotExposedInInterface", "CovariantCompareTo"})
public class RandomAccessByteIterable implements ByteIterable {

    public static final RandomAccessByteIterable EMPTY = new RandomAccessByteIterable(0, null) {
        @Override
        public ByteIteratorWithAddress iterator() {
            return ByteIteratorWithAddress.EMPTY;
        }

        @Override
        public ByteIteratorWithAddress iterator(long offset) {
            return ByteIteratorWithAddress.EMPTY;
        }

    };

    private final Log log;
    private final long address;

    public RandomAccessByteIterable(long address, Log log) {
        this.address = address;
        this.log = log;
    }

    @Override
    public ByteIteratorWithAddress iterator() {
        return new CompoundByteIterator(address, log);
    }

    public ByteIteratorWithAddress iterator(long offset) {
        return new CompoundByteIterator(address + offset, log);
    }

    @Override
    public byte[] getBytesUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(ByteIterable right) {
        // can't compare  
        throw new UnsupportedOperationException();
    }

    public Log getLog() {
        return log;
    }

    public long getAddress() {
        return address;
    }

    public static int compare(final int offset, final int myLen, final ByteIterable right, Log log, long address) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        long alignedAddress = address + offset;
        long endAddress = alignedAddress + myLen;
        endAddress -= endAddress % pageSize;
        int leftStep = (int) (alignedAddress % pageSize);
        alignedAddress -= leftStep;
        ArrayByteIterable left = cache.getPage(log, alignedAddress);

        int leftLen = left.getLength();
        if (leftLen <= leftStep) { // alignment is >= 0 for sure
            throw new BlockNotFoundException(alignedAddress);
        }
        byte[] leftArray = left.getBytesUnsafe();
        byte[] rightArray = right.getBytesUnsafe();
        final int rightLen = right.getLength();
        int rightStep = 0;

        while (true) {
            int limit = Math.min(myLen, Math.min(leftLen - leftStep, rightLen));
            while (rightStep < limit) {
                byte b1 = leftArray[leftStep++];
                byte b2 = rightArray[rightStep++];
                if (b1 != b2) {
                    return (b1 & 0xff) - (b2 & 0xff);
                }
            }
            if (rightStep == rightLen || alignedAddress >= endAddress) {
                return myLen - rightLen;
            }
            // move left array to next cache page
            left = cache.getPage(log, alignedAddress += pageSize);
            leftArray = left.getBytesUnsafe();
            leftLen = left.getLength();
            leftStep = 0;
        }
    }

    @NotNull
    public RandomAccessByteIterable clone(long offset) {
        return new RandomAccessByteIterable(address + offset, log);
    }

    @SuppressWarnings({"OverlyLongMethod", "MethodWithTooManyParameters"})
    public static int binarySearch(@NotNull final IByteIterableComparator comparator,
                                   final ByteIterable key,
                                   int low, int high,
                                   final int bytesPerLong,
                                   Log log, long startAddress) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        long leftAddress = -1L;
        byte[] leftPage = null;
        long rightAddress = -1L;
        byte[] rightPage = null;
        final BinarySearchIterator it = new BinarySearchIterator(pageSize);

        while (low <= high) {
            final int mid = (low + high + 1) >>> 1;
            final long midAddress = startAddress + (long) (mid * bytesPerLong);
            it.offset = (int) (midAddress % (long) pageSize);
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

        // for testing
        /* private ArrayByteIterable loadPage(Log log, LogCache cache, long address) {
            if (loaded.contains(address)) {
                throw new IllegalStateException("Already loaded!");
            }
            loaded.add(address);
            return cache.getPage(log, address);
        }*/

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
