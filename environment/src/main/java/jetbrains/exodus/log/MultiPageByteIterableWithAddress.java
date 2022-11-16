/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import org.jetbrains.annotations.NotNull;

final class MultiPageByteIterableWithAddress extends ByteIterableWithAddress {

    @NotNull
    private final Log log;
    private final int length;
    private byte[] bytes = null;


    public MultiPageByteIterableWithAddress(final long address, final int length, @NotNull final Log log) {
        super(address);

        this.log = log;
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public byte[] getBytesUnsafe() {
        if (bytes != null) {
            return bytes;
        }

        if (length == 0) {
            bytes = EMPTY_BYTES;
        } else if (length == 1) {
            var iterator = iterator();
            bytes = ByteIterableBase.SINGLE_BYTES[0xFF & iterator.next()];
        } else {
            var iterator = iterator();
            bytes = new byte[length];

            for (int i = 0; i < length; i++) {
                bytes[i] = iterator.next();
            }
        }

        return bytes;
    }


    @Override
    public ByteIteratorWithAddress iterator() {
        return new DataIterator(log, getDataAddress(), length);
    }

    public ByteIteratorWithAddress iterator(final int offset) {
        if (offset == 0) {
            return new DataIterator(log, getDataAddress(), length);
        }

        return new DataIterator(log, log.adjustLoggableAddress(getDataAddress(), offset), length);
    }


    @Override
    public byte byteAt(int offset) {
        return iterator(offset).next();
    }

    @Override
    public long nextLong(int offset, int length) {
        return iterator(offset).nextLong(length);
    }

    @Override
    public int getCompressedUnsignedInt() {
        return CompressedUnsignedLongByteIterable.getInt(this);
    }

    public int compareTo(final int offset, final int len, @NotNull final ByteIterable right) {
        return compare(offset, len, right, log, getDataAddress());
    }

    @Override
    public int compareTo(@NotNull ByteIterable right) {
        return compare(0, length, right, log, getDataAddress());
    }

    @Override
    public ByteIterableWithAddress cloneWithOffset(int offset) {
        if (offset > length) {
            throw new IllegalArgumentException("Provided offset is " + offset +
                    " but maximum allowed offset (length) is " + length);
        }

        var newAddress = log.adjustLoggableAddress(this.address, offset);
        return new MultiPageByteIterableWithAddress(newAddress, length - offset, log);
    }

    @Override
    public ByteIterableWithAddress cloneWithAddressAndLength(long address, int length) {
        return new MultiPageByteIterableWithAddress(address, length, log);
    }

    @Override
    public @NotNull ByteIterable subIterable(int offset, int length) {
        return new LogAwareFixedLengthByteIterable(this, offset, length);
    }

    private static int compare(final int offset, final int len, final ByteIterable right, Log log, final long address) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        final boolean formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();
        final int mask = pageSize - 1;

        long alignedAddress = log.adjustLoggableAddress(address, offset);
        long endAddress = log.adjustLoggableAddress(alignedAddress, len);

        endAddress -= ((int) endAddress) & mask;

        int leftStep = ((int) alignedAddress) & mask;

        alignedAddress -= leftStep;
        ArrayByteIterable left = cache.getPageIterable(log, alignedAddress, formatWithHashCodeIsUsed);

        final int leftLen = left.getLength();
        if (leftLen <= leftStep) { // alignment is >= 0 for sure
            BlockNotFoundException.raise(log, alignedAddress);
        }
        byte[] leftArray = left.getBytesUnsafe();
        final byte[] rightArray = right.getBytesUnsafe();
        final int rightLen = right.getLength();
        int rightStep = 0;
        int limit = Math.min(len, Math.min(leftLen - leftStep, rightLen));

        while (true) {
            while (rightStep < limit) {
                byte b1 = leftArray[leftStep++];
                byte b2 = rightArray[rightStep++];
                if (b1 != b2) {
                    return (b1 & 0xff) - (b2 & 0xff);
                }
            }
            if (rightStep == rightLen || alignedAddress >= endAddress) {
                return len - rightLen;
            }
            // move left array to next cache page
            left = cache.getPageIterable(log, alignedAddress += pageSize, formatWithHashCodeIsUsed);
            leftArray = left.getBytesUnsafe();
            leftStep = 0;
            limit = Math.min(len, Math.min(left.getLength() + rightStep, rightLen));
        }
    }
}
