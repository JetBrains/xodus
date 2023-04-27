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
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import org.jetbrains.annotations.NotNull;

final class MultiPageByteIterableWithAddress implements ByteIterableWithAddress {

    private final long address;
    @NotNull
    private final Log log;
    private final int length;
    private byte[] bytes = null;


    public MultiPageByteIterableWithAddress(final long address, final int length, @NotNull final Log log) {
        this.address = address;

        this.log = log;
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public byte[] getBytesUnsafe() {
        return doBytesUnsafe();
    }

    private byte[] doBytesUnsafe() {
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
    public int baseOffset() {
        return 0;
    }

    @Override
    public byte[] getBaseBytes() {
        return doBytesUnsafe();
    }

    @Override
    public ByteIteratorWithAddress iterator() {
        return new DataIterator(log, address, length);
    }

    public ByteIteratorWithAddress iterator(final int offset) {
        if (offset == 0) {
            return new DataIterator(log, address, length);
        }

        return new DataIterator(log, log.adjustLoggableAddress(address, offset), length);
    }


    @Override
    public long getDataAddress() {
        return address;
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


    @Override
    public int compareTo(@NotNull ByteIterable right) {
        return compare(0, address, length, right, 0, right.getLength(), log);
    }

    @Override
    public int compareTo(int length, ByteIterable right, int rightLength) {
        return compare(0, address, length, right, 0, rightLength, log);
    }

    @Override
    public int compareTo(int from, int length, ByteIterable right, int rightFrom, int rightLength) {
        return compare(from, address, length, right, rightFrom, rightLength, log);
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

    private static int compare(final int leftOffset, final long leftAddress, final int len,
                               final ByteIterable right, int rightOffset, final int rightLen,
                               final Log log) {
        final int pageSize = log.getCachePageSize();
        final int mask = pageSize - 1;

        long alignedAddress = log.adjustLoggableAddress(leftAddress, leftOffset);
        long endAddress = log.adjustLoggableAddress(alignedAddress, len);

        endAddress -= ((int) endAddress) & mask;

        int leftStep = ((int) alignedAddress) & mask;

        alignedAddress -= leftStep;
        ArrayByteIterable leftPage = log.getPageIterable(alignedAddress);

        final int leftPageLen = leftPage.getLength();
        if (leftPageLen <= leftStep) { // alignment is >= 0 for sure
            BlockNotFoundException.raise(log, alignedAddress);
        }

        byte[] leftBaseArray = leftPage.getBaseBytes();
        int leftBaseOffset = leftPage.baseOffset();

        final byte[] rightBaseArray = right.getBaseBytes();
        final int rightBaseLen = Math.min(right.getLength(), rightLen);
        final int rightBaseOffset = rightOffset + right.baseOffset();

        int rightStep = 0;
        int limit = Math.min(len, Math.min(leftPageLen - leftStep, rightBaseLen));

        while (true) {
            while (rightStep < limit) {
                byte b1 = leftBaseArray[leftBaseOffset + leftStep++];
                byte b2 = rightBaseArray[rightBaseOffset + rightStep++];
                if (b1 != b2) {
                    return (b1 & 0xff) - (b2 & 0xff);
                }
            }
            if (rightStep == rightBaseLen || alignedAddress >= endAddress) {
                return len - rightBaseLen;
            }
            // move left array to next cache page
            alignedAddress += pageSize;
            leftPage = log.getPageIterable(alignedAddress);
            leftBaseArray = leftPage.getBaseBytes();
            leftBaseOffset = leftPage.baseOffset();

            leftStep = 0;
            limit = Math.min(len, Math.min(leftPage.getLength() + rightStep, rightBaseLen));
        }
    }
}
