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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

final class ArrayByteIterableWithAddress extends ByteIterableWithAddress {

    private final byte @NotNull [] bytes;
    private final int start;
    private final int end;

    ArrayByteIterableWithAddress(final long address, final byte @NotNull [] bytes, final int start, final int length) {
        super(address);
        this.bytes = bytes;
        this.start = start;
        this.end = Math.min(start + length, bytes.length);
    }

    @Override
    public byte byteAt(final int offset) {
        return bytes[start + offset];
    }

    @Override
    public long nextLong(final int offset, final int length) {
        return LongBinding.entryToUnsignedLong(bytes, start + offset, length);
    }

    @Override
    public int getCompressedUnsignedInt() {
        int result = 0;
        int shift = 0;
        for (int i = start; ; ++i) {
            final byte b = bytes[i];
            result += (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        }
    }

    @Override
    public int compareTo(@NotNull ByteIterable right) {
        return compareTo(0, getLength(), right);
    }

    @Override
    public ArrayByteIteratorWithAddress iterator() {
        return iterator(0);
    }

    @Override
    public ArrayByteIteratorWithAddress iterator(final int offset) {
        return new ArrayByteIteratorWithAddress(offset);
    }

    @Override
    public int compareTo(final int offset, final int len, @NotNull final ByteIterable right) {
        if (right instanceof ArrayByteIterableWithAddress) {
            final ArrayByteIterableWithAddress r = (ArrayByteIterableWithAddress) right;
            return Arrays.compareUnsigned(bytes, start + offset,
                    start + offset + len,
                    r.bytes, r.start, r.end);
        }

        var rightStart = right.baseOffset();
        var rightLen = right.getLength();

        return Arrays.compareUnsigned(bytes, start + offset,
                start + offset + len, right.getBaseBytes(), rightStart,
                rightStart + rightLen);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof ByteIterable &&
                compareTo(0, getLength(), (ByteIterable) obj) == 0;
    }

    /**
     * @return hash code computed using all bytes of the iterable.
     */
    @Override
    public int hashCode() {
        int result = 1;
        for (int i = start; i < end; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }

    @Override
    public ArrayByteIterableWithAddress cloneWithOffset(int offset) {
        return new ArrayByteIterableWithAddress(address + offset, bytes,
                start + offset, end - start - offset);
    }

    @Override
    public ArrayByteIterableWithAddress cloneWithAddressAndLength(long address, int length) {
        final int offset = (int) (address - this.address);
        return new ArrayByteIterableWithAddress(address, bytes,
                start + offset, length);
    }

    @Override
    public int getLength() {
        return end - start;
    }

    @Override
    public byte[] getBytesUnsafe() {
        if (start == 0) {
            return bytes;
        }

        var len = getLength();
        var result = new byte[len];

        System.arraycopy(bytes, start, result, 0, len);
        return result;
    }

    @NotNull
    @Override
    public ByteIterableWithAddress subIterable(final int offset, final int length) {
        final int adjustedLen = Math.min(length, Math.max(getLength() - offset, 0));
        return adjustedLen == 0 ? ByteIterableWithAddress.EMPTY :
                new ArrayByteIterableWithAddress(address + offset, bytes,
                        start + offset, adjustedLen);
    }

    @Override
    public String toString() {
        return ByteIterableBase.toString(bytes, start, end);
    }

//    @Override
//    public int baseOffset() {
//        return start;
//    }
//
//    @Override
//    public byte[] getBaseBytes() {
//        return bytes;
//    }

    private final class ArrayByteIteratorWithAddress extends ByteIteratorWithAddress {

        private int i;

        ArrayByteIteratorWithAddress(final int offset) {
            i = start + offset;
        }

        @Override
        public int available() {
            return end - i;
        }

        @Override
        public long getAddress() {
            return ArrayByteIterableWithAddress.this.getDataAddress() + i - start;
        }

        @Override
        public boolean hasNext() {
            return i < end;
        }

        @Override
        public byte next() {
            return bytes[i++];
        }

        @Override
        public long skip(final long bytes) {
            final int skipped = Math.min(end - i, (int) bytes);
            i += skipped;
            return skipped;
        }

        @Override
        public int getOffset() {
            return i;
        }

        @Override
        public long nextLong(final int length) {
            final long result = LongBinding.entryToUnsignedLong(bytes, i, length);
            i += length;
            return result;
        }
    }
}
