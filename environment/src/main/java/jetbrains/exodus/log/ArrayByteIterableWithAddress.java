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
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

final class ArrayByteIterableWithAddress extends ArrayByteIterable implements ByteIterableWithAddress {

    private final long address;

    ArrayByteIterableWithAddress(final long address, final byte @NotNull [] bytes,
                                 final int start, final int length) {
        super(bytes, start, length);
        this.address = address;
    }

    @Override
    public long getDataAddress() {
        return address;
    }

    @Override
    public long nextLong(final int offset, final int length) {
        final int start = this.offset + offset;
        final int end = start + length;

        long result = 0;
        for (int i = start; i < end; ++i) {
            result = (result << 8) + ((int) bytes[i] & 0xff);
        }

        return result;
    }

    @Override
    public int getCompressedUnsignedInt() {
        int result = 0;
        int shift = 0;
        for (int i = offset; ; ++i) {
            final byte b = bytes[i];
            result += (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        }
    }

    @Override
    public ArrayByteIteratorWithAddress iterator() {
        return new ArrayByteIteratorWithAddress(bytes, this.offset, length);
    }

    @Override
    public ArrayByteIteratorWithAddress iterator(final int offset) {
        return new ArrayByteIteratorWithAddress(bytes,
                this.offset + offset, length - offset);
    }


    @Override
    public ArrayByteIterableWithAddress cloneWithOffset(int offset) {
        return new ArrayByteIterableWithAddress(address + offset, bytes,
                this.offset + offset, length - offset);
    }

    @Override
    public ArrayByteIterableWithAddress cloneWithAddressAndLength(long address, int length) {
        final int offset = (int) (address - this.address);
        return new ArrayByteIterableWithAddress(address, bytes,
                this.offset + offset, length);
    }

    private final class ArrayByteIteratorWithAddress extends Iterator implements ByteIteratorWithAddress {
        ArrayByteIteratorWithAddress(final byte[] bytes, final int offset, final int length) {
            super(bytes, offset, length);
        }

        @Override
        public int available() {
            return end - offset;
        }

        @Override
        public long getAddress() {
            return ArrayByteIterableWithAddress.this.address + offset - ArrayByteIterableWithAddress.this.offset;
        }

        @Override
        public int getOffset() {
            return offset;
        }

        @Override
        public long nextLong(final int length) {
            final long result = LongBinding.entryToUnsignedLong(bytes, offset, length);
            offset += length;
            return result;
        }

        @Override
        public int getCompressedUnsignedInt() {
            if (offset == end) {
                throw new NoSuchElementException();
            }

            int result = 0;
            int shift = 0;
            do {
                final byte b = bytes[offset++];
                result += (b & 0x7f) << shift;
                if ((b & 0x80) != 0) {
                    return result;
                }
                shift += 7;
            } while (offset < end);

            throw new NoSuchElementException();
        }

        @Override
        public long getCompressedUnsignedLong() {
            if (offset == end) {
                throw new NoSuchElementException();
            }

            long result = 0;
            int shift = 0;
            do {
                final byte b = bytes[offset++];

                result += (long) (b & 0x7f) << shift;
                if ((b & 0x80) != 0) {
                    return result;
                }

                shift += 7;
            } while (offset < end);

            throw new NoSuchElementException();
        }
    }


}
