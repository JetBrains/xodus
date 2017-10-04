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
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

class ArrayByteIterableWithAddress extends ByteIterableWithAddress {

    @NotNull
    private final byte[] bytes;
    private final int start;
    private final int end;

    ArrayByteIterableWithAddress(final long address, @NotNull final byte[] bytes, final int start, final int length) {
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
    public ByteIteratorWithAddress iterator() {
        return iterator(0);
    }

    @Override
    public ByteIteratorWithAddress iterator(final int offset) {
        return new ArrayByteIteratorWithAddress(offset);
    }

    @Override
    public int compareTo(final int offset, final int len, @NotNull final ByteIterable right) {
        if (right instanceof SubIterable) {
            final SubIterable r = (SubIterable) right;
            return ByteIterableUtil.compare(bytes, len, start + offset, r.getRawBytes(), r.getLength(), r.offset);
        }
        return ByteIterableUtil.compare(bytes, len, start + offset, right.getBytesUnsafe(), right.getLength());
    }

    @Override
    public ByteIterableWithAddress clone(final int offset) {
        return new ArrayByteIterableWithAddress(getDataAddress() + offset, bytes, start + offset, end - start - offset);
    }

    @Override
    public int getLength() {
        return end - start;
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        final int adjustedLen = Math.min(length, Math.max(getLength() - offset, 0));
        return adjustedLen == 0 ? ArrayByteIterable.EMPTY : new SubIterable(bytes, start + offset, adjustedLen);
    }

    @Override
    public String toString() {
        return ByteIterableBase.toString(bytes, start, end);
    }

    private class ArrayByteIteratorWithAddress extends ByteIteratorWithAddress {

        private int i;

        ArrayByteIteratorWithAddress(final int offset) {
            i = start + offset;
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
        public long nextLong(final int length) {
            final long result = LongBinding.entryToUnsignedLong(bytes, i, length);
            i += length;
            return result;
        }
    }

    private static class SubIterable extends ByteIterableBase {

        private int offset;

        SubIterable(@NotNull final byte[] bytes, final int offset, final int length) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int compareTo(@NotNull final ByteIterable right) {
            if (right instanceof SubIterable) {
                final SubIterable r = (SubIterable) right;
                return ByteIterableUtil.compare(bytes, length, offset, r.bytes, r.length, r.offset);
            }
            return ByteIterableUtil.compare(bytes, length, offset, right.getBytesUnsafe(), right.getLength());
        }

        @Override
        public ByteIterator iterator() {
            return getIterator();
        }

        @Override
        protected ByteIterator getIterator() {
            return new ByteIterator() {

                int i = offset;

                @Override
                public boolean hasNext() {
                    return length > i - offset;
                }

                @Override
                public byte next() {
                    return bytes[i++];
                }

                @Override
                public long skip(long bytes) {
                    final int result = Math.min(length - i + offset, (int) bytes);
                    i += result;
                    return result;
                }
            };
        }

        @Override
        public byte[] getBytesUnsafe() {
            if (offset > 0) {
                bytes = Arrays.copyOfRange(bytes, offset, offset + length);
                offset = 0;
            }
            return bytes;
        }

        private byte[] getRawBytes() {
            return bytes;
        }
    }
}
