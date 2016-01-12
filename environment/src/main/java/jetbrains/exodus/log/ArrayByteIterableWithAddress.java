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
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;

class ArrayByteIterableWithAddress extends ByteIterableWithAddress {

    @NotNull
    private final byte[] bytes;
    private final int start;
    private final int end;

    public ArrayByteIterableWithAddress(final long address, @NotNull final byte[] bytes, final int start, final int length) {
        super(address);
        this.bytes = bytes;
        this.start = start;
        this.end = Math.min(start + length, bytes.length);
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
        final int absoluteOffset = start + offset;
        return ByteIterableUtil.compare(bytes, absoluteOffset + len, absoluteOffset, right.getBytesUnsafe(), right.getLength());
    }

    @Override
    public ByteIterableWithAddress clone(final int offset) {
        return new ArrayByteIterableWithAddress(getDataAddress() + offset, bytes, start + offset, end - start - offset);
    }

    @Override
    public int getLength() {
        return end - start;
    }

    @Override
    public String toString() {
        return ByteIterableBase.toString(bytes, start, end);
    }

    private class ArrayByteIteratorWithAddress implements ByteIteratorWithAddress {

        private int i;

        public ArrayByteIteratorWithAddress(final int offset) {
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
        public long skip(final long length) {
            final int skipped = Math.min(end - i, (int) length);
            i += skipped;
            return skipped;
        }
    }
}
