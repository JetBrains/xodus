/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus;

import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 * An adapter of byte array to {@link ByteIterable}.
 */
public class ArrayByteIterable extends ByteIterableBase {

    public static final EmptyIterable EMPTY = new EmptyIterable();

    private static final ArrayByteIterable SINGLE_BYTE_ITERABLES[];

    static {
        SINGLE_BYTE_ITERABLES = new ArrayByteIterable[256];
        for (int i = 0; i < SINGLE_BYTE_ITERABLES.length; i++) {
            SINGLE_BYTE_ITERABLES[i] = new ArrayByteIterable(SINGLE_BYTES[i]);
        }
    }

    public ArrayByteIterable(@NotNull final ByteIterable bi) {
        final int length = bi.getLength();
        if (length == 0) {
            fillBytes(EMPTY_ITERATOR);
        } else {
            final byte[] bytes = bi.getBytesUnsafe();
            if (length == 1) {
                fillBytes(bytes[0], EMPTY_ITERATOR);
            } else {
                this.length = length;
                this.bytes = bytes;
            }
        }
    }

    public ArrayByteIterable(@NotNull final ByteIterator it) {
        fillBytes(it);
    }

    public ArrayByteIterable(@NotNull final ByteIterator it, final int size) {
        bytes = readIterator(it, size);
        length = size;
    }

    public ArrayByteIterable(final byte firstByte, @NotNull final ByteIterable bi) {
        fillBytes(firstByte, bi.iterator());
    }

    public ArrayByteIterable(final byte firstByte, @NotNull final ByteIterator it) {
        fillBytes(firstByte, it);
    }

    public ArrayByteIterable(byte[] bytes, int length) {
        if (length == 0) {
            fillBytes(EMPTY_ITERATOR);
        } else if (length == 1) {
            fillBytes(bytes[0], EMPTY_ITERATOR);
        } else {
            this.bytes = bytes;
            this.length = length;
        }
    }

    public ArrayByteIterable(@NotNull byte[] bytes) {
        this(bytes, bytes.length);
    }

    @Override
    public Iterator iterator() {
        return getIterator();
    }

    public Iterator iterator(final int offset) {
        return new Iterator(offset);
    }

    public void setBytes(@NotNull byte[] bytes) {
        this.bytes = bytes;
        length = bytes.length;
    }

    @Override
    public byte[] getBytesUnsafe() {
        return bytes;
    }

    public void writeTo(@NotNull final LightOutputStream output) {
        output.write(bytes, 0, length);
    }

    public static Iterator getEmptyIterator() {
        return EMPTY.ITERATOR;
    }

    @Override
    protected Iterator getIterator() {
        return new Iterator(0);
    }

    @Override
    protected void fillBytes() {
        // do nothing
    }

    public static ArrayByteIterable fromByte(final byte b) {
        return SINGLE_BYTE_ITERABLES[b & 0xff];
    }

    public class Iterator extends ByteIterator {

        private int offset = 0;

        public Iterator(int offset) {
            this.offset = offset;
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public byte next() {
            final int offset = this.offset;
            final byte result = bytes[offset];
            // such logic prevents from advancing of empty iterator
            this.offset = offset + 1;
            return result;
        }

        @Override
        public long skip(long bytes) {
            final long result = Math.min(bytes, ArrayByteIterable.this.length - offset);
            offset += (int) result;
            return result;
        }

        public byte[] getBytesUnsafe() {
            return bytes;
        }

        public int getLength() {
            return length;
        }

        public int getOffset() {
            return offset;
        }
    }

    @SuppressWarnings({"NonConstantFieldWithUpperCaseName"})
    public static final class EmptyIterable extends ArrayByteIterable {

        public final Iterator ITERATOR = new Iterator(0);

        EmptyIterable() {
            super(EMPTY_BYTES, 0);
        }

        @Override
        public Iterator iterator(int offset) {
            return ITERATOR;
        }

        @Override
        protected Iterator getIterator() {
            return ITERATOR;
        }

        @Override
        public void setBytes(@NotNull byte[] bytes) {
            throw new UnsupportedOperationException();
        }
    }

}
