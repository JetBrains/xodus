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
package jetbrains.exodus;

import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * An adapter of byte array to {@link ByteIterable}.
 */
public class ArrayByteIterable extends ByteIterableBase {

    @SuppressWarnings("StaticInitializerReferencesSubClass")
    public static final EmptyIterable EMPTY = new EmptyIterable();

    protected int offset;

    private static final ArrayByteIterable[] SINGLE_BYTE_ITERABLES;

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

            if (length == 1) {
                fillBytes(bi.byteAt(0), EMPTY_ITERATOR);
            } else {
                this.length = length;

                final byte[] bytes = bi.getBaseBytes();
                final int baseOffset = bi.baseOffset();

                if (baseOffset == 0) {
                    this.bytes = bytes;
                } else {
                    this.bytes = Arrays.copyOfRange(bytes, baseOffset, baseOffset + length);
                }
            }
        }

        this.offset = 0;
    }

    @Override
    public int compareTo(@NotNull ByteIterable right) {
        if (right instanceof ArrayByteIterable) {
            var rightIterable = (ArrayByteIterable) right;

            return Arrays.compareUnsigned(bytes, offset, offset + length, rightIterable.bytes,
                    rightIterable.offset, rightIterable.offset + rightIterable.length);
        }

        var rightOffset = right.baseOffset();
        var rightBase = right.getBaseBytes();

        return Arrays.compareUnsigned(bytes, offset, offset + length, rightBase,
                rightOffset, rightOffset + right.getLength());
    }


    @Override
    public int compareTo(int length, ByteIterable right, int rightLength) {
        if (right instanceof ArrayByteIterable) {
            var rightIterable = (ArrayByteIterable) right;

            return Arrays.compareUnsigned(bytes, offset, offset + length, rightIterable.bytes,
                    rightIterable.offset, rightIterable.offset + rightLength);
        }

        var rightOffset = right.baseOffset();
        var rightBase = right.getBaseBytes();

        return Arrays.compareUnsigned(bytes, offset, offset + length, rightBase,
                rightOffset, rightOffset + rightLength);
    }

    @Override
    public int compareTo(int from, int length, ByteIterable right, int rightFrom, int rightLength) {
        var offset = from + this.offset;

        if (right instanceof ArrayByteIterable) {
            var rightIterable = (ArrayByteIterable) right;
            var rightOffset = rightFrom + rightIterable.offset;

            return Arrays.compareUnsigned(bytes, offset, offset + length, rightIterable.bytes,
                    rightOffset, rightOffset + rightLength);
        }

        var rightOffset = rightFrom + right.baseOffset();
        var rightBase = right.getBaseBytes();

        return Arrays.compareUnsigned(bytes, offset, offset + length, rightBase,
                rightOffset, rightOffset + rightLength);
    }

    public ArrayByteIterable(@NotNull final ByteIterator it) {
        fillBytes(it);

        this.offset = 0;
    }

    public ArrayByteIterable(@NotNull final ByteIterator it, final int size) {
        bytes = readIterator(it, size);
        length = size;
        this.offset = 0;
    }

    public ArrayByteIterable(final byte firstByte, @NotNull final ByteIterable bi) {
        fillBytes(firstByte, bi.iterator());
        this.offset = 0;
    }

    public ArrayByteIterable(final byte firstByte, @NotNull final ByteIterator it) {
        fillBytes(firstByte, it);
        this.offset = 0;
    }

    public ArrayByteIterable(byte[] bytes, int length) {
        this(bytes, 0, length);
    }

    public ArrayByteIterable(byte[] bytes, int offset, int length) {
        if (length == 0) {
            fillBytes(EMPTY_ITERATOR);
            this.offset = 0;
        } else if (length == 1) {
            fillBytes(bytes[offset], EMPTY_ITERATOR);
            this.offset = 0;
        } else {
            this.bytes = bytes;
            this.length = length;
            this.offset = offset;
        }
    }

    public ArrayByteIterable(byte @NotNull [] bytes) {
        this(bytes, bytes.length);
    }

    @Override
    public Iterator iterator() {
        return getIterator();
    }

    public Iterator iterator(final int offset) {
        return new Iterator(this.offset + offset, length - offset);
    }

    public void setBytes(byte @NotNull [] bytes) {
        this.bytes = bytes;
        length = bytes.length;
        this.offset = 0;
    }

    @Override
    public byte[] getBytesUnsafe() {
        if (offset == 0) {
            return bytes;
        }

        return Arrays.copyOfRange(bytes, offset, offset + length);
    }

    @Override
    public @NotNull ByteIterable subIterable(int offset, int length) {
        return new ArrayByteIterable(bytes, this.offset + offset, length);
    }

    public void writeTo(@NotNull final LightOutputStream output) {
        output.write(bytes, offset, length);
    }

    @Override
    protected Iterator getIterator() {
        return new Iterator(this.offset, length);
    }

    @Override
    protected void fillBytes() {
        // do nothing
    }

    @Override
    public int baseOffset() {
        return offset;
    }

    @Override
    public byte[] getBaseBytes() {
        return bytes;
    }

    @Override
    public byte byteAt(int offset) {
        return bytes[this.offset + offset];
    }

    public static ArrayByteIterable fromByte(final byte b) {
        return SINGLE_BYTE_ITERABLES[b & 0xff];
    }

    public class Iterator implements ByteIterator {
        protected final int end;
        protected int offset;

        public Iterator(int offset, int length) {
            this.offset = offset;
            this.end = offset + length;
        }

        @Override
        public boolean hasNext() {
            return offset < end;
        }

        @Override
        public byte next() {
            return bytes[offset++];
        }

        @Override
        public long skip(long bytes) {
            final int result = (int) Math.min(bytes, end - offset);
            offset += result;
            return result;
        }
    }

    @SuppressWarnings({"NonConstantFieldWithUpperCaseName"})
    private static final class EmptyIterable extends ArrayByteIterable {

        public final Iterator ITERATOR = new Iterator(0, 0);

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
        public void setBytes(byte @NotNull [] bytes) {
            throw new UnsupportedOperationException();
        }
    }

}
