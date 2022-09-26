/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.util;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.bindings.BindingUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

public final class ArrayBackedByteIterable implements ByteIterable {
    public static final ArrayBackedByteIterable EMPTY = new ArrayBackedByteIterable(new byte[]{});

    private int offset;
    private int limit;

    public byte @NotNull [] bytes;

    public ArrayBackedByteIterable(byte @NotNull [] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ArrayBackedByteIterable(byte @NotNull [] bytes, int offset, int length) {
        assert bytes.length >= length;

        this.offset = offset;
        this.limit = offset + length;
        this.bytes = bytes;
    }

    public ArrayBackedByteIterable duplicate() {
        return new ArrayBackedByteIterable(bytes, offset, limit - offset);
    }

    @Override
    public ArrayBackedByteIterable toArrayBackedIterable() {
        return this;
    }

    @Override
    public ByteIterator iterator() {
        return new ByteIterator() {
            private int currentOffset = offset;

            @Override
            public boolean hasNext() {
                return currentOffset < limit;
            }

            @Override
            public byte next() {
                if (currentOffset < limit) {
                    return bytes[currentOffset++];
                }

                throw new NoSuchElementException();
            }

            @Override
            public long skip(long bytes) {
                var diff = Math.min(limit - currentOffset, bytes);
                currentOffset += diff;

                return diff;
            }
        };
    }

    @Override
    public long getNativeLong(int offset) {
        assert offset >= 0 && this.offset + offset <= limit - Long.BYTES;
        return (long) LONG_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public int getNativeInt(int offset) {
        assert offset >= 0 && this.offset + offset <= limit - Integer.BYTES;
        return (int) INT_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public int getNativeShort(int offset) {
        assert offset >= 0 && this.offset + offset <= limit - Short.BYTES;
        return (short) SHORT_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public long getLong(int offset) {
        if (offset < 0 || offset > limit - Long.BYTES) {
            throw new IndexOutOfBoundsException();
        }

        return BindingUtils.readLong(bytes, this.offset + offset);
    }

    @Override
    public int getInt(int offset) {
        if (offset < 0 || offset > limit - Integer.BYTES) {
            throw new IndexOutOfBoundsException();
        }

        return BindingUtils.readInt(bytes, this.offset + offset);
    }

    @Override
    public short getShort(int offset) {
        if (offset < 0 || offset > limit - Short.BYTES) {
            throw new IndexOutOfBoundsException();
        }

        return BindingUtils.readShort(bytes, this.offset + offset);
    }

    @Override
    public String getString(int offset) {
        if (offset < 0 || offset > limit) {
            throw new IndexOutOfBoundsException();
        }

        return BindingUtils.readString(bytes, this.offset + offset, this.limit - offset);
    }

    @Override
    public byte getByte(int offset) {
        if (this.offset + offset < limit) {
            return bytes[this.offset + offset];
        }

        throw new IndexOutOfBoundsException();
    }

    @Override
    public byte[] getBytesUnsafe() {
        if (offset == 0) {
            return bytes;
        }

        var result = new byte[limit - offset];
        System.arraycopy(bytes, offset, result, 0, result.length);
        return result;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return ByteBuffer.wrap(bytes, offset, limit - offset);
    }

    @Override
    public int getLength() {
        return limit - offset;
    }

    @Override
    public void writeIntoBuffer(ByteBuffer buffer, int bufferPosition) {
        buffer.put(bufferPosition, bytes, offset, limit - offset);
    }

    @Override
    public @NotNull ArrayBackedByteIterable subIterable(int offset, int length) {
        if (offset == 0 && length == this.limit - this.offset) {
            return this;
        }

        assert this.offset < this.limit;

        var len = this.limit - this.offset;
        if (offset > len || offset + length > len) {
            throw new IllegalStateException();
        }

        if (length == 0) {
            return EMPTY;
        }

        return new ArrayBackedByteIterable(this.bytes, this.offset + offset, length);
    }

    @Override
    public int mismatch(ByteIterable other) {
        if (other instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            return Arrays.mismatch(this.bytes, this.offset, this.limit, arrayBackedByteIterable.bytes,
                    arrayBackedByteIterable.offset, arrayBackedByteIterable.limit);
        } else if (other instanceof CompoundByteIterable compoundByteIterable) {
            return compoundByteIterable.mismatch(this);
        }

        return ByteIterable.super.mismatch(other);
    }

    @Override
    public int compareTo(@NotNull ByteIterable o) {
        if (o instanceof ArrayBackedByteIterable other) {
            var otherOffset = other.offset;
            var otherLimit = other.limit;

            return Arrays.compareUnsigned(bytes, offset, limit,
                    other.bytes, otherOffset, otherLimit);
        } else if (o instanceof CompoundByteIterable compoundByteIterable) {
            return -compoundByteIterable.compareTo(this);
        }

        var otherArray = o.getBytesUnsafe();
        var otherLength = o.getLength();

        return Arrays.compareUnsigned(bytes, offset, limit, otherArray, 0, otherLength);
    }

    @Override
    public int commonPrefix(ByteIterable other) {
        assert compareTo(other) < 0;

        if (other instanceof CompoundByteIterable compoundByteIterable) {
            return compoundByteIterable.reversedCommonPrefix(bytes, offset, limit - offset);
        }

        int otherArrayLength = other.getLength();

        byte[] otherArray;
        int otherOffset;

        if (other instanceof ArrayBackedByteIterable otherArrayBackedByteIterable) {
            otherOffset = otherArrayBackedByteIterable.offset;
            otherArray = otherArrayBackedByteIterable.bytes;
        } else {
            otherOffset = 0;
            otherArray = other.getBytesUnsafe();
        }

        var arrayLength = limit - offset;
        var mismatch = Arrays.mismatch(bytes, offset, limit, otherArray, otherOffset,
                otherOffset + otherArrayLength);

        //first key is a prefix of second one
        if (mismatch == arrayLength) {
            return mismatch;
        }

        //if second key is only one byte longer
        if (otherArrayLength == mismatch + 1) {
            var mismatchedByteFirst = bytes[offset + mismatch];
            var mismatchedByteSecond = otherArray[otherOffset + mismatch];

            if (Byte.toUnsignedInt(mismatchedByteSecond) - Byte.toUnsignedInt(mismatchedByteFirst) == 1) {
                return mismatch + 1;
            }
        }


        return mismatch;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof ByteIterable && compareTo((ByteIterable) obj) == 0;
    }

    @Override
    public int hashCode() {
        int result = 1;

        for (int i = offset; i < limit; i++) {
            result = 31 * result + bytes[i];
        }

        return result;
    }

    @Override
    public String toString() {
        if (offset == limit) {
            return "[]";
        }

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset; ; ) {
            b.append(bytes[i++]);
            if (i == limit) {
                return b.append(']').toString();
            }
            b.append(", ");
        }
    }

    public int limit() {
        return limit;
    }

    public void limit(int limit) {
        assert limit >= offset && limit <= bytes.length;

        this.limit = limit;
    }

    public int offset() {
        return offset;
    }

    public void offset(int offset) {
        assert limit >= offset;

        this.offset = offset;
    }
}
