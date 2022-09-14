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
import jetbrains.exodus.bindings.BindingUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

public final class ArrayBackedByteIterable implements ByteIterable {
    public static final ArrayBackedByteIterable EMPTY = new ArrayBackedByteIterable(new byte[]{});
    public int offset;
    public int limit;

    @NotNull
    public byte[] bytes;

    public ArrayBackedByteIterable(@NotNull byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ArrayBackedByteIterable(@NotNull byte[] bytes, int offset, int length) {
        assert bytes.length >= length;

        this.offset = offset;
        this.limit = offset + length;
        this.bytes = bytes;
    }

    public ArrayBackedByteIterable duplicate() {
        return new ArrayBackedByteIterable(bytes, offset, limit - offset);
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
                    return bytes[currentOffset];
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
        return (long) LONG_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public int getNativeInt(int offset) {
        return (int) INT_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public int getNativeShort(int offset) {
        return (short) SHORT_HANDLE.get(bytes, offset + this.offset);
    }

    @Override
    public long getLong(int offset) {
        return BindingUtils.readLong(bytes, this.offset + offset);
    }

    @Override
    public int getInt(int offset) {
        return BindingUtils.readInt(bytes, this.offset + offset);
    }

    @Override
    public short getShort(int offset) {
        return BindingUtils.readShort(bytes, this.offset + offset);
    }

    @Override
    public String getString(int offset) {
        return BindingUtils.readString(bytes, this.offset + offset, this.limit - offset);
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
    public @NotNull ArrayBackedByteIterable subIterable(int offset, int length) {
        assert this.offset < this.limit;

        var len = limit - this.offset;
        if (offset > len || offset + length > len) {
            throw new IllegalStateException();
        }

        return new ArrayBackedByteIterable(bytes, this.offset + offset, length);
    }


    @Override
    public int compareTo(@NotNull ByteIterable o) {
        if (o instanceof ArrayBackedByteIterable other) {
            var otherOffset = other.offset;
            var otherLimit = other.limit;

            return Arrays.compareUnsigned(bytes, offset, limit,
                    other.bytes, otherOffset, otherLimit);
        }

        var otherArray = o.getBytesUnsafe();
        var otherLength = o.getLength();

        return Arrays.compareUnsigned(bytes, offset, limit, otherArray, 0, otherLength);
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
        if (bytes == null) {
            return "null";
        }
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
}
