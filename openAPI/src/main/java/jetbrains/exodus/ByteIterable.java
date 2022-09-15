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

import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.util.ArrayBackedByteIterable;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * If working with {@link jetbrains.exodus.env.Environment}, any key and value should be a ByteIterable.
 * ByteIterable is a mix of iterable and array. It allows to lazily enumerate bytes without boxing.
 * On the other hand, you can get its length using method getLength(). Generally, iterating over bytes
 * of ByteIterable is performed by means of getting {@link ByteIterator}.
 *
 * @see jetbrains.exodus.ArrayByteIterable
 * @see jetbrains.exodus.ByteBufferByteIterable
 * @see jetbrains.exodus.FileByteIterable
 * @see jetbrains.exodus.FixedLengthByteIterable
 * @see jetbrains.exodus.CompoundByteIterable
 */
public interface ByteIterable extends Comparable<ByteIterable> {
    VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class,
            ByteOrder.nativeOrder());
    VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class,
            ByteOrder.nativeOrder());
    VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class,
            ByteOrder.nativeOrder());


    byte[] EMPTY_BYTES = {};

    ByteIterator iterator();

    default long getNativeLong(int offset) {
        var bytes = getBytesUnsafe();
        return (long) LONG_HANDLE.get(bytes, offset);
    }

    default int getNativeInt(int offset) {
        var bytes = getBytesUnsafe();
        return (int) INT_HANDLE.get(bytes, offset);
    }


    @SuppressWarnings("unused")
    default int getNativeShort(int offset) {
        var bytes = getBytesUnsafe();
        return (short) SHORT_HANDLE.get(bytes, offset);
    }

    default long getLong(int offset) {
        var bytes = getBytesUnsafe();
        return BindingUtils.readLong(bytes, offset);
    }

    default int getInt(int offset) {
        var bytes = getBytesUnsafe();
        return BindingUtils.readInt(bytes, offset);
    }

    default short getShort(int offset) {
        var bytes = getBytesUnsafe();
        return BindingUtils.readShort(bytes, offset);
    }

    default byte getByte(int offset) {
        var bytes = getBytesUnsafe();
        var len = getLength();

        if (offset < len) {
            return bytes[offset];
        }

        throw new IndexOutOfBoundsException();
    }

    default String getString(int offset) {
        var bytes = getBytesUnsafe();
        return BindingUtils.readString(bytes, 0, getLength());
    }

    /**
     * @return raw content of the {@code ByteIterable}. May return array with length greater than {@link #getLength()}.
     */
    default byte[] getBytesUnsafe() {
        var bytes = new byte[getLength()];
        var iterator = iterator();

        for (int i = 0; i < bytes.length; i++) {
            assert iterator.hasNext();
            bytes[i] = iterator.next();
        }

        return bytes;
    }

    default ByteBuffer getByteBuffer() {
        var data = getBytesUnsafe();
        var len = getLength();

        return ByteBuffer.wrap(data, 0, len);
    }

    default ArrayBackedByteIterable toArrayBackedIterable() {
        var data = getBytesUnsafe();
        var len = getLength();

        return new ArrayBackedByteIterable(data, 0, len);
    }

    /**
     * @return length of the {@code ByteIterable}.
     */
    default int getLength() {
        int length = 0;
        var iterator = iterator();
        while (iterator.hasNext()) {
            length++;
        }

        return length;
    }

    default boolean isEmpty() {
        return getLength() == 0;
    }

    default void writeIntoBuffer(ByteBuffer buffer, int bufferPosition) {
        var array = getBytesUnsafe();
        var arrayLength = getLength();

        buffer.put(bufferPosition, array, 0, arrayLength);
    }

    default int commonPrefix(ByteIterable other) {
        var array = getBytesUnsafe();
        var arrayLength = getLength();

        var otherArray = other.getBytesUnsafe();
        var otherArrayLength = other.getLength();

        assert Arrays.compareUnsigned(array, 0, arrayLength, otherArray, 0, otherArrayLength) < 0;

        var mismatch = Arrays.mismatch(array, 0, arrayLength, otherArray, 0, otherArrayLength);

        //first key is a prefix of second one
        if (mismatch == arrayLength) {
            return mismatch;
        }

        //if second key is only one byte longer
        if (otherArrayLength == mismatch + 1) {
            var mismatchedByteFirst = array[mismatch];
            var mismatchedByteSecond = otherArray[mismatch];

            if (Byte.toUnsignedInt(mismatchedByteSecond) - Byte.toUnsignedInt(mismatchedByteFirst) == 1) {
                return mismatch + 1;
            }
        }


        return mismatch;
    }

    default int mismatch(ByteIterable other) {
        var array = getBytesUnsafe();
        var arrayLength = getLength();

        var otherArray = other.getBytesUnsafe();
        var otherArrayLength = other.getLength();

        return Arrays.mismatch(array, 0, arrayLength, otherArray, 0, otherArrayLength);
    }

    /**
     * @param offset start offset, inclusive
     * @param length length of the sub-iterable
     * @return a fixed-length sub-iterable of the {@code ByteIterable} starting from {@code offset}.
     */
    @NotNull
    ByteIterable subIterable(final int offset, final int length);

    ByteIterator EMPTY_ITERATOR = new ByteIterator() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public byte next() {
            return (byte) 0;
        }

        @Override
        public long skip(long bytes) {
            return 0;
        }
    };

    ByteIterable EMPTY = new ByteIterable() {

        @Override
        public ByteIterator iterator() {
            return EMPTY_ITERATOR;
        }

        @Override
        public int compareTo(@NotNull ByteIterable right) {
            return right.iterator().hasNext() ? -1 : 0;
        }

        @Override
        public byte[] getBytesUnsafe() {
            return EMPTY_BYTES;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @NotNull
        @Override
        public ByteIterable subIterable(int offset, int length) {
            return this;
        }

        @Override
        public String toString() {
            return "[ByteIterable.EMPTY]";
        }
    };
}
