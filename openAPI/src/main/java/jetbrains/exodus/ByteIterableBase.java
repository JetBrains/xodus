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
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Base class for most of {@link ByteIterable} implementations.
 */
public abstract class ByteIterableBase implements ByteIterable {
    public static final byte[][] SINGLE_BYTES;

    static {
        SINGLE_BYTES = new byte[256][];
        for (int i = 0; i < SINGLE_BYTES.length; i++) {
            //noinspection ObjectAllocationInLoop
            SINGLE_BYTES[i] = new byte[]{(byte) i};

        }
    }

    protected byte[] bytes = null;
    protected int length = -1;

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        var thisStart = baseOffset();
        var thisLen = getLength();

        var rightStart = right.baseOffset();
        var rightLen = right.getLength();

        return Arrays.compareUnsigned(this.getBaseBytes(), thisStart,
                thisStart + thisLen, right.getBaseBytes(), rightStart,
                rightStart + rightLen);
    }

    @Override
    public int compareTo(int length, ByteIterable right, int rightLength) {
        var thisStart = baseOffset();
        var rightStart = right.baseOffset();

        return Arrays.compareUnsigned(this.getBaseBytes(), thisStart,
                thisStart + length, right.getBaseBytes(), rightStart,
                rightStart + rightLength);
    }

    @Override
    public int compareTo(int from, int length, ByteIterable right, int rightFrom, int rightLength) {
        var thisStart = baseOffset();
        var rightStart = right.baseOffset();

        return Arrays.compareUnsigned(this.getBaseBytes(), thisStart + from,
                thisStart + length, right.getBaseBytes(), rightStart + rightFrom,
                rightStart + rightLength);
    }

    @Override
    public byte byteAt(int offset) {
        var base = getBaseBytes();
        var baseOffset = baseOffset();

        return base[baseOffset + offset];
    }

    @Override
    public ByteIterator iterator() {
        if (bytes == null) {
            return getIterator();
        }

        final byte[] bytes = this.bytes;
        final int len = length;

        return new ByteIterator() {

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < len;
            }

            @Override
            public byte next() {
                final byte result = bytes[i];
                ++i;
                return result;
            }

            @Override
            public long skip(long bytes) {
                final int result = Math.min(len - i, (int) bytes);
                i += result;
                return result;
            }
        };
    }

    @Override
    public byte[] getBytesUnsafe() {
        if (bytes == null) {
            fillBytes();
        }
        return bytes;
    }

    @Override
    public int baseOffset() {
        return 0;
    }

    @Override
    public byte[] getBaseBytes() {
        if (bytes == null) {
            fillBytes();
        }

        return bytes;
    }

    @Override
    public int getLength() {
        if (length == -1) {
            fillBytes();
        }
        return length;
    }

    @NotNull
    public ByteIterable subIterable(final int offset, final int length) {
        if (length == 0) {
            return EMPTY;
        }

        if (bytes != null) {
            return new ArrayByteIterable(bytes, baseOffset() + offset, length);
        }

        return new FixedLengthByteIterable(this, offset, length);
    }

    public ByteIterable getSource() {
        return this;
    }

    protected abstract ByteIterator getIterator();

    protected void fillBytes() {
        fillBytes(getIterator());
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof ByteIterable && compareTo((ByteIterable) obj) == 0;
    }

    /**
     * @return hash code computed using all bytes of the iterable.
     */
    @Override
    public int hashCode() {
        final byte[] a = getBaseBytes();
        final int offset = baseOffset();

        if (a == null) {
            return 0;
        }
        int result = 1;
        final int length = getLength();
        for (int i = 0; i < length; i++) {
            result = 31 * result + a[offset + i];
        }

        return result;
    }

    @Override
    public String toString() {
        var offset = baseOffset();
        return toString(getBaseBytes(), offset, offset + getLength());
    }

    public static String toString(final byte @Nullable [] bytes, final int start, final int end) {
        if (bytes == null) {
            return "null";
        }
        if (end <= start) {
            return "[]";
        }
        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = start; ; ) {
            b.append(bytes[i++]);
            if (i == end) {
                return b.append(']').toString();
            }
            b.append(", ");
        }
    }

    public static void fillBytes(@NotNull final ByteIterable bi, @NotNull final LightOutputStream output) {
        if (bi instanceof ArrayByteIterable) {
            final ArrayByteIterable abi = (ArrayByteIterable) bi;
            final int length = abi.getLength();
            final int offset = abi.baseOffset();
            if (length > 0) {
                output.write(abi.bytes, offset, length);
            }
        } else {
            final ByteIterator it = bi.iterator();
            if (it.hasNext()) {
                fillBytes(it, output);
            }
        }
    }

    public static byte[] readIterator(@NotNull final ByteIterator it, final int size) {
        if (size == 0) {
            return EMPTY_BYTES;
        }
        if (size == 1) {
            return SINGLE_BYTES[(it.next() & 0xff)];
        }
        final byte[] result = new byte[size];
        for (int i = 0; i < size; i++) {
            result[i] = it.next();
        }
        return result;
    }

    // same as readIterator() but wuth respect to it.hasNext()
    public static byte[] readIteratorSafe(@NotNull final ByteIterator it, final int size) {
        if (size == 0) {
            return EMPTY_BYTES;
        }
        if (size == 1) {
            return it.hasNext() ? SINGLE_BYTES[(it.next() & 0xff)] : EMPTY_BYTES;
        }
        final byte[] result = new byte[size];
        for (int i = 0; i < size; i++) {
            if (!it.hasNext()) {
                return i == 0 ? EMPTY_BYTES : Arrays.copyOf(result, i);
            }
            result[i] = it.next();
        }
        return result;
    }

    public static byte @NotNull [] readIterable(@NotNull final ByteIterable it) {
        return readIterator(it.iterator(), it.getLength());
    }

    private static void fillBytes(@NotNull final ByteIterator it, @NotNull final LightOutputStream output) {
        do {
            output.write(it.next());
        } while (it.hasNext());
    }

    protected void fillBytes(@NotNull final ByteIterator it) {
        if (!it.hasNext()) {
            bytes = EMPTY_BYTES;
            length = 0;
        } else {
            fillBytes(it.next(), it);
        }
    }

    protected void fillBytes(final byte firstByte, @NotNull final ByteIterator it) {
        if (!it.hasNext()) {
            bytes = SINGLE_BYTES[firstByte & 0xff];
            length = 1;
        } else {
            final LightOutputStream output = new LightOutputStream(4);
            output.write(firstByte);
            fillBytes(it, output);
            bytes = output.getBufferBytes();
            length = output.size();
        }
    }
}
