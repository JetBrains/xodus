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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * An adapter of {@link ByteBuffer} to {@link ByteIterable}.
 */

public class ByteBufferByteIterable implements ByteIterable {

    @NotNull
    private final ByteBuffer buffer;  // this buffer should be immutable with position = 0
    private final int length;

    public ByteBufferByteIterable(@NotNull final ByteBuffer buffer) {
        this(buffer.slice(), buffer.remaining());
    }

    private ByteBufferByteIterable(@NotNull final ByteBuffer buffer, final int length) {
        this.buffer = buffer;
        this.length = length;
    }

    @Override
    public ByteIterator iterator() {
        final ByteBuffer copy = buffer.slice();
        return new ByteIterator() {

            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < length;
            }

            @Override
            public byte next() {
                return copy.get(index++);
            }

            @Override
            public long skip(long bytes) {
                final int result = (int) Math.min(bytes, ByteBufferByteIterable.this.length - index);
                index += result;
                return result;
            }
        };
    }

    @Override
    public byte[] getBytesUnsafe() {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes, buffer.position(), bytes.length);

        return bytes;
    }

    @Override
    public int baseOffset() {
        if (buffer.hasArray()) {
            return buffer.position();
        }

        return 0;
    }

    @Override
    public byte[] getBaseBytes() {
        if (buffer.hasArray()) {
            return buffer.array();
        }

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes, buffer.position(), bytes.length);

        return bytes;
    }

    @Override
    public byte byteAt(int offset) {
        return buffer.get(buffer.position() + offset);
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int compareTo(int length, ByteIterable right, int rightLength) {
        if (right instanceof ByteBufferByteIterable) {
            var copy = buffer.slice();
            var rightCopy = ((ByteBufferByteIterable) right).buffer.slice();

            copy.limit(length);
            rightCopy.limit(rightLength);

            return copy.compareTo(rightCopy);
        }


        final byte[] rightBase = right.getBaseBytes();
        final int rightOffset = right.baseOffset();

        final byte[] base = getBaseBytes();
        final int offset = baseOffset();

        return Arrays.compareUnsigned(base, offset, offset + length,
                rightBase, rightOffset, rightOffset + rightLength);
    }

    @Override
    public int compareTo(int from, int length, ByteIterable right, int rightFrom, int rightLength) {
        if (right instanceof ByteBufferByteIterable) {
            var copy = buffer.slice();
            var rightCopy = ((ByteBufferByteIterable) right).buffer.slice();

            copy.position(from);
            copy.limit(from + length);

            rightCopy.position(rightFrom);
            rightCopy.limit(rightFrom + rightLength);

            return copy.compareTo(rightCopy);
        }


        final byte[] rightBase = right.getBaseBytes();
        final int rightOffset = rightFrom + right.baseOffset();

        final byte[] base = getBaseBytes();
        final int offset = baseOffset();

        return Arrays.compareUnsigned(base, from + offset, from + offset + length,
                rightBase, rightFrom + rightOffset,
                rightFrom + rightOffset + rightLength);
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        final ByteBuffer copy = buffer.slice();
        copy.position(offset).limit(Math.min(buffer.limit(), offset + length));
        return new ByteBufferByteIterable(copy, length);
    }

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        if (right instanceof ByteBufferByteIterable) {
            return buffer.compareTo(((ByteBufferByteIterable) right).buffer);
        }


        final byte[] rightBase = right.getBaseBytes();
        final int rightOffset = right.baseOffset();

        final byte[] base = getBaseBytes();
        final int offset = buffer.position();

        return Arrays.compareUnsigned(base, offset, offset + length,
                rightBase, rightOffset, rightOffset + right.getLength());
    }
}
