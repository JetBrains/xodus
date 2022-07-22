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

import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * An adapter of {@link ByteBuffer} to {@link ByteIterable}. Doesn't support {@link #getBytesUnsafe()} as
 * it unconditionally throws {@link UnsupportedOperationException}.
 */

public final class ByteBufferByteIterable implements ByteIterable, ByteBufferIterable {

    @NotNull
    private final ByteBuffer buffer;  // this buffer should be immutable with position = 0

    public ByteBufferByteIterable(@NotNull final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private ByteBufferByteIterable(@NotNull final ByteBuffer buffer, final int length) {
        this.buffer = buffer.slice(0, length).order(buffer.order());
    }

    @Override
    public ByteBufferIterableByteIterator iterator() {
        if (buffer.hasArray()) {
            return new ByteArrayIterator(buffer);
        }

        return new ByteBufferIterator(buffer.duplicate());
    }

    public ByteBufferIterableByteIterator iterator(int offset) {
        if (offset >= buffer.limit()) {
            return EMPTY_ITERATOR;
        }

        if (buffer.hasArray()) {
            return new ByteArrayIterator(buffer.slice(offset, buffer.limit() - offset));
        }

        return new ByteBufferIterator(buffer.slice(offset, buffer.limit() - offset));
    }

    @Override
    public byte[] getBytesUnsafe() {
        var data = new byte[buffer.limit()];
        buffer.get(0, data);

        return data;
    }

    @Override
    public String toString() {
        var data = new byte[buffer.limit()];
        buffer.get(0, data);

        return Arrays.toString(data);
    }

    @Override
    public int getLength() {
        return buffer.limit();
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        final ByteBuffer copy = buffer.slice(offset, Math.min(buffer.limit(), offset + length)).order(buffer.order());
        return new ByteBufferByteIterable(copy, length);
    }

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        if (right instanceof ByteBufferByteIterable) {
            return buffer.compareTo(((ByteBufferByteIterable) right).buffer);
        }
        return ByteIterableUtil.compare(this, right);
    }

    public ByteBuffer getUnderlying() {
        return buffer;
    }

    public static final ByteBufferIterableByteIterator EMPTY_ITERATOR =
            new ByteArrayIterator(ByteBuffer.allocate(0));

    @Override
    public ByteBuffer getByteBuffer() {
        return buffer.asReadOnlyBuffer().order(buffer.order());
    }

    public static abstract class ByteBufferIterableByteIterator extends ByteIterator {
        public abstract int getOffset();

        public abstract int nextBytes(byte[] array, int off, int len);
    }

    private static final class ByteArrayIterator extends ByteBufferIterableByteIterator {
        private final int arrayOffset;
        private final int len;
        private final byte[] array;

        private int offset;

        private ByteArrayIterator(ByteBuffer buffer) {
            assert buffer.hasArray();

            arrayOffset = buffer.arrayOffset();
            array = buffer.array();
            len = buffer.limit();
        }

        @Override
        public boolean hasNext() {
            return offset < len;
        }

        @Override
        public byte next() {
            final int off = offset + arrayOffset;
            final byte data = array[off];
            offset++;
            return data;
        }

        @Override
        public long skip(long bytes) {
            final int result = (int) Math.min(bytes, len - offset);
            offset += result;

            return result;
        }

        public int getOffset() {
            return offset;
        }

        public int nextBytes(byte[] array, int off, int len) {
            len = Math.min(len, this.len - offset);
            System.arraycopy(this.array, offset + arrayOffset, array, off, len);

            offset += len;
            return len;
        }
    }

    private static final class ByteBufferIterator extends ByteBufferIterableByteIterator {
        private final ByteBuffer buffer;

        private ByteBufferIterator(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean hasNext() {
            return buffer.remaining() > 0;
        }

        @Override
        public byte next() {
            return buffer.get();
        }

        @Override
        public long skip(long bytes) {
            final int result = (int) Math.min(bytes, buffer.remaining());
            buffer.position(buffer.position() + result);

            return result;
        }

        public int getOffset() {
            return buffer.position();
        }

        public int nextBytes(byte[] array, int off, int len) {
            len = Math.min(len, buffer.remaining());

            buffer.get(array, off, len);

            return len;
        }
    }
}
