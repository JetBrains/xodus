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

import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * An adapter of {@link ByteBuffer} to {@link ByteIterable}. Doesn't support {@link #getBytesUnsafe()} as
 * it unconditionally throws {@link UnsupportedOperationException}.
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

    /**
     * @return nothing since unconditionally throws {@link UnsupportedOperationException}.
     * @throws UnsupportedOperationException always since this operation is unsupported
     */
    @Override
    public byte[] getBytesUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        return length;
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
        return ByteIterableUtil.compare(this, right);
    }
}
