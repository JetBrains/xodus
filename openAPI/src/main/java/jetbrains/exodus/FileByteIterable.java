/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * An adapter to the contents of a region of a file. Doesn't support {@link #getBytesUnsafe()} as it
 * unconditionally throws {@link UnsupportedOperationException}.
 * <p>
 * Avoid using this class with Android API level less than 26.
 */
public class FileByteIterable implements ByteIterable {

    private final File file;
    private final long offset;
    private final int length;

    public FileByteIterable(@NotNull final File file) {
        this(file, 0L, (int) file.length());
    }

    public FileByteIterable(@NotNull final File file, final long offset, final int len) {
        this.file = file;
        this.offset = offset;
        this.length = len;
    }

    @Override
    public ByteIterator iterator() {
        try {
            try (FileChannel channel = openChannel()) {
                final ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
                return new ByteBufferByteIterable(buffer).iterator();
            }
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
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
    public int baseOffset() {
        return 0;
    }

    @Override
    public byte[] getBaseBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        return length;
    }

    @NotNull
    @Override
    public ByteIterable subIterable(final int offset, final int length) {
        return new FixedLengthByteIterable(this, offset, length);
    }

    @Override
    public int compareTo(@NotNull final ByteIterable right) {
        var bytes = getBaseBytes();
        var offset = baseOffset();

        var rightBytes = right.getBaseBytes();
        var rightOffset = right.baseOffset();

        return Arrays.compareUnsigned(bytes, offset, offset + this.getLength(),
                rightBytes, rightOffset, rightOffset + right.getLength());
    }

    @Override
    public int compareTo(final int length, final ByteIterable right, final int rightLength) {
        var bytes = getBaseBytes();
        var offset = baseOffset();

        var rightBytes = right.getBaseBytes();
        var rightOffset = right.baseOffset();

        return Arrays.compareUnsigned(bytes, offset, offset + length,
                rightBytes, rightOffset, rightOffset + rightLength);
    }


    @Override
    public int compareTo(int from, int length, ByteIterable right, int rightFrom, int rightLength) {
        var bytes = getBaseBytes();
        var offset = from + baseOffset();

        var rightBytes = right.getBaseBytes();
        var rightOffset = rightFrom + right.baseOffset();

        return Arrays.compareUnsigned(bytes, offset, offset + length,
                rightBytes, rightOffset, rightOffset + rightLength);
    }

    @Override
    public byte byteAt(int offset) {
        throw new UnsupportedOperationException();
    }

    private FileChannel openChannel() throws IOException {
        return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }
}
