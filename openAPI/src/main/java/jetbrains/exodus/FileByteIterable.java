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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * An adapter to the contents of a region of a file. Doesn't support {@link #getBytesUnsafe()} as it
 * unconditionally throws {@link UnsupportedOperationException}.
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
        return ByteIterableUtil.compare(this, right);
    }

    public InputStream asStream() throws IOException {
        return Channels.newInputStream(openChannel());
    }

    private FileChannel openChannel() throws IOException {
        return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }
}
