/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.util.SharedRandomAccessFile;
import jetbrains.exodus.util.SharedRandomAccessFileCache;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class FileByteIterable implements ByteIterable {

    private static final SharedRandomAccessFileCache defaultSharedRandomAccessFileCache = new SharedRandomAccessFileCache(16);

    private final File file;
    private final long offset;
    private final int length;
    private final SharedRandomAccessFileCache sharedRandomAccessFileCache;

    public FileByteIterable(@NotNull final File file) {
        this(file, 0L, (int) file.length());
    }

    public FileByteIterable(@NotNull final File file, final long offset, final int len) {
        this(file, offset, len, defaultSharedRandomAccessFileCache);
    }

    public FileByteIterable(@NotNull final File file,
                            final long offset,
                            final int len,
                            @NotNull final SharedRandomAccessFileCache sharedRandomAccessFileCache) {
        this.file = file;
        this.offset = offset;
        this.length = len;
        this.sharedRandomAccessFileCache = sharedRandomAccessFileCache;
    }

    @Override
    public ByteIterator iterator() {
        return asArrayByteIterable().iterator();
    }

    @Override
    public byte[] getBytesUnsafe() {
        return toArray();
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int compareTo(ByteIterable right) {
        throw new UnsupportedOperationException();
    }

    public ArrayByteIterable asArrayByteIterable() {
        return new ArrayByteIterable(toArray());
    }

    public InputStream asStream() throws IOException {
        return new RandomAccessFileInputStream(getRandomAccessFile(), length);
    }

    private byte[] toArray() {
        if (length == 0) {
            return EMPTY_BYTES;
        }
        try {
            final SharedRandomAccessFile f = getRandomAccessFile();
            try {
                if (length == 1) {
                    return ByteIterableBase.SINGLE_BYTES[f.read() & 0xff];
                }
                final byte[] result = new byte[length];
                int readTotal = 0;
                while (readTotal < length) {
                    final int read = f.read(result, readTotal, length - readTotal);
                    if (read <= 0) {
                        throw new EOFException();
                    }
                    readTotal += read;
                }
                return result;
            } finally {
                f.close();
            }
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
    }

    private SharedRandomAccessFile getRandomAccessFile() throws IOException {
        final SharedRandomAccessFile result = sharedRandomAccessFileCache.getFile(file);
        result.seek(offset);
        return result;
    }

    private static class RandomAccessFileInputStream extends InputStream {

        private final SharedRandomAccessFile f;
        private int remainingBytes;

        private RandomAccessFileInputStream(@NotNull final SharedRandomAccessFile f,
                                            final int length) throws IOException {
            this.f = f;
            this.remainingBytes = length;
        }

        @Override
        public int read() throws IOException {
            if (remainingBytes <= 0) {
                return -1;
            }
            --remainingBytes;
            return f.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remainingBytes <= 0) {
                return -1;
            }
            final int bytesRead = f.read(b, off, Math.min(len, remainingBytes));
            remainingBytes -= bytesRead;
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            f.close();
        }
    }
}
