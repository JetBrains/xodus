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
package jetbrains.exodus.log;

import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.io.Block;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BlockDataIterator extends ByteIteratorWithAddress {

    private final Log log;
    private final Block block;
    private final ByteBuffer lastPage;
    private final long lastPageAddress;
    private long position;
    private final long end;
    private final BufferedInputStream stream;

    private int lastPageCount;

    public BlockDataIterator(Log log, LogTip prevTip, Block block, long startAddress) {
        this.log = log;
        this.block = block;
        this.position = startAddress;
        this.end = block.getAddress() + block.length();
        this.lastPageAddress = log.getHighPageAddress(end);
        this.lastPage = LogUtil.allocatePage(log.getCachePageSize());
        final LogConfig config = log.getConfig();
        if (lastPageAddress == prevTip.pageAddress) {
            lastPageCount = prevTip.count;
            // fill with unencrypted bytes
            lastPage.put(0, prevTip.bytes, 0, prevTip.count);
        }
        this.stream = new BufferedInputStream(new BlockStream(config, block, position), log.getCachePageSize());
    }

    @Override
    public boolean hasNext() {
        return position < end;
    }

    @Override
    public byte next() {
        if (position >= end) {
            DataCorruptionException.raise(
                    "DataIterator: no more bytes available", log, position);
        }
        try {
            final byte[] result = new byte[1];
            if (stream.read(result, 0, 1) < 1) {
                DataCorruptionException.raise(
                        "DataIterator: no more bytes available", log, position);
            }
            position++;
            return result[0];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long skip(long bytes) {
        try {
            long result = 0;
            while (result < bytes) {
                final long skipped = stream.skip(bytes - result);
                if (skipped <= 0) {
                    break;
                }
                result += skipped;
            }
            position += result;
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getAddress() {
        return position;
    }

    @Override
    public int getOffset() {
        return (int) (position - block.getAddress());
    }

    public ByteBuffer getLastPage() {
        return lastPage;
    }

    public long getLastPageAddress() {
        return lastPageAddress;
    }

    public int getLastPageCount() {
        return lastPageCount;
    }

    private class BlockStream extends InputStream {
        @NotNull
        private final LogConfig config;
        private final Block block;
        private final boolean crypt;
        private long position;
        private StreamCipher cipher;

        BlockStream(@NotNull LogConfig config, Block block, long position) {
            this.config = config;
            this.block = block;
            if (config.getCipherProvider() != null) {
                final long skip = position % LogUtil.LOG_BLOCK_ALIGNMENT;
                this.position = position - skip; // pre-load some data to initialize cipher properly
                crypt = true;
                cipher = makeCipher((int) (this.position - block.getAddress()));
                if (skip > 0) {
                    final long skipped = skipInternal(skip);
                    if (skipped < skip) {
                        DataCorruptionException.raise(
                                "DataIterator: no more bytes available", log, position - skipped);
                    }
                }
            } else {
                this.position = position;
                crypt = false;
            }
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long skip(long n) {
            if (n >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException();
            }
            final byte[] skipped = new byte[(int) n]; // TODO: try to optimize it i. e. by using BufferedInputStream#mark
            return read(skipped, 0, (int) n);
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) {
            final int readLength = block.read(b, position - block.getAddress(), off, len);
            if (readLength > 0) {
                if (crypt) {
                    final int end = off + readLength;
                    long currentPosition = position;
                    for (int i = off; i < end; i++) {
                        b[i] = cipher.crypt(b[i]);
                        currentPosition++;
                        // TODO: optimize (find aligned position and reset cipher only on it)
                        checkCipher(currentPosition);
                    }
                }
                final long nextPosition = position + readLength;
                if (nextPosition > lastPageAddress) {
                    final int skip;
                    final int offset;
                    if (position < lastPageAddress) {
                        skip = (int) (lastPageAddress - position);
                        offset = 0;
                    } else {
                        offset = (int) (position - lastPageAddress);
                        skip = 0;
                    }
                    final int length = Math.min(lastPage.limit() - offset, readLength - skip);
                    lastPage.put(offset, b, off + skip, length);
                    lastPageCount += length;
                }
                position = nextPosition;
            }
            return readLength;
        }

        private long skipInternal(long n) {
            final int count = (int) n;
            final byte[] skipped = new byte[count];
            final int read = block.read(skipped, position - block.getAddress(), 0, count);
            for (int i = 0; i < read; i++) {
                cipher.crypt(skipped[i]);
            }
            position += read;
            return read;
        }

        private void checkCipher(final long position) {
            final int offset = (int) (position - block.getAddress());
            if (offset % LogUtil.LOG_BLOCK_ALIGNMENT == 0) {
                cipher = makeCipher(offset);
            }
        }

        private StreamCipher makeCipher(final Integer offset) {
            final StreamCipher result = config.getCipherProvider().newCipher();
            final long iv = config.getCipherBasicIV() + ((block.getAddress() + offset) / LogUtil.LOG_BLOCK_ALIGNMENT);
            result.init(config.getCipherKey(), EnvKryptKt.asHashedIV(iv));
            return result;
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
