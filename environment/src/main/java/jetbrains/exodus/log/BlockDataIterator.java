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
package jetbrains.exodus.log;

import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.io.Block;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BlockDataIterator extends ByteIteratorWithAddress {

    private final Log log;
    private final Block block;
    private final byte[] lastPage;
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
        this.lastPage = new byte[log.getCachePageSize()];
        final LogConfig config = log.getConfig();

        if (lastPageAddress == prevTip.pageAddress) {
            lastPageCount = prevTip.count;
            System.arraycopy(prevTip.bytes, 0, lastPage, 0, prevTip.count); // fill with unencrypted bytes
        }

        final long skip = config.getCipherProvider() != null ? position % LogUtil.LOG_BLOCK_ALIGNMENT : 0;
        this.stream = new BufferedInputStream(new BlockStream(config, block, position - skip), log.getCachePageSize());

        if (skip > 0) {
            try {
                final long skipped = stream.skip(skip);
                if (skipped < skip) {
                    DataCorruptionException.raise(
                            "DataIterator: no more bytes available", log, position - skipped);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
            long result = stream.skip(bytes);
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

    public byte[] getLastPage() {
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
        private boolean finished = false;

        BlockStream(@NotNull LogConfig config, Block block, long position) {
            this.config = config;
            this.block = block;
            this.position = position;
            if (config.getCipherProvider() != null) {
                crypt = true;
                if (position % LogUtil.LOG_BLOCK_ALIGNMENT != 0) {
                    finished = true;
                }
                cipher = makeCipher((int) (position - block.getAddress()));
            } else {
                crypt = false;
            }
        }

        @Override
        public int available() throws IOException {
            return super.available();
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long skip(long n) {
            if (finished) {
                return -1;
            }
            if (crypt) {
                if (position % LogUtil.LOG_BLOCK_ALIGNMENT != 0) {
                    finished = true;
                    return -1;
                }
                if (n >= LogUtil.LOG_BLOCK_ALIGNMENT) {
                    throw new IllegalStateException("Cannot skip this much");
                }
                final int count = (int) n;
                final byte[] skipped = new byte[count];
                final int read = block.read(skipped, position - block.getAddress(), 0, count);
                for (int i = 0; i < read; i++) {
                    cipher.crypt(skipped[i]);
                }
                position += read;
                return read;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) {
            if (finished) {
                return -1;
            }
            final int readLength = block.read(b, position - block.getAddress(), off, len);
            if (readLength > 0) {
                if (crypt) {
                    final int end = off + readLength;
                    for (int i = off; i < end; i++) {
                        b[i] = cipher.crypt(b[i]);
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
                    final int length = Math.min(lastPage.length - offset, readLength - skip);
                    System.arraycopy(b, off + skip, lastPage, offset, length);
                    lastPageCount += length;
                }
                position = nextPosition;
            }
            checkCipher();
            return readLength;
        }

        private void checkCipher() {
            if (crypt) {
                if (position % LogUtil.LOG_BLOCK_ALIGNMENT != 0) {
                    finished = true;
                } else {
                    final int offset = (int) (position - block.getAddress());
                    if (offset % LogUtil.LOG_BLOCK_ALIGNMENT == 0) {
                        cipher = makeCipher(offset);
                    }
                }
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
