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
        final long length = block.length();
        this.end = block.getAddress() + length;
        this.lastPageAddress = log.getHighPageAddress(end);
        this.lastPage = new byte[log.getCachePageSize()];
        if (lastPageAddress == prevTip.pageAddress) {
            lastPageCount = prevTip.count;
            System.arraycopy(prevTip.bytes, 0, lastPage, 0, prevTip.count);
        }
        this.stream = new BufferedInputStream(new BlockStream(block, length, position), log.getCachePageSize());
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
            final long nextPosition = position + 1;
            // force BufferedInputStream to bulk read remaining bytes to limit Block.read invocations count on skip()
            if (nextPosition >= lastPageAddress) {
                stream.mark(lastPage.length);
            }
            position = nextPosition;
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
        private final Block block;
        private final long length;
        private long position;

        BlockStream(Block block, long length, long position) {
            this.block = block;
            this.length = length;
            this.position = position;
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
            final long startingPosition = position;
            final long nextPosition = position + n;
            final long skipLength;
            if (nextPosition < length) {
                position = nextPosition;
                skipLength = n;
            } else {
                position = length;
                skipLength = length - startingPosition;
            }
            final long unreadStartAddress = lastPageAddress + lastPageCount;
            if (nextPosition > unreadStartAddress) {
                final long lastPageEnd = lastPageAddress + lastPage.length;
                if (unreadStartAddress < lastPageEnd) {
                    block.read(lastPage, unreadStartAddress - block.getAddress(), lastPageCount, (int) (lastPageEnd - unreadStartAddress));
                }
            }
            return skipLength;
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) {
            final int writtenLength = block.read(b, position - block.getAddress(), off, len);
            if (writtenLength > 0) {
                final long nextPosition = position + writtenLength;
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
                    final int length = Math.min(lastPage.length - offset, writtenLength - skip);
                    System.arraycopy(b, off + skip, lastPage, offset, length);
                    lastPageCount += length;
                }
                position = nextPosition;
            }
            return writtenLength;
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
