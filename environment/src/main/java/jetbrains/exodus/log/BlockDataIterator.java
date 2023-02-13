/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.log;

import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.io.Block;

public class BlockDataIterator implements ByteIteratorWithAddress {

    private final Log log;
    private final Block block;
    private long position;
    private final long end;

    private byte[] currentPage;

    private final int pageSize;

    private final int chunkSize;

    private final boolean checkPage;

    private final LogConfig config;

    public BlockDataIterator(Log log, Block block, long startAddress, boolean checkPage) {
        this.checkPage = checkPage;
        this.log = log;
        this.block = block;
        this.position = startAddress;
        this.end = block.getAddress() + block.length();
        this.pageSize = log.getCachePageSize();

        config = log.getConfig();

        if (log.getFormatWithHashCodeIsUsed()) {
            chunkSize = pageSize - BufferedDataWriter.HASH_CODE_SIZE;
        } else {
            chunkSize = pageSize;
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

        if (currentPage != null) {
            var pageOffset = (int) position & (pageSize - 1);

            assert pageOffset <= pageSize;

            var result = currentPage[pageOffset];
            position++;

            if (pageOffset + 1 == chunkSize) {
                position = position - pageOffset - 1 + pageSize;
                currentPage = null;
            }

            return result;
        }

        loadPage();
        return next();
    }

    private void loadPage() {
        int currentPageSize = (int) Math.min(end - position, pageSize);
        final byte[] result = new byte[currentPageSize];
        final int read = block.read(result, position - block.getAddress(), 0, currentPageSize);

        if (read != currentPageSize) {
            DataCorruptionException.raise("Incorrect amount of bytes was read, expected " +
                            currentPageSize + " but was " + read,
                    log, position);
        }

        if (checkPage) {
            if (currentPageSize != pageSize) {
                DataCorruptionException.raise("Incorrect page size -  " + currentPageSize, log, position);
            }

            BufferedDataWriter.checkPageConsistency(position, result, pageSize, log);
        }

        var cipherProvider = config.getCipherProvider();
        int encryptedBytes;
        if (cipherProvider != null) {
            if (currentPageSize < pageSize) {
                encryptedBytes = currentPageSize;
            } else {
                encryptedBytes = chunkSize;
            }

            EnvKryptKt.cryptBlocksMutable(
                    cipherProvider, config.getCipherKey(), config.getCipherBasicIV(),
                    position, result, 0, encryptedBytes, LogUtil.LOG_BLOCK_ALIGNMENT
            );
        }

        this.currentPage = result;
    }

    @Override
    public long skip(long bytes) {
        final long bytesToSkip = Math.min(bytes, position - end);
        var currentPageOffset = (int) position & (pageSize - 1);

        final long pageBytesToSkip = Math.min(bytesToSkip, chunkSize - currentPageOffset);
        position += pageBytesToSkip;

        if (bytesToSkip > pageBytesToSkip) {
            final long rest = bytesToSkip - pageBytesToSkip;

            final long pagesToSkip = rest / chunkSize;
            final long pageSkip = pagesToSkip * pageSize;
            final long offsetSkip = pagesToSkip * chunkSize;


            final int pageOffset = (int) (rest - offsetSkip);
            final long addressDiff = pageSkip + pageOffset;

            position += addressDiff;
            currentPage = null;
        }

        return bytesToSkip;
    }

    @Override
    public long getAddress() {
        return position;
    }

    @Override
    public int getOffset() {
        return (int) (position - block.getAddress());
    }

    @Override
    public int available() {
        throw new UnsupportedOperationException();
    }
}
