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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

public final class BufferedSyncDataWriter implements BufferedDataWriter {

    // immutable state
    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    @NotNull
    private final DataWriter child;
    @NotNull
    private final LogTip initialPage;
    @Nullable
    private final StreamCipherProvider cipherProvider;
    private final byte[] cipherKey;
    private final long cipherBasicIV;
    private final int pageSize;
    @NotNull
    private final BlockSet.Mutable blockSetMutable;

    // mutable state
    @NotNull
    private MutablePage currentPage;
    public long highAddress;
    private int count;

    private final int maxBufferSize;

    BufferedSyncDataWriter(@NotNull final Log log,
                           @NotNull final DataWriter child,
                           @NotNull final LogTip page,
                           int maxBufferSize) {
        this.log = log;
        this.maxBufferSize = maxBufferSize;
        logCache = log.cache;
        this.blockSetMutable = page.blockSet.beginWrite();
        this.initialPage = page;
        this.child = child;
        this.highAddress = page.highAddress;
        final boolean validInitialPage = page.count >= 0;
        pageSize = log.getCachePageSize();
        if (validInitialPage) {
            if (pageSize != page.bytes.limit()) {
                throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + page.bytes.limit());
            }
            currentPage = new MutablePage(null, page.bytes, page.pageAddress, page.count);
        } else {
            currentPage = new MutablePage(null, logCache.allocPage(), page.pageAddress, 0);
        }
        cipherProvider = log.getConfig().getCipherProvider();
        cipherKey = log.getConfig().getCipherKey();
        cipherBasicIV = log.getConfig().getCipherBasicIV();
    }

    @NotNull
    public BlockSet.Mutable getBlockSetMutable() {
        return blockSetMutable;
    }

    public void setHighAddress(long highAddress) {
        allocLastPage(highAddress - (((int) highAddress) & (log.getCachePageSize() - 1))); // don't alloc full page
        this.highAddress = highAddress;
    }

    public MutablePage allocLastPage(long pageAddress) {
        if (count > 0) {
            commit();
        }

        MutablePage result = currentPage;
        if (pageAddress == result.pageAddress) {
            return result;
        }

        result = new MutablePage(null, logCache.allocPage(), pageAddress, 0);
        currentPage = result;
        return result;
    }

    public void write(byte b) {
        final int count = this.count;
        MutablePage currentPage = this.currentPage;

        final int writtenCount = currentPage.writtenCount;
        if (currentPage.pageAddress + writtenCount == 196608) {
            System.out.println();
        }
        if (writtenCount < pageSize) {
            currentPage.bytes.put(writtenCount, b);
            currentPage.writtenCount = writtenCount + 1;
        } else {
            currentPage = allocNewPage();
            currentPage.bytes.put(0, b);
            currentPage.writtenCount = 1;
        }
        this.count = count + 1;

        if (this.count >= maxBufferSize) {
            commit();
        }
    }

    public void write(ByteBuffer b, int len) throws ExodusException {
        int off = 0;
        final int count = this.count + len;
        MutablePage currentPage = this.currentPage;
        while (len > 0) {
            int bytesToWrite = pageSize - currentPage.writtenCount;
            if (bytesToWrite == 0) {
                currentPage = allocNewPage();
                bytesToWrite = pageSize;
            }
            if (bytesToWrite > len) {
                bytesToWrite = len;
            }
            currentPage.bytes.put(currentPage.writtenCount, b, off, bytesToWrite);
            currentPage.writtenCount += bytesToWrite;
            len -= bytesToWrite;
            off += bytesToWrite;
        }
        this.count = count;

        if (this.count >= maxBufferSize) {
            commit();
        }
    }

    private void commit() {
        var pagesCount = (count + pageSize - 1) / pageSize;
        count = 0;

        final MutablePage currentPage = this.currentPage;
        currentPage.committedCount = currentPage.writtenCount;
        MutablePage previousPage = currentPage.previousPage;

        if (previousPage != null) {
            MutablePage partiallyFlushedPage = null;

            final ArrayList<ByteBuffer> fullPages = new ArrayList<>(pagesCount);
            do {
                assert previousPage.bytes.remaining() == pageSize;
                if (previousPage.flushedCount > 0) {
                    assert partiallyFlushedPage == null;
                    partiallyFlushedPage = previousPage;
                } else {
                    assert partiallyFlushedPage == null;

                    if (cipherProvider != null) {
                        fullPages.add(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                                previousPage.pageAddress, previousPage.bytes, 0, pageSize,
                                LogUtil.LOG_BLOCK_ALIGNMENT));
                    } else {
                        fullPages.add(previousPage.bytes.duplicate());
                    }
                }
                cachePage(previousPage.bytes, previousPage.pageAddress);

                previousPage = previousPage.previousPage;

            } while (previousPage != null);

            if (partiallyFlushedPage != null) {
                final ByteBuffer bytes = partiallyFlushedPage.bytes;
                final int off = partiallyFlushedPage.flushedCount;
                final int len = pageSize - off;

                final long pageAddress = partiallyFlushedPage.pageAddress;
                if (cipherProvider == null) {
                    writePage(bytes, off, len);
                } else {
                    writePage(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                            pageAddress, bytes, off, len, LogUtil.LOG_BLOCK_ALIGNMENT), 0, len);
                }
            }

            if (!fullPages.isEmpty()) {
                Collections.reverse(fullPages);
                writePages(fullPages.toArray(new ByteBuffer[0]));
            }

            currentPage.previousPage = null;
        }
    }

    public void flush() {
        if (count > 0) {
            commit();
        }

        final MutablePage currentPage = this.currentPage;
        final int committedCount = currentPage.committedCount;
        final int flushedCount = currentPage.flushedCount;

        if (committedCount > flushedCount) {
            final ByteBuffer bytes = currentPage.bytes;
            final int len = committedCount - flushedCount;
            final long pageAddress = currentPage.pageAddress;
            if (cipherProvider == null) {
                writePage(bytes, flushedCount, len);
            } else {
                writePage(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                        pageAddress, bytes, flushedCount, len, LogUtil.LOG_BLOCK_ALIGNMENT), 0, len);
            }
            if (committedCount == pageSize) {
                cachePage(bytes, pageAddress);
            }

            currentPage.flushedCount = committedCount;
        }
    }

    public Block openOrCreateBlock(long address, long length) {
        return child.openOrCreateBlock(address, length);
    }

    public void setLastPageLength(int lastPageLength) {
        currentPage.setCounts(lastPageLength);
    }

    public int getLastPageLength() {
        return currentPage.writtenCount;
    }

    public long getLastWrittenFileLength(long fileLengthBound) {
        return highAddress % fileLengthBound;
    }

    @NotNull
    public LogTip getStartingTip() {
        return initialPage;
    }

    @Override
    public void incHighAddress(long delta) {
        highAddress += delta;
    }

    @Override
    public long getHighAddress() {
        return highAddress;
    }

    @NotNull
    public LogTip getUpdatedTip() {
        final MutablePage currentPage = this.currentPage;
        final BlockSet.Immutable blockSetImmutable = blockSetMutable.endWrite();
        return new LogTip(currentPage.bytes, currentPage.pageAddress, currentPage.committedCount, highAddress, highAddress, blockSetImmutable);
    }

    public byte getByte(long address, byte max) {
        final int offset = ((int) address) & (pageSize - 1);
        final long pageAddress = address - offset;
        final MutablePage page = getWrittenPage(pageAddress);
        if (page != null) {
            final byte result = (byte) (page.bytes.get(offset) ^ 0x80);
            if (result < 0 || result > max) {
                throw new IllegalStateException("Unknown written page loggable type: " + result);
            }
            return result;
        }

        // slow path: unconfirmed file saved to disk, read byte from it
        final long fileAddress = log.getFileAddress(address);
        if (!blockSetMutable.contains(fileAddress)) {
            BlockNotFoundException.raise("Address is out of log space, underflow", log, address);
        }

        final ByteBuffer output = LogUtil.allocatePage(pageSize);

        final Block block = blockSetMutable.getBlock(fileAddress);

        final int readBytes = block.read(output, pageAddress - fileAddress, 0, output.limit());

        if (readBytes < offset) {
            throw new ExodusException("Can't read expected page bytes");
        }

        if (cipherProvider != null) {
            EnvKryptKt.cryptBlocksMutable(cipherProvider, cipherKey, cipherBasicIV, pageAddress, output, 0, readBytes, LogUtil.LOG_BLOCK_ALIGNMENT);
        }

        final byte result = (byte) (output.get(offset) ^ 0x80);
        if (result < 0 || result > max) {
            throw new IllegalStateException("Unknown written file loggable type: " + result + ", address: " + address);
        }
        return result;
    }

    private void writePage(@NotNull final ByteBuffer bytes, final int off, final int len) {
        final Block block = child.write(bytes, off, len);
        blockSetMutable.add(block.getAddress(), block);
    }

    private void writePages(@NotNull final ByteBuffer[] buffers) {
        final Block block = child.write(buffers);
        blockSetMutable.add(block.getAddress(), block);
    }

    private void cachePage(@NotNull final ByteBuffer bytes, final long pageAddress) {
        logCache.cachePage(log, pageAddress, bytes);
    }

    // warning: this method is O(N), where N is number of added pages
    private MutablePage getWrittenPage(long alignedAddress) {
        MutablePage currentPage = this.currentPage;
        do {
            final long highPageAddress = currentPage.pageAddress;
            if (alignedAddress == highPageAddress) {
                return currentPage;
            }
            currentPage = currentPage.previousPage;
        } while (currentPage != null);
        return null;
    }

    private MutablePage allocNewPage() {
        MutablePage currentPage = this.currentPage;
        return this.currentPage = new MutablePage(currentPage, logCache.allocPage(), currentPage.pageAddress + pageSize, 0);
    }
}
