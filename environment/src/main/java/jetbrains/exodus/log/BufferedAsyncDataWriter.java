/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.log;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.execution.locks.Semaphore;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.AsyncDataWriter;
import jetbrains.exodus.io.Block;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class BufferedAsyncDataWriter implements BufferedDataWriter {
    // immutable state
    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    @NotNull
    private final AsyncDataWriter child;
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
    private final Semaphore bufferUsed;

    private final ConcurrentLinkedQueue<PageWriteResult> writeCompletionResults = new ConcurrentLinkedQueue<>();

    BufferedAsyncDataWriter(@NotNull final Log log,
                            @NotNull final AsyncDataWriter child,
                            @NotNull final LogTip page,
                            int maxBufferSize) {
        this.log = log;
        this.maxBufferSize = maxBufferSize;
        this.bufferUsed = new Semaphore(maxBufferSize);

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

    @Override
    public Pair<ByteBuffer, ByteBuffer> allocatePage(int size) {
        var written = currentPage.writtenCount;
        var buffer = currentPage.bytes;
        var alignmentOffset = buffer.alignmentOffset(written, Long.BYTES);

        int padding;
        if (alignmentOffset == 0) {
            padding = 0;
        } else {
            padding = Long.BYTES - alignmentOffset;
        }

        if (written + padding + size <= pageSize) {
            count += padding;
            currentPage.writtenCount += padding;

            if (padding == 0) {
                return new ObjectObjectImmutablePair<>(
                        buffer.slice(written, pageSize - written).order(ByteOrder.nativeOrder()), null);
            } else {
                var offset = written + padding;
                return new ObjectObjectImmutablePair<>(buffer.slice(offset, pageSize - offset).
                        order(ByteOrder.nativeOrder()),
                        buffer.slice(written, padding));
            }
        }

        var fileLengthBound = log.getFileLengthBound();
        if ((highAddress % fileLengthBound) + pageSize >= fileLengthBound) {
            return null;
        }

        var prevWritten = currentPage.writtenCount;
        var prevBuffer = currentPage.bytes;

        currentPage.writtenCount = pageSize;

        currentPage = allocNewPage(null);
        buffer = currentPage.bytes;

        assert buffer.alignmentOffset(0, Long.BYTES) == 0;

        count += pageSize - prevWritten;

        if (prevWritten < pageSize) {
            return new ObjectObjectImmutablePair<>(buffer,
                    prevBuffer.slice(prevWritten, pageSize - prevWritten));
        } else {
            return new ObjectObjectImmutablePair<>(buffer,
                    null);
        }
    }

    @Override
    public void finishPageWrite(int size) {
        count += size;
        currentPage.writtenCount += size;

        if (count >= pageSize) {
            commit();
        }
    }

    public void setHighAddress(long highAddress) {
        allocLastPage(highAddress - (((int) highAddress) & (log.getCachePageSize() - 1))); // don't alloc full page
        this.highAddress = highAddress;
    }

    public void allocLastPage(long pageAddress) {
        if (count > 0) {
            commit();
        }

        MutablePage result = currentPage;
        if (pageAddress == result.pageAddress) {
            return;
        }

        result = new MutablePage(null, logCache.allocPage(), pageAddress, 0);
        currentPage = result;
    }

    public void write(byte b) {
        final int count = this.count;
        MutablePage currentPage = this.currentPage;

        final int writtenCount = currentPage.writtenCount;
        if (writtenCount < pageSize) {
            currentPage.bytes.put(writtenCount, b);
            currentPage.writtenCount = writtenCount + 1;
        } else {
            currentPage = allocNewPage(null);
            currentPage.bytes.put(0, b);
            currentPage.writtenCount = 1;
        }
        this.count = count + 1;

        if (this.count >= pageSize) {
            commit();
        }
    }

    public void write(ByteBuffer b, int len, boolean canBeConsumed) throws ExodusException {
        int off = 0;
        final int count = this.count + len;
        MutablePage currentPage = this.currentPage;
        while (len > 0) {
            int bytesToWrite = pageSize - currentPage.writtenCount;

            boolean pageConsumed = false;

            if (bytesToWrite == 0) {
                if (canBeConsumed && len == pageSize && pageCanBeConsumed(b)) {
                    currentPage = allocNewPage(b);
                    currentPage.writtenCount = pageSize;
                    pageConsumed = true;
                    len = 0;
                } else {
                    currentPage = allocNewPage(null);
                    bytesToWrite = pageSize;
                }
            }

            if (!pageConsumed) {
                if (bytesToWrite > len) {
                    bytesToWrite = len;
                }
                currentPage.bytes.put(currentPage.writtenCount, b, off, bytesToWrite);
                currentPage.writtenCount += bytesToWrite;
                len -= bytesToWrite;
                off += bytesToWrite;
            }
        }

        this.count = count;

        if (this.count >= pageSize) {
            commit();
        }
    }

    private boolean pageCanBeConsumed(ByteBuffer buffer) {
        return buffer.limit() == pageSize && buffer.alignmentOffset(0, Long.BYTES) == 0;
    }

    private void commit() {
        var pagesCount = (count + pageSize - 1) / pageSize;

        final MutablePage currentPage = this.currentPage;
        currentPage.committedCount = currentPage.writtenCount;
        count = currentPage.committedCount;

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
                        fullPages.add(previousPage.bytes);
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
                    var encryptedPage = EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                            pageAddress, bytes, off, len, LogUtil.LOG_BLOCK_ALIGNMENT);
                    writePage(encryptedPage, 0, len);
                }
            }

            if (!fullPages.isEmpty()) {
                Collections.reverse(fullPages);
                for (final ByteBuffer buffer : fullPages) {
                    writePage(buffer, 0, buffer.limit());
                }
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

        waitTillWriteTasksAreCompleted();
    }

    public Block openOrCreateBlock(long address, long length) {
        return child.openOrCreateBlock(address, length);
    }

    public long getLastWrittenFileLength(long fileLengthBound) {
        return highAddress % fileLengthBound;
    }

    @NotNull
    public LogTip getStartingTip() {
        return initialPage;
    }

    public void incHighAddress(long delta) {
        highAddress += delta;
    }

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

        ByteBuffer bytes;
        if (page != null) {
            bytes = page.bytes;
        } else {
            bytes = logCache.getCachedPage(log, pageAddress, false);
        }

        if (bytes != null) {
            final byte result = (byte) (bytes.get(offset) ^ 0x80);
            if (result < 0 || result > max) {
                throw new IllegalStateException("Unknown written page loggable type: " + result);
            }
            return result;
        }

        // slow path: unconfirmed file saved to disk, read byte from it

        //wait till all write tasks are completed
        waitTillWriteTasksAreCompleted();
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

    private void waitTillWriteTasksAreCompleted() {
        bufferUsed.acquire(maxBufferSize);
        bufferUsed.release(maxBufferSize);
        consumeWrittenBlocks();
    }

    private void writePage(@NotNull final ByteBuffer bytes, final int off, final int len) {
        bufferUsed.acquire(len);
        var writeTask = child.writeAsync(bytes, off, len);

        writeTask.whenComplete((block, throwable) -> {
            bufferUsed.release(len);
            writeCompletionResults.add(new PageWriteResult(block, throwable));
        });

        consumeWrittenBlocks();
    }

    private void consumeWrittenBlocks() {
        var resultsIter = writeCompletionResults.iterator();

        while (resultsIter.hasNext()) {
            var result = resultsIter.next();

            if (result.block != null) {
                var block = result.block;
                blockSetMutable.add(block.getAddress(), block);
            } else {
                throw ExodusException.toExodusException(result.ex);
            }

            resultsIter.remove();
        }
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

    private MutablePage allocNewPage(ByteBuffer buffer) {
        MutablePage currentPage = this.currentPage;
        if (buffer != null) {
            this.currentPage = new MutablePage(currentPage, buffer.order(ByteOrder.nativeOrder()),
                    currentPage.pageAddress + pageSize, 0);
        } else {
            this.currentPage = new MutablePage(currentPage, logCache.allocPage(),
                    currentPage.pageAddress + pageSize, 0);
        }

        return this.currentPage;
    }

    private static final class PageWriteResult {
        private final Block block;
        private final Throwable ex;

        private PageWriteResult(Block block, Throwable ex) {
            this.block = block;
            this.ex = ex;
        }
    }
}
