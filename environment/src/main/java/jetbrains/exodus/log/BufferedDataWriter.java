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
package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.core.dataStructures.LongIntPair;
import jetbrains.exodus.core.dataStructures.LongObjectBifFunction;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.RemoveBlockType;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class BufferedDataWriter {
    public static final long XX_HASH_SEED = 0xADEF1279AL;
    public static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestJavaInstance();
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();
    public static final int HASH_CODE_SIZE = Long.BYTES;
    public static final int FIRST_ITERABLE_OFFSET_SIZE = Integer.BYTES;
    public static final int FIRST_ITERABLE_OFFSET = HASH_CODE_SIZE + FIRST_ITERABLE_OFFSET_SIZE;
    public static final int LOGGABLE_DATA = FIRST_ITERABLE_OFFSET;
    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    private final DataWriter writer;
    private LogTip initialPage;
    @Nullable
    private final StreamCipherProvider cipherProvider;
    private final byte[] cipherKey;
    private final long cipherBasicIV;
    private final int pageSize;
    private final int pageSizeMask;
    private final int adjustedPageSize;
    private BlockSet.Mutable blockSetMutable;
    private final boolean calculateHashCode;
    private final Semaphore writeBoundarySemaphore;
    private final Semaphore localWritesSemaphore;

    private final BiConsumer<LongIntPair, ? super Throwable> writeCompletionHandler;

    private final LongObjectBifFunction<PageHolder, PageHolder> writeCacheFlushCompute;

    private MutablePage currentPage;

    private long highAddress;

    private volatile Throwable writeError;

    private final NonBlockingHashMapLong<PageHolder> writeCache;

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter writer,
                       final boolean calculateHashCode,
                       final Semaphore writeBoundarySemaphore,
                       final int maxWriteBoundary, LogTip logTip) {
        this.log = log;
        this.writer = writer;

        logCache = log.cache;
        this.calculateHashCode = calculateHashCode;

        pageSize = log.getCachePageSize();


        cipherProvider = log.getConfig().getCipherProvider();
        cipherKey = log.getConfig().getCipherKey();
        cipherBasicIV = log.getConfig().getCipherBasicIV();

        pageSizeMask = (pageSize - 1);
        adjustedPageSize = pageSize - BufferedDataWriter.LOGGABLE_DATA;

        this.writeCache = new NonBlockingHashMapLong<>(maxWriteBoundary, false);

        this.writeBoundarySemaphore = writeBoundarySemaphore;
        this.localWritesSemaphore = new Semaphore(Integer.MAX_VALUE);

        initCurrentPage(logTip);

        this.writeCompletionHandler = (positionWrittenPair, err) -> {
            var position = positionWrittenPair.first;
            var written = positionWrittenPair.second;

            var pageOffset = position & (pageSize - 1);
            var pageAddress = position - pageOffset;

            computeWriteCache(writeCache, pageAddress, (pa, pageHolder) -> {
                var writtenInPage = Objects.requireNonNull(pageHolder).written;
                var result = writtenInPage.addAndGet(written);

                if (result == pageSize) {
                    return null;
                }

                return pageHolder;
            });

            if (err != null) {
                writeError = err;
            }

            writeBoundarySemaphore.release();
            localWritesSemaphore.release();
        };

        this.writeCacheFlushCompute = (pa, holder) -> {
            if (holder == null) {
                return new PageHolder(currentPage.bytes);
            }

            holder.page = currentPage.bytes;
            return holder;
        };
    }


    public void beforeWrite(LogTip logTip) {
        checkWriteError();

        this.blockSetMutable = logTip.blockSet.beginWrite();
        this.initialPage = logTip;

        if (this.highAddress != logTip.highAddress) {
            throw new ExodusException("Position of the writer " + highAddress + " and postion of log tip " +
                    logTip.highAddress + " are not synchronized");
        }

        if (currentPage == null) {
            throw new ExodusException("Current page is not initialized in the buffered writer.");
        }

        assert highAddress % pageSize == currentPage.writtenCount % pageSize;
    }

    private void initCurrentPage(LogTip logTip) {
        this.initialPage = logTip;
        this.highAddress = logTip.highAddress;

        final boolean validInitialPage = logTip.count >= 0;

        if (validInitialPage) {
            if (pageSize != logTip.bytes.length) {
                throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + logTip.bytes.length);
            }

            currentPage = new MutablePage(logTip.bytes, logTip.pageAddress, logTip.count);
            currentPage.xxHash64 = BufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(
                    BufferedDataWriter.XX_HASH_SEED);
            currentPage.xxHash64.update(logTip.bytes, 0, logTip.count);
            currentPage.firstLoggable =
                    BindingUtils.readInt(logTip.bytes, pageSize - BufferedDataWriter.LOGGABLE_DATA);
        } else {
            byte[] newPage = new byte[pageSize];
            BindingUtils.writeInt(-1, newPage, pageSize - BufferedDataWriter.LOGGABLE_DATA);
            currentPage = new MutablePage(newPage, logTip.pageAddress, 0);
            currentPage.xxHash64 = BufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(
                    BufferedDataWriter.XX_HASH_SEED);
        }

        assert highAddress % pageSize == currentPage.writtenCount % pageSize;
    }

    public LogTip endWrite() {
        checkWriteError();

        final MutablePage currentPage = this.currentPage;
        final BlockSet.Immutable blockSetImmutable = blockSetMutable.endWrite();

        this.blockSetMutable = null;
        this.initialPage = null;

        assert highAddress % pageSize == currentPage.writtenCount % pageSize;

        return new LogTip(currentPage.bytes, currentPage.pageAddress,
                currentPage.writtenCount, highAddress, highAddress,
                blockSetImmutable);
    }

    public static void checkPageConsistency(long pageAddress, byte @NotNull [] bytes, int pageSize, Log log) {
        if (pageSize != bytes.length) {
            DataCorruptionException.raise("Unexpected page size (bytes). {expected " + pageSize
                    + ": , actual : " + bytes.length + "}", log, pageAddress);
        }

        final XXHash64 xxHash = BufferedDataWriter.xxHash;
        final long calculatedHash = xxHash.hash(bytes, 0,
                bytes.length - HASH_CODE_SIZE, XX_HASH_SEED);
        final long storedHash = BindingUtils.readLong(bytes, pageSize - HASH_CODE_SIZE);

        if (storedHash != calculatedHash) {
            DataCorruptionException.raise("Page is broken. Expected and calculated hash codes are different.",
                    log, pageAddress);
        }
    }

    public static void updateHashCode(final byte @NotNull [] bytes) {
        final int hashCodeOffset = bytes.length - HASH_CODE_SIZE;
        final long hash =
                xxHash.hash(bytes, 0, hashCodeOffset,
                        XX_HASH_SEED);

        BindingUtils.writeLong(hash, bytes, hashCodeOffset);
    }

    public static void writeFirstLoggableOffset(final byte @NotNull [] bytes, int offset) {
        BindingUtils.writeInt(offset, bytes, bytes.length - LOGGABLE_DATA);
    }

    public static byte[] generateNullPage(int pageSize) {
        final byte[] data = new byte[pageSize];
        Arrays.fill(data, 0, pageSize - LOGGABLE_DATA, (byte) 0x80);

        final long hash = xxHash.hash(data, 0, pageSize - HASH_CODE_SIZE, XX_HASH_SEED);
        BindingUtils.writeLong(hash, data, pageSize - HASH_CODE_SIZE);

        return data;
    }

    void write(byte b, long firstLoggable) {
        checkWriteError();

        MutablePage currentPage = allocateNewPageIfNeeded();

        int delta = 1;
        int writtenCount = currentPage.writtenCount;
        assert (int) (highAddress & pageSizeMask) == (writtenCount & pageSizeMask);

        assert writtenCount < adjustedPageSize;
        currentPage.bytes[writtenCount] = b;

        writtenCount++;
        currentPage.writtenCount = writtenCount;

        if (writtenCount == adjustedPageSize) {
            currentPage.writtenCount = pageSize;
            delta += LOGGABLE_DATA;
        }

        highAddress += delta;
        writeFirstLoggableOffset(firstLoggable, currentPage);

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);

        if (currentPage.writtenCount == pageSize) {
            writePage(currentPage);
        }
    }

    void write(byte[] b, int offset, int len) {
        checkWriteError();

        int off = 0;
        int delta = len;

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);

        while (len > 0) {
            MutablePage currentPage = allocateNewPageIfNeeded();
            final int bytesToWrite = Math.min(adjustedPageSize - currentPage.writtenCount, len);

            System.arraycopy(b, offset + off, currentPage.bytes,
                    currentPage.writtenCount, bytesToWrite);

            currentPage.writtenCount += bytesToWrite;

            if (currentPage.writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize;
                delta += LOGGABLE_DATA;

                writePage(currentPage);
            }

            len -= bytesToWrite;
            off += bytesToWrite;
        }

        this.highAddress += delta;

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);
    }

    boolean fitsIntoSingleFile(long fileLengthBound, int loggableSize) {
        final long fileAddress = highAddress / fileLengthBound;
        final long nextFileAddress =
                (log.adjustLoggableAddress(highAddress, loggableSize) - 1) / fileLengthBound;

        return fileAddress == nextFileAddress;
    }


    byte[] readPage(final long pageAddress) {
        var holder = writeCache.get(pageAddress);
        if (holder != null) {
            return holder.page;
        }

        var page = new byte[pageSize];
        log.readBytes(page, pageAddress);

        return page;
    }

    byte[] getCurrentlyWritten(final long pageAddress) {
        if (currentPage.pageAddress == pageAddress) {
            return currentPage.bytes;
        }

        return null;
    }

    void removeBlock(final long blockAddress, final RemoveBlockType rbt) {
        writer.removeBlock(blockAddress, rbt);
    }

    void forgetFiles(final long[] files, long fileBoundary) {
        boolean waitForCompletion = false;
        for (final long file : files) {
            blockSetMutable.remove(file);

            if (!waitForCompletion) {
                final long fileEnd = file + fileBoundary;
                for (long pageAddress = file; pageAddress < fileEnd; pageAddress += pageSize) {
                    if (writeCache.containsKey(pageAddress)) {
                        waitForCompletion = true;
                    }
                }
            }
        }

        if (waitForCompletion) {
            ensureWritesAreCompleted();
        }
    }

    void flush() {
        checkWriteError();
        if (currentPage.committedCount < pageSize) {
            computeWriteCache(writeCache, currentPage.pageAddress, writeCacheFlushCompute);
        }

    }

    private void checkWriteError() {
        if (writeError != null) {
            throw ExodusException.toExodusException(writeError);
        }
    }

    int padPageWithNulls() {
        checkWriteError();

        final int written = doPadPageWithNulls();
        this.highAddress += written;

        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);

        if (written > 0) {
            assert currentPage.writtenCount == pageSize;
            writePage(currentPage);
        }

        return written;
    }

    void padWholePageWithNulls() {
        checkWriteError();

        final int written = doPadWholePageWithNulls();

        if (written > 0) {
            writePage(currentPage);
            highAddress += written;
        }
    }

    boolean padWithNulls(long fileLengthBound, byte[] nullPage) {
        checkWriteError();

        assert nullPage.length == pageSize;
        int written = doPadPageWithNulls();

        if (written > 0) {
            assert currentPage.writtenCount == pageSize;
            writePage(currentPage);
        }

        final long spaceWritten = ((highAddress + written) % fileLengthBound);
        if (spaceWritten == 0) {
            highAddress += written;

            assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);
            return written > 0;
        }

        final long reminder = fileLengthBound - spaceWritten;
        final long pages = reminder / pageSize;

        assert reminder % pageSize == 0;

        for (int i = 0; i < pages; i++) {
            var currentPage = allocNewPage(nullPage);
            writePage(currentPage);

            written += pageSize;
        }

        highAddress += written;
        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);
        return written > 0;
    }

    private MutablePage allocateNewPageIfNeeded() {
        var currentPage = this.currentPage;
        if (currentPage.writtenCount == pageSize) {
            return allocNewPage();
        }

        return currentPage;
    }

    void sync() {
        var currentPage = this.currentPage;

        if (currentPage.writtenCount > currentPage.committedCount) {
            writePage(currentPage);
        }

        //wait till all local writes will be flushed to the disk
        ensureWritesAreCompleted();

        checkWriteError();

        writer.sync();
    }

    void close() {
        sync();

        writer.close();

        writeCache.clear();
        highAddress = 0;

        if (currentPage != null) {
            currentPage.xxHash64.close();
            currentPage = null;
        }

        blockSetMutable = null;
    }

    int getFilesSize() {
        return blockSetMutable.size();
    }

    void release() {
        writer.release();
    }

    void clear() {
        ensureWritesAreCompleted();

        writeCache.clear();

        highAddress = 0;
        if (currentPage != null) {
            currentPage.xxHash64.close();
        }

        currentPage = new MutablePage(new byte[pageSize], 0, 0);
        currentPage.xxHash64 = BufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(
                BufferedDataWriter.XX_HASH_SEED);

        blockSetMutable = null;

        writer.clear();
    }

    private void ensureWritesAreCompleted() {
        localWritesSemaphore.acquireUninterruptibly(Integer.MAX_VALUE);
        localWritesSemaphore.release(Integer.MAX_VALUE);

        assert assertWriteCompletedWriteCache();
    }

    private boolean assertWriteCompletedWriteCache() {
        assert writeCache.size() <= 1;

        if (writeCache.size() == 1) {
            var entry = writeCache.entrySet().iterator().next();
            assert entry.getKey().longValue() == currentPage.pageAddress;
            assert entry.getValue().written.get() < pageSize;
        }

        return true;
    }

    private void writePage(MutablePage page) {
        byte[] bytes = page.bytes;

        final StreamingXXHash64 xxHash64 = page.xxHash64;
        final long pageAddress = page.pageAddress;

        final int writtenCount = page.writtenCount;
        final int committedCount = page.committedCount;

        final int len = writtenCount - committedCount;

        if (len > 0) {
            final int contentLen;
            if (writtenCount < pageSize) {
                contentLen = len;
            } else {
                contentLen = len - HASH_CODE_SIZE;
            }

            byte[] encryptedBytes = null;
            if (cipherProvider == null) {
                if (calculateHashCode) {
                    xxHash64.update(bytes, committedCount, contentLen);

                    if (writtenCount == pageSize) {
                        BindingUtils.writeLong(xxHash64.getValue(), bytes,
                                adjustedPageSize + FIRST_ITERABLE_OFFSET_SIZE);
                    }
                }
            } else {
                encryptedBytes = EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey,
                        cipherBasicIV, pageAddress, bytes, committedCount, len, LogUtil.LOG_BLOCK_ALIGNMENT);

                if (calculateHashCode) {
                    xxHash64.update(encryptedBytes, 0, contentLen);

                    if (writtenCount == pageSize) {
                        BindingUtils.writeLong(xxHash64.getValue(), encryptedBytes, contentLen);
                    }
                }
            }

            logCache.cachePage(log, pageAddress, bytes);

            writeBoundarySemaphore.acquireUninterruptibly();
            localWritesSemaphore.acquireUninterruptibly();

            assert writer.position() == currentPage.pageAddress % log.getFileLengthBound()
                    + currentPage.committedCount;

            Pair<Block, CompletableFuture<LongIntPair>> result;
            if (cipherProvider != null) {
                result = writer.asyncWrite(encryptedBytes, 0, len);
            } else {
                result = writer.asyncWrite(bytes, committedCount, len);
            }

            assert writer.position() == currentPage.pageAddress % log.getFileLengthBound()
                    + currentPage.writtenCount;

            var block = result.getFirst();
            blockSetMutable.add(block.getAddress(), block);

            computeWriteCache(writeCache, pageAddress, (pa, holder) -> {
                if (holder == null) {
                    return new PageHolder(bytes);
                }

                holder.page = bytes;
                return holder;
            });

            page.committedCount = page.writtenCount;
            result.getSecond().whenComplete(writeCompletionHandler);
        }
    }


    private MutablePage allocNewPage() {
        MutablePage currentPage = this.currentPage;
        currentPage.xxHash64.close();

        currentPage = this.currentPage = new MutablePage(new byte[pageSize],
                currentPage.pageAddress + pageSize, 0);
        currentPage.xxHash64 = XX_HASH_FACTORY.newStreamingHash64(XX_HASH_SEED);

        return currentPage;
    }

    private MutablePage allocNewPage(byte[] pageData) {
        assert pageData.length == pageSize;
        MutablePage currentPage = this.currentPage;
        currentPage.xxHash64.close();

        return this.currentPage = new MutablePage(pageData,
                currentPage.pageAddress + pageSize, pageData.length);
    }

    long getHighAddress() {
        return highAddress;
    }

    boolean closeFileIfNecessary(long fileLengthBound, boolean makeFileReadOnly) {
        if (writer.position() == fileLengthBound) {
            sync();

            writer.close();

            var blockSet = blockSetMutable;
            var lastFile = blockSet.getMaximum();
            if (lastFile != null) {
                var lastFileAddress = lastFile.longValue();
                var block = blockSet.getBlock(lastFileAddress);

                var refreshed = block.refresh();
                if (block != refreshed) {
                    blockSet.add(lastFileAddress, refreshed);
                }

                var length = refreshed.length();
                if (length < fileLengthBound) {
                    throw new IllegalStateException(
                            "File's too short (" + LogUtil.getLogFilename(lastFileAddress)
                                    + "), block.length() = " + length + ", fileLengthBound = " + fileLengthBound
                    );
                }

                if (makeFileReadOnly && block instanceof File) {
                    //noinspection ResultOfMethodCallIgnored
                    ((File) block).setReadOnly();
                }
            }

            return true;
        }

        return false;
    }

    void openNewFileIfNeeded(long fileLengthBound, Log log) {
        if (!this.writer.isOpen()) {
            var fileAddress = highAddress - highAddress % fileLengthBound;
            var block = writer.openOrCreateBlock(fileAddress, highAddress % fileLengthBound);

            boolean fileCreated = !blockSetMutable.contains(fileAddress);
            if (fileCreated) {
                blockSetMutable.add(fileAddress, block);
                // fsync the directory to ensure we will find the log file in the directory after system crash
                writer.syncDirectory();
                log.notifyBlockCreated(fileAddress);
            } else {
                log.notifyBlockModified(fileAddress);
            }
        }
    }

    @NotNull
    LogTip getStartingTip() {
        return initialPage;
    }

    private void writeFirstLoggableOffset(long firstLoggable, BufferedDataWriter.MutablePage currentPage) {
        if (firstLoggable >= 0 && currentPage.firstLoggable < 0) {
            int loggableOffset = (int) (firstLoggable & pageSizeMask);

            currentPage.firstLoggable = loggableOffset;
            BufferedDataWriter.writeFirstLoggableOffset(currentPage.bytes, loggableOffset);
        }
    }

    private int doPadWholePageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;

        if (writtenInPage > 0) {
            final int written = pageSize - writtenInPage;

            Arrays.fill(currentPage.bytes, writtenInPage, pageSize, (byte) 0x80);

            currentPage.writtenCount = pageSize;
            highAddress += written;

            return written;
        }

        return 0;
    }

    private int doPadPageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;
        if (writtenInPage > 0) {
            final int pageDelta = adjustedPageSize - writtenInPage;

            int written = 0;
            if (pageDelta > 0) {
                Arrays.fill(currentPage.bytes, writtenInPage, adjustedPageSize, (byte) 0x80);
                currentPage.writtenCount = pageSize;

                written = pageDelta + BufferedDataWriter.LOGGABLE_DATA;
            }

            return written;
        } else {
            return 0;
        }
    }

    /**
     * Implementation of compute to avoid boxing/unboxing.
     */
    private static void computeWriteCache(NonBlockingHashMapLong<PageHolder> writeCache,
                                          final long pageAddress,
                                          final LongObjectBifFunction<PageHolder, PageHolder> remappingFunction) {
        retry:
        for (; ; ) {
            PageHolder oldValue = writeCache.get(pageAddress);

            for (; ; ) {
                final PageHolder newValue = remappingFunction.apply(pageAddress, oldValue);
                if (newValue != null) {
                    if (oldValue != null) {
                        if (writeCache.replace(pageAddress, oldValue, newValue)) {
                            return;
                        }
                    } else if ((oldValue = writeCache.putIfAbsent(pageAddress, newValue)) == null) {
                        return;
                    } else {
                        continue;
                    }
                } else if (oldValue == null || writeCache.remove(pageAddress, oldValue)) {
                    return;
                }

                continue retry;
            }
        }
    }

    private static class MutablePage {
        private final byte @NotNull [] bytes;
        private final long pageAddress;

        int committedCount;
        int writtenCount;
        int firstLoggable;

        StreamingXXHash64 xxHash64;

        MutablePage(final byte @NotNull [] page,
                    final long pageAddress, final int count) {
            this.bytes = page;
            this.pageAddress = pageAddress;
            committedCount = writtenCount = count;
            this.firstLoggable = -1;
        }
    }

    private static final class PageHolder {
        private volatile byte @NotNull [] page;
        private final AtomicInteger written;

        private PageHolder(final byte @NotNull [] page) {
            this.page = page;
            this.written = new AtomicInteger();
        }
    }
}
