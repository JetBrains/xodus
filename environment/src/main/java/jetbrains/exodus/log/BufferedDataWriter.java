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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.core.dataStructures.LongIntPair;
import jetbrains.exodus.core.dataStructures.LongObjectBifFunction;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.*;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class BufferedDataWriter {
    private static final Logger logger = LoggerFactory.getLogger(BufferedDataWriter.class);

    public static final long XX_HASH_SEED = 0xADEF1279AL;
    public static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestJavaInstance();
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();
    public static final int HASH_CODE_SIZE = Long.BYTES;
    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    private final DataWriter writer;
    @Nullable
    private final StreamCipherProvider cipherProvider;
    private final byte[] cipherKey;
    private final long cipherBasicIV;
    private final int pageSize;
    private final int pageSizeMask;
    private final int adjustedPageSize;
    private @Nullable BlockSet.Mutable blockSetMutable;

    private boolean blockSetWasChanged;

    private final boolean calculateHashCode;
    private final Semaphore writeBoundarySemaphore;
    private final Semaphore localWritesSemaphore;

    private final BiConsumer<LongIntPair, ? super Throwable> writeCompletionHandler;

    private final LongObjectBifFunction<PageHolder, PageHolder> writeCacheFlushCompute;

    private MutablePage currentPage;

    private long currentHighAddress;

    private volatile long committedHighAddress;
    private volatile BlockSet.Immutable blockSet;

    private volatile Throwable writeError;

    private volatile long lastSyncedAddress;
    private volatile long lastSyncedTs;

    private final long syncPeriod;

    private final NonBlockingHashMapLong<PageHolder> writeCache;

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter writer,
                       final boolean calculateHashCode,
                       final Semaphore writeBoundarySemaphore,
                       final int maxWriteBoundary,
                       final BlockSet.Immutable files,
                       final long highAddress,
                       final byte[] page,
                       final long syncPeriod) {
        this.log = log;
        this.writer = writer;

        logCache = log.cache;
        this.calculateHashCode = calculateHashCode;

        pageSize = log.getCachePageSize();


        cipherProvider = log.getConfig().getCipherProvider();
        cipherKey = log.getConfig().getCipherKey();
        cipherBasicIV = log.getConfig().getCipherBasicIV();
        this.syncPeriod = syncPeriod;

        pageSizeMask = (pageSize - 1);
        adjustedPageSize = pageSize - HASH_CODE_SIZE;

        this.writeCache = new NonBlockingHashMapLong<>(maxWriteBoundary, false);

        this.writeBoundarySemaphore = writeBoundarySemaphore;
        this.localWritesSemaphore = new Semaphore(Integer.MAX_VALUE);

        initCurrentPage(files, highAddress, page);

        this.writeCompletionHandler = (positionWrittenPair, err) -> {
            var position = positionWrittenPair.first;
            var written = positionWrittenPair.second;

            var pageOffset = position & (pageSize - 1);
            var pageAddress = position - pageOffset;

            var pageHolder = writeCache.get(pageAddress);
            assert pageHolder != null;

            var writtenInPage = pageHolder.written;
            var result = writtenInPage.addAndGet(written);

            if (result == pageSize) {
                writeCache.remove(pageAddress);
            }

            if (err != null) {
                writeError = err;
                logger.error("Error during writing of data to the file for the log " +
                                log.getLocation(),
                        err);
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


    public long beforeWrite() {
        checkWriteError();

        if (currentPage == null) {
            throw new ExodusException("Current page is not initialized in the buffered writer.");
        }

        blockSetMutable = blockSet.beginWrite();
        blockSetWasChanged = false;

        assert currentHighAddress == committedHighAddress;
        assert currentHighAddress % pageSize == currentPage.writtenCount % pageSize;
        return currentHighAddress;
    }

    private void initCurrentPage(final BlockSet.Immutable files,
                                 final long highAddress,
                                 final byte[] page) {
        this.currentHighAddress = highAddress;
        this.committedHighAddress = highAddress;

        if (pageSize != page.length) {
            throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                    pageSize + ", actual page size = " + page.length);
        }

        final int pageOffset = (int) highAddress & (pageSize - 1);
        final long pageAddress = highAddress - pageOffset;

        currentPage = new MutablePage(page, pageAddress, pageOffset);
        var xxHash64 =
                BufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(BufferedDataWriter.XX_HASH_SEED);
        currentPage.xxHash64 = xxHash64;

        if (calculateHashCode && pageOffset < pageSize) {
            if (cipherProvider != null) {
                byte[] encryptedBytes = EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey,
                        cipherBasicIV, pageAddress, page, 0, pageOffset, LogUtil.LOG_BLOCK_ALIGNMENT);
                xxHash64.update(encryptedBytes, 0, encryptedBytes.length);
            } else {
                xxHash64.update(page, 0, pageOffset);
            }
        }

        currentPage.firstLoggable =
                BindingUtils.readInt(page, pageSize - BufferedDataWriter.HASH_CODE_SIZE);
        blockSet = files;

        assert currentHighAddress % pageSize == currentPage.writtenCount % pageSize;
    }

    public long endWrite() {
        checkWriteError();

        assert blockSetMutable != null;

        assert currentHighAddress % pageSize == currentPage.writtenCount % pageSize;

        flush();

        if (doNeedsToBeSynchronized(currentHighAddress)) {
            sync();
        }

        if (blockSetWasChanged) {
            blockSet = blockSetMutable.endWrite();

            blockSetWasChanged = false;
            blockSetMutable = null;
        }

        assert currentPage.committedCount == pageSize ||
                currentPage.pageAddress == (currentHighAddress & (~(long) (pageSize - 1)));
        assert currentPage.committedCount == pageSize || writeCache.get(currentPage.pageAddress) != null;
        assert this.committedHighAddress <= currentHighAddress;

        this.committedHighAddress = currentHighAddress;
        return currentHighAddress;
    }

    public boolean needsToBeSynchronized() {
        return doNeedsToBeSynchronized(this.committedHighAddress);
    }

    private boolean doNeedsToBeSynchronized(long committedHighAddress) {
        if (lastSyncedAddress < committedHighAddress) {
            final long now = System.currentTimeMillis();
            return now - lastSyncedTs >= syncPeriod;
        }

        return false;
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

    public static byte[] generateNullPage(int pageSize) {
        final byte[] data = new byte[pageSize];
        Arrays.fill(data, 0, pageSize - HASH_CODE_SIZE, (byte) 0x80);

        final long hash = xxHash.hash(data, 0, pageSize - HASH_CODE_SIZE, XX_HASH_SEED);
        BindingUtils.writeLong(hash, data, pageSize - HASH_CODE_SIZE);

        return data;
    }

    void write(byte b) {
        checkWriteError();

        MutablePage currentPage = allocateNewPageIfNeeded();

        int delta = 1;
        int writtenCount = currentPage.writtenCount;
        assert (int) (currentHighAddress & pageSizeMask) == (writtenCount & pageSizeMask);

        assert writtenCount < adjustedPageSize;
        currentPage.bytes[writtenCount] = b;

        writtenCount++;
        currentPage.writtenCount = writtenCount;

        if (writtenCount == adjustedPageSize) {
            currentPage.writtenCount = pageSize;
            delta += HASH_CODE_SIZE;
        }

        currentHighAddress += delta;

        assert (int) (currentHighAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);

        if (currentPage.writtenCount == pageSize) {
            writePage(currentPage);
        }
    }

    void write(byte[] b, int offset, int len) {
        checkWriteError();

        int off = 0;
        int delta = len;

        assert (int) (currentHighAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);

        while (len > 0) {
            MutablePage currentPage = allocateNewPageIfNeeded();
            final int bytesToWrite = Math.min(adjustedPageSize - currentPage.writtenCount, len);

            System.arraycopy(b, offset + off, currentPage.bytes,
                    currentPage.writtenCount, bytesToWrite);

            currentPage.writtenCount += bytesToWrite;

            if (currentPage.writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize;
                delta += HASH_CODE_SIZE;

                writePage(currentPage);
            }

            len -= bytesToWrite;
            off += bytesToWrite;
        }

        this.currentHighAddress += delta;

        assert (int) (currentHighAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);
    }

    boolean fitsIntoSingleFile(long fileLengthBound, int loggableSize) {
        final long fileAddress = currentHighAddress / fileLengthBound;
        final long nextFileAddress =
                (log.adjustLoggableAddress(currentHighAddress, loggableSize) - 1) / fileLengthBound;

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
        var block = blockSet.getBlock(blockAddress);

        log.notifyBeforeBlockDeleted(block);
        try {
            writer.removeBlock(blockAddress, rbt);
            log.clearFileFromLogCache(blockAddress, 0);
        } finally {
            log.notifyAfterBlockDeleted(blockAddress);
        }
    }

    Block getBlock(long blockAddress) {
        return blockSet.getBlock(blockAddress);
    }

    void forgetFiles(final long[] files, long fileBoundary) {
        assert blockSetMutable != null;

        boolean waitForCompletion = false;
        for (final long file : files) {
            blockSetMutable.remove(file);
            blockSetWasChanged = true;

            if (!waitForCompletion) {
                final long fileEnd = file + fileBoundary;
                for (long pageAddress = file; pageAddress < fileEnd; pageAddress += pageSize) {
                    if (writeCache.containsKey(pageAddress)) {
                        waitForCompletion = true;
                        break;
                    }
                }
            }
        }

        if (waitForCompletion) {
            ensureWritesAreCompleted();
        }
    }

    long[] allFiles() {
        return blockSet.getFiles();
    }

    Long getMinimumFile() {
        return blockSet.getMinimum();
    }

    Long getMaximumFile() {
        return blockSet.getMaximum();
    }


    void flush() {
        checkWriteError();

        if (currentPage.committedCount < pageSize) {
            logCache.cachePage(log, currentPage.pageAddress, currentPage.bytes);
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
        this.currentHighAddress += written;

        assert (int) (currentHighAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);

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
            currentHighAddress += written;
        }
    }

    int padWithNulls(long fileLengthBound, byte[] nullPage) {
        checkWriteError();

        assert nullPage.length == pageSize;
        int written = doPadPageWithNulls();

        if (written > 0) {
            assert currentPage.writtenCount == pageSize;
            writePage(currentPage);
        }

        final long spaceWritten = ((currentHighAddress + written) % fileLengthBound);
        if (spaceWritten == 0) {
            currentHighAddress += written;

            assert (int) (currentHighAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);
            return written;
        }

        final long reminder = fileLengthBound - spaceWritten;
        final long pages = reminder / pageSize;

        assert reminder % pageSize == 0;

        for (int i = 0; i < pages; i++) {
            var currentPage = allocNewPage(nullPage);
            writePage(currentPage);

            written += pageSize;
        }

        currentHighAddress += written;
        assert (int) (currentHighAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);
        return written;
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

        ensureWritesAreCompleted();
        checkWriteError();

        writer.sync();

        assert lastSyncedAddress <= committedHighAddress;

        lastSyncedAddress = committedHighAddress;
        lastSyncedTs = System.currentTimeMillis();
    }

    void close(boolean sync) {
        if (sync) {
            sync();
        } else {
            ensureWritesAreCompleted();
        }

        writer.close();

        writeCache.clear();

        if (currentPage != null) {
            currentPage.xxHash64.close();
            currentPage = null;
        }

        if (blockSetMutable != null) {
            blockSetMutable.clear();
            blockSet = blockSetMutable.endWrite();
            blockSetMutable = null;
        }


        lastSyncedTs = Long.MAX_VALUE;
        lastSyncedAddress = Long.MAX_VALUE;
    }

    int getFilesSize() {
        assert blockSetMutable != null;

        return blockSetMutable.size();
    }

    void clear() {
        ensureWritesAreCompleted();

        writeCache.clear();
        writer.clear();

        currentHighAddress = 0;
        committedHighAddress = 0;

        if (currentPage != null) {
            currentPage.xxHash64.close();
        }

        blockSetMutable = null;

        currentPage = new MutablePage(new byte[pageSize], 0, 0);
        currentPage.xxHash64 = BufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(
                BufferedDataWriter.XX_HASH_SEED);
        blockSet = new BlockSet.Immutable(log.getFileLengthBound());
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
            assert entry.getKey() == currentPage.pageAddress;
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
                                adjustedPageSize);
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
            var blockAddress = block.getAddress();

            assert blockSetMutable != null;

            if (!blockSetMutable.contains(blockAddress)) {
                blockSetMutable.add(block.getAddress(), block);
                blockSetWasChanged = false;
            }


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

    long getCurrentHighAddress() {
        return currentHighAddress;
    }

    long getHighAddress() {
        return committedHighAddress;
    }

    int numberOfFiles() {
        return blockSet.size();
    }

    LongIterator getFilesFrom(final long address) {
        return blockSet.getFilesFrom(address);
    }

    void closeFileIfNecessary(long fileLengthBound, boolean makeFileReadOnly) {
        if (writer.position() == fileLengthBound) {
            sync();

            assert lastSyncedAddress <= committedHighAddress;

            lastSyncedAddress = committedHighAddress;
            lastSyncedTs = System.currentTimeMillis();

            writer.close();

            assert blockSetMutable != null;

            var blockSet = blockSetMutable;
            var lastFile = blockSet.getMaximum();
            if (lastFile != null) {
                var block = blockSet.getBlock(lastFile);

                var refreshed = block.refresh();
                if (block != refreshed) {
                    blockSet.add(lastFile, refreshed);
                }

                var length = refreshed.length();
                if (length < fileLengthBound) {
                    throw new IllegalStateException(
                            "File's too short (" + LogUtil.getLogFilename(lastFile)
                                    + "), block.length() = " + length + ", fileLengthBound = " + fileLengthBound
                    );
                }

                if (makeFileReadOnly && block instanceof File) {
                    //noinspection ResultOfMethodCallIgnored
                    ((File) block).setReadOnly();
                }
            }
        }
    }

    void openNewFileIfNeeded(long fileLengthBound, Log log) {
        assert blockSetMutable != null;

        if (!this.writer.isOpen()) {
            var fileAddress = currentHighAddress - currentHighAddress % fileLengthBound;
            var block = writer.openOrCreateBlock(fileAddress, currentHighAddress % fileLengthBound);

            boolean fileCreated = !blockSetMutable.contains(fileAddress);
            if (fileCreated) {
                blockSetMutable.add(fileAddress, block);
                blockSetWasChanged = true;

                // fsync the directory to ensure we will find the log file in the directory after system crash
                writer.syncDirectory();
                log.notifyBlockCreated(block);
            } else {
                log.notifyBlockModified(block);
            }
        }
    }

    BlockSet.Mutable mutableBlocksUnsafe() {
        return blockSet.beginWrite();
    }

    void updateBlockSetHighAddressUnsafe(final long prevHighAddress, final long highAddress, final BlockSet.Immutable blockSet) {
        ensureWritesAreCompleted();

        if (currentHighAddress != committedHighAddress || blockSetMutable != null ||
                currentPage.committedCount < currentPage.writtenCount) {
            throw new ExodusException("Can not update high address and block set once they are changing");
        }

        if (currentHighAddress != prevHighAddress) {
            throw new ExodusException("High address was changed and can not be updated");
        }

        this.currentHighAddress = highAddress;
        this.committedHighAddress = highAddress;

        var pageOffset = (int) highAddress & (pageSize - 1);
        var pageAddress = highAddress - pageOffset;

        var page = logCache.getPage(log, pageAddress, -1);

        initCurrentPage(blockSet, highAddress, page);
    }


    private int doPadWholePageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;

        if (writtenInPage > 0) {
            final int written = pageSize - writtenInPage;

            Arrays.fill(currentPage.bytes, writtenInPage, pageSize, (byte) 0x80);

            currentPage.writtenCount = pageSize;
            currentHighAddress += written;

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

                written = pageDelta + BufferedDataWriter.HASH_CODE_SIZE;
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
