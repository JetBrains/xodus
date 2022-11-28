package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.io.AsyncDataWriter;
import jetbrains.exodus.io.Block;
import net.jpountz.xxhash.StreamingXXHash64;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

final class AsyncBufferedDataWriter extends BufferedDataWriter {

    private final AsyncDataWriter writer;
    private final Semaphore writeBoundarySemaphore;
    private final Semaphore localWritesSemaphore;

    private final BiConsumer<Void, ? super Throwable> writeCompletionHandler;

    private volatile Throwable writeError;

    AsyncBufferedDataWriter(@NotNull final Log log,
                            @NotNull final AsyncDataWriter writer,
                            @NotNull final LogTip page,
                            final boolean calculateHashCode,
                            final Semaphore writeBoundarySemaphore) {
        super(log, writer, page, calculateHashCode);

        this.writer = writer;

        this.writeBoundarySemaphore = writeBoundarySemaphore;
        this.localWritesSemaphore = new Semaphore(Integer.MAX_VALUE);

        this.writeCompletionHandler = (Void, err) -> {
            writeBoundarySemaphore.release();
            localWritesSemaphore.release();

            if (err != null) {
                writeError = err;
            }
        };
    }

    @Override
    void write(byte b, long firstLoggable) {
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


    @Override
    void write(byte[] b, int offset, int len) {
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

    @Override
    void commit() {
        checkWriteError();
    }

    private void checkWriteError() {
        if (writeError != null) {
            throw ExodusException.toExodusException(writeError);
        }
    }

    @Override
    public int padPageWithNulls() {
        final int written = doPadPageWithNulls();
        this.highAddress += written;

        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);

        if (written > 0) {
            assert currentPage.writtenCount == pageSize;
            writePage(currentPage);
        }

        return written;
    }

    @Override
    public void padWholePageWithNulls() {
        final int written = doPadWholePageWithNulls();

        if (written > 0) {
            writePage(currentPage);
            highAddress += written;
        }
    }

    @Override
    boolean padWithNulls(long fileLengthBound, byte[] nullPage) {
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

    @Override
    void flush() {
        checkWriteError();
        var currentPage = this.currentPage;

        if (currentPage.writtenCount > currentPage.committedCount) {
            writePage(currentPage);
        }

        //wait till all local writes will be flushed to the disk
        localWritesSemaphore.acquireUninterruptibly(Integer.MAX_VALUE);
        localWritesSemaphore.release(Integer.MAX_VALUE);
        checkWriteError();

        assert highAddress % log.getFileLengthBound() == writer.position() % log.getFileLengthBound() &&
                writer.position() <= log.getFileLengthBound();
        assert blockSetMutable.getMaximum() == null ||
                blockSetMutable.getBlock(blockSetMutable.getMaximum()).length() % log.getFileLengthBound() ==
                        highAddress % log.getFileLengthBound();
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

            cachePage(bytes, pageAddress);
            xxHash64.close();

            writeBoundarySemaphore.acquireUninterruptibly();
            localWritesSemaphore.acquireUninterruptibly();

            assert writer.position() == currentPage.pageAddress % log.getFileLengthBound()
                    + currentPage.committedCount;
            Pair<Block, CompletableFuture<Void>> result;
            if (cipherProvider != null) {
                result = writer.asyncWrite(encryptedBytes, 0, len);
            } else {
                result = writer.asyncWrite(bytes, committedCount, len);
            }
            assert writer.position() == currentPage.pageAddress % log.getFileLengthBound()
                    + currentPage.writtenCount;

            var block = result.getFirst();
            blockSetMutable.add(block.getAddress(), block);

            result.getSecond().whenComplete(writeCompletionHandler);
            page.committedCount = page.writtenCount;
        }
    }

    private MutablePage allocNewPage() {
        MutablePage currentPage = this.currentPage;

        currentPage = this.currentPage = new MutablePage(null, logCache.allocPage(),
                currentPage.pageAddress + pageSize, 0);
        currentPage.xxHash64 = XX_HASH_FACTORY.newStreamingHash64(XX_HASH_SEED);

        return currentPage;
    }

    private MutablePage allocNewPage(byte[] pageData) {
        assert pageData.length == pageSize;
        MutablePage currentPage = this.currentPage;

        return this.currentPage = new MutablePage(null, pageData,
                currentPage.pageAddress + pageSize, pageData.length);
    }
}
