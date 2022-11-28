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
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataWriter;
import net.jpountz.xxhash.StreamingXXHash64;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public final class SyncBufferedDataWriter extends BufferedDataWriter {
    private int count;

    SyncBufferedDataWriter(@NotNull final Log log,
                           @NotNull final DataWriter writer,
                           @NotNull final LogTip page,
                           final boolean calculateHashCode) {
        super(log, writer, page, calculateHashCode);
    }


    void write(byte b, long firstLoggable) {
        int count = this.count;
        MutablePage currentPage = this.currentPage;

        int writtenCount = currentPage.writtenCount;
        assert (int) (highAddress & pageSizeMask) == (writtenCount & pageSizeMask);

        if (writtenCount < adjustedPageSize) {
            currentPage.bytes[writtenCount] = b;

            writtenCount++;
            currentPage.writtenCount = writtenCount;

            count++;
            if (writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize;
                count += LOGGABLE_DATA;
            }
        } else {
            currentPage = allocNewPage();

            currentPage.bytes[0] = b;
            currentPage.writtenCount = 1;

            count++;
        }

        final int delta = count - this.count;
        highAddress += delta;

        this.count = count;
        writeFirstLoggableOffset(firstLoggable, currentPage);

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);
    }

    void write(byte[] b, int offset, int len) throws ExodusException {
        int off = 0;
        int count = this.count + len;

        MutablePage currentPage = this.currentPage;
        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);

        while (len > 0) {
            int bytesToWrite = adjustedPageSize - currentPage.writtenCount;

            if (bytesToWrite <= 0) {
                assert currentPage.writtenCount == pageSize;

                currentPage = allocNewPage();
                bytesToWrite = adjustedPageSize;
            }

            if (bytesToWrite > len) {
                bytesToWrite = len;
            }

            System.arraycopy(b, offset + off, currentPage.bytes,
                    currentPage.writtenCount, bytesToWrite);

            currentPage.writtenCount += bytesToWrite;

            if (currentPage.writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize;
                count += LOGGABLE_DATA;
            }

            len -= bytesToWrite;
            off += bytesToWrite;
        }

        final int delta = count - this.count;

        this.highAddress += delta;
        this.count = count;

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);
    }

    void commit() {
        count = 0;
        final MutablePage currentPage = this.currentPage;
        currentPage.committedCount = currentPage.writtenCount;
        MutablePage previousPage = currentPage.previousPage;

        if (previousPage != null) {
            final ArrayList<MutablePage> fullPages = new ArrayList<>();
            do {
                fullPages.add(0, previousPage);
                previousPage = previousPage.previousPage;
            } while (previousPage != null);

            for (final MutablePage mutablePage : fullPages) {
                final byte[] bytes = mutablePage.bytes;
                final int off = mutablePage.flushedCount;

                final StreamingXXHash64 xxHash64 = mutablePage.xxHash64;
                final long pageAddress = mutablePage.pageAddress;


                final int len = pageSize - off;
                if (len > 0) {
                    int contentLen = adjustedPageSize - off + FIRST_ITERABLE_OFFSET_SIZE;

                    if (cipherProvider == null) {
                        if (this.calculateHashCode) {
                            xxHash64.update(bytes, off, contentLen);
                            BindingUtils.writeLong(xxHash64.getValue(), bytes,
                                    adjustedPageSize + FIRST_ITERABLE_OFFSET_SIZE);
                        }

                        writePage(bytes, off, len);
                    } else {
                        final byte[] encryptedBytes = EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey,
                                cipherBasicIV, pageAddress, bytes, off, len, LogUtil.LOG_BLOCK_ALIGNMENT);

                        if (this.calculateHashCode) {
                            xxHash64.update(encryptedBytes, 0, contentLen);
                            BindingUtils.writeLong(xxHash64.getValue(), encryptedBytes, contentLen);
                        }

                        writePage(encryptedBytes, 0, len);
                    }
                }

                cachePage(bytes, pageAddress);
                xxHash64.close();
            }

            currentPage.previousPage = null;
        }

        //noinspection ConstantConditions
        assert blockSetMutable.getBlock(blockSetMutable.getMaximum()).length() % log.getFileLengthBound() ==
                (highAddress - (currentPage.writtenCount - currentPage.flushedCount)) % log.getFileLengthBound();
    }

    void flush() {
        if (count > 0) {
            commit();
        }

        final MutablePage currentPage = this.currentPage;
        final int committedCount = currentPage.committedCount;
        final int flushedCount = currentPage.flushedCount;

        if (committedCount > flushedCount) {
            final byte[] bytes = currentPage.bytes;
            final int len = committedCount - flushedCount;

            final StreamingXXHash64 xxHash64 = currentPage.xxHash64;

            final long pageAddress = currentPage.pageAddress;

            final int contentLen;
            if (committedCount < pageSize) {
                contentLen = len;
            } else {
                contentLen = len - HASH_CODE_SIZE;
            }

            if (cipherProvider == null) {
                if (this.calculateHashCode) {
                    xxHash64.update(bytes, flushedCount, contentLen);

                    if (committedCount == pageSize) {

                        BindingUtils.writeLong(xxHash64.getValue(), bytes,
                                adjustedPageSize + FIRST_ITERABLE_OFFSET_SIZE);
                    }
                }

                writePage(bytes, flushedCount, len);
            } else {
                final byte[] encryptedBytes = EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                        pageAddress, bytes, flushedCount, len, LogUtil.LOG_BLOCK_ALIGNMENT);

                if (this.calculateHashCode) {
                    xxHash64.update(encryptedBytes, 0, contentLen);

                    if (committedCount == pageSize) {
                        BindingUtils.writeLong(xxHash64.getValue(), encryptedBytes,
                                contentLen);
                    }
                }

                writePage(encryptedBytes, 0, len);
            }

            if (committedCount == pageSize) {
                cachePage(bytes, pageAddress);
            }

            currentPage.flushedCount = committedCount;
        }

        //noinspection ConstantConditions
        assert blockSetMutable.getBlock(blockSetMutable.getMaximum()).length() % log.getFileLengthBound() ==
                highAddress % log.getFileLengthBound();
    }


    boolean padWithNulls(long fileLengthBound, byte[] nullPage) {
        assert nullPage.length == pageSize;
        int written = doPadPageWithNulls();
        count += written;

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
            allocNewPage(nullPage);
            written += pageSize;
        }

        highAddress += written;
        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);
        return written > 0;
    }

    public int padPageWithNulls() {
        final int written = doPadPageWithNulls();
        count += written;

        this.highAddress += written;

        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);

        return written;
    }

    public void padWholePageWithNulls() {
        var written = doPadWholePageWithNulls();
        count += written;
    }


    private void writePage(final byte @NotNull [] bytes, final int off, final int len) {
        final Block block = writer.write(bytes, off, len);
        blockSetMutable.add(block.getAddress(), block);
    }

    private MutablePage allocNewPage() {
        MutablePage currentPage = this.currentPage;

        currentPage = this.currentPage = new MutablePage(currentPage, logCache.allocPage(),
                currentPage.pageAddress + pageSize, 0);
        currentPage.xxHash64 = XX_HASH_FACTORY.newStreamingHash64(XX_HASH_SEED);

        return currentPage;
    }

    private void allocNewPage(byte[] pageData) {
        assert pageData.length == pageSize;
        MutablePage currentPage = this.currentPage;

        this.currentPage = new MutablePage(currentPage, pageData,
                currentPage.pageAddress + pageSize, pageData.length);
    }

}
