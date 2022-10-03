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
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataWriter;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;

public class BufferedDataWriter {
    public static final long HASH_SEED = 0xADEF1279AL;
    public static final XXHash64 xxHash = XXHashFactory.fastestInstance().hash64();

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
    private final int adjustedPageSize;
    @NotNull
    private final BlockSet.Mutable blockSetMutable;

    // mutable state
    @NotNull
    private MutablePage currentPage;
    private long highAddress;
    private int count;

    private final int pageSizeMask;

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter child,
                       @NotNull final LogTip page) {
        this.log = log;
        logCache = log.cache;
        this.blockSetMutable = page.blockSet.beginWrite();
        this.initialPage = page;
        this.child = child;
        this.highAddress = page.highAddress;
        final boolean validInitialPage = page.count >= 0;
        pageSize = log.getCachePageSize();

        if (validInitialPage) {
            if (pageSize != page.bytes.length) {
                throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + page.bytes.length);
            }
            currentPage = new MutablePage(null, page.bytes, page.pageAddress, page.count);
            currentPage.firstLoggable = BindingUtils.readInt(page.bytes, pageSize - Log.LOGGABLE_DATA);
        } else {
            byte[] newPage = logCache.allocPage();
            BindingUtils.writeInt(-1, newPage, pageSize - Log.LOGGABLE_DATA);
            currentPage = new MutablePage(null, newPage, page.pageAddress, 0);
        }

        cipherProvider = log.getConfig().getCipherProvider();
        cipherKey = log.getConfig().getCipherKey();
        cipherBasicIV = log.getConfig().getCipherBasicIV();

        pageSizeMask = (pageSize - 1);
        adjustedPageSize = pageSize - Log.LOGGABLE_DATA;
    }

    public static void checkPageConsistency(long pageAddress, long checkHashCodeSince,
                                            byte @NotNull [] bytes, int pageSize, Log log) {
        if (pageAddress >= checkHashCodeSince && bytes.length == pageSize) {
            XXHash64 xxHash = BufferedDataWriter.xxHash;

            long calculatedHash = xxHash.hash(bytes, 0,
                    bytes.length - Log.HASH_CODE_SIZE, BufferedDataWriter.HASH_SEED);
            long storedHash = BindingUtils.readLong(bytes, bytes.length - Log.HASH_CODE_SIZE);

            if (storedHash != calculatedHash) {
                DataCorruptionException.raise("Page is broken", log, pageAddress);
            }
        }
    }

    @NotNull
    public BlockSet.Mutable getBlockSetMutable() {
        return blockSetMutable;
    }

    public void setHighAddress(long highAddress) {
        allocLastPage(highAddress - (((int) highAddress) & (log.getCachePageSize() - 1))); // don't alloc full page
        this.highAddress = highAddress;
    }

    public void allocLastPage(long pageAddress) {
        MutablePage result = currentPage;

        if (pageAddress == result.pageAddress) {
            return;
        }

        result = new MutablePage(null, logCache.allocPage(), pageAddress, 0);
        currentPage = result;
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
                count += Log.LOGGABLE_DATA;
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
        if (firstLoggable >= 0 && currentPage.firstLoggable < 0) {
            int loggableOffset = (int) (firstLoggable & pageSizeMask);

            currentPage.firstLoggable = loggableOffset;
            BindingUtils.writeInt(loggableOffset, currentPage.bytes, pageSize - Log.FIRST_ITERABLE_OFFSET);
        }

        assert (int) (highAddress & pageSizeMask) == (currentPage.writtenCount & pageSizeMask);
    }

    void write(byte[] b, int len) throws ExodusException {
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

            System.arraycopy(b, off, currentPage.bytes, currentPage.writtenCount, bytesToWrite);

            currentPage.writtenCount += bytesToWrite;

            if (currentPage.writtenCount == adjustedPageSize) {
                currentPage.writtenCount = pageSize;
                count += Log.LOGGABLE_DATA;
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
                final int len = pageSize - off;
                final long pageAddress = mutablePage.pageAddress;

                generateHashCode(bytes, adjustedPageSize);

                if (cipherProvider == null) {
                    writePage(bytes, off, len);
                } else {
                    writePage(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                            pageAddress, bytes, off, len, LogUtil.LOG_BLOCK_ALIGNMENT), 0, len);
                }
                cachePage(bytes, pageAddress);
            }
            currentPage.previousPage = null;
        }
    }

    void flush() {
        if (count > 0) {
            throw new IllegalStateException("Can't flush uncommitted writer: " + count);
        }

        final MutablePage currentPage = this.currentPage;
        final int committedCount = currentPage.committedCount;
        final int flushedCount = currentPage.flushedCount;
        if (committedCount > flushedCount) {
            final byte[] bytes = currentPage.bytes;
            final int len = committedCount - flushedCount;
            final long pageAddress = currentPage.pageAddress;

            if (committedCount == pageSize) {
                generateHashCode(bytes, adjustedPageSize);
            }

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

    private static void generateHashCode(byte[] bytes, int adjustedPageSize) {
        final long hash =
                xxHash.hash(bytes, 0, adjustedPageSize + Log.FIRST_ITERABLE_OFFSET_SIZE, HASH_SEED);
        BindingUtils.writeLong(hash, bytes, adjustedPageSize + Log.FIRST_ITERABLE_OFFSET_SIZE);
    }

    Block openOrCreateBlock(long address, long length) {
        return child.openOrCreateBlock(address, length);
    }

    long getHighAddress() {
        return highAddress;
    }

    boolean fitsIntoSingleFile(long fileLengthBound, int loggableSize) {
        final long fileAddress = highAddress / fileLengthBound;
        final long nextFileAddress = (log.adjustedLoggableAddress(highAddress, loggableSize) - 1) / fileLengthBound;

        return fileAddress == nextFileAddress;
    }

    boolean isFileFull(long fileLengthBound) {
        return highAddress % fileLengthBound == 0;
    }

    boolean padWithNulls(long fileLengthBound, byte[] nullPage) {
        assert nullPage.length == pageSize;
        int written = doPadPageWithNulls();

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
        this.highAddress += written;

        assert (int) (highAddress & pageSizeMask) == (this.currentPage.writtenCount & pageSizeMask);

        return written;
    }

    private int doPadPageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;
        if (writtenInPage > 0) {
            final int pageDelta = adjustedPageSize - writtenInPage;

            int written = 0;
            if (pageDelta > 0) {
                Arrays.fill(currentPage.bytes, writtenInPage, adjustedPageSize, (byte) 0x80);
                currentPage.writtenCount = pageSize;

                count += pageDelta + Log.LOGGABLE_DATA;
                written = pageDelta + Log.LOGGABLE_DATA;
            }

            return written;
        } else {
            return 0;
        }
    }

    @NotNull
    LogTip getStartingTip() {
        return initialPage;
    }

    @NotNull
    LogTip getUpdatedTip() {
        final MutablePage currentPage = this.currentPage;
        final BlockSet.Immutable blockSetImmutable = blockSetMutable.endWrite();
        return new LogTip(currentPage.bytes, currentPage.pageAddress,
                currentPage.committedCount, highAddress, highAddress, blockSetImmutable);
    }

    byte getByte(long address, byte max, long checkHashCodeSince) {
        final int offset = ((int) address) & (pageSize - 1);
        final long pageAddress = address - offset;
        final MutablePage page = getWrittenPage(pageAddress);
        if (page != null) {
            final byte result = (byte) (page.bytes[offset] ^ 0x80);
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

        final byte[] output = new byte[pageSize];

        final Block block = blockSetMutable.getBlock(fileAddress);

        final int readBytes = block.read(output, pageAddress - fileAddress, 0, output.length);

        if (readBytes < offset) {
            throw new ExodusException("Can't read expected page bytes");
        }

        if (cipherProvider != null) {
            EnvKryptKt.cryptBlocksMutable(cipherProvider, cipherKey, cipherBasicIV, pageAddress, output, 0, readBytes, LogUtil.LOG_BLOCK_ALIGNMENT);
        }

        checkPageConsistency(pageAddress, checkHashCodeSince, output, pageSize, log);

        final byte result = (byte) (output[offset] ^ 0x80);
        if (result < 0 || result > max) {
            throw new IllegalStateException("Unknown written file loggable type: " + result + ", address: " + address);
        }

        return result;
    }

    private void writePage(final byte @NotNull [] bytes, final int off, final int len) {
        final Block block = child.write(bytes, off, len);
        blockSetMutable.add(block.getAddress(), block);
    }

    private void cachePage(final byte @NotNull [] bytes, final long pageAddress) {
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
        return this.currentPage = new MutablePage(currentPage, logCache.allocPage(),
                currentPage.pageAddress + pageSize, 0);
    }

    private void allocNewPage(byte[] pageData) {
        assert pageData.length == pageSize;
        MutablePage currentPage = this.currentPage;

        this.currentPage = new MutablePage(currentPage, pageData,
                currentPage.pageAddress + pageSize, pageData.length);
    }

    public static byte[] generateNullPage(int pageSize) {
        final byte[] data = new byte[pageSize];
        Arrays.fill(data, 0, pageSize - Log.LOGGABLE_DATA, (byte) 0x80);
        generateHashCode(data, pageSize - Log.LOGGABLE_DATA);

        return data;
    }

    public static class MutablePage {

        @Nullable
        MutablePage previousPage;
        byte @NotNull [] bytes;
        final long pageAddress;
        int flushedCount;
        int committedCount;
        int writtenCount;
        int firstLoggable;

        MutablePage(@Nullable final MutablePage previousPage, final byte @NotNull [] page,
                    final long pageAddress, final int count) {
            this.previousPage = previousPage;
            this.bytes = page;
            this.pageAddress = pageAddress;
            flushedCount = committedCount = writtenCount = count;
            this.firstLoggable = -1;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public int getCount() {
            return writtenCount;
        }
    }
}
