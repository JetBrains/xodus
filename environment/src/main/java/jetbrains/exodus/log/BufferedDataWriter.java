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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

public class BufferedDataWriter {

    // immutable state
    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    @NotNull
    private final DataWriter child;
    @NotNull
    private final DataReader reader;
    @NotNull
    private final LogTip initialPage;
    @Nullable
    private final StreamCipherProvider cipherProvider;
    private final byte[] cipherKey;
    private final long cipherBasicIV;
    private final int pageSize;
    @NotNull
    private final LogFileSet.Mutable fileSetMutable;

    // mutable state
    @NotNull
    private MutablePage currentPage;
    private long highAddress;
    private int count;

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter child,
                       @NotNull final DataReader reader,
                       @NotNull final LogTip page) {
        this.log = log;
        logCache = log.cache;
        this.fileSetMutable = page.logFileSet.beginWrite();
        this.initialPage = page;
        this.child = child;
        this.reader = reader;
        this.highAddress = page.highAddress;
        final boolean validInitialPage = page.count >= 0;
        pageSize = log.getCachePageSize();
        if (validInitialPage) {
            if (pageSize != page.bytes.length) {
                throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + page.bytes.length);
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
    public LogFileSet.Mutable getFileSetMutable() {
        return fileSetMutable;
    }

    public void setHighAddress(long highAddress) {
        allocLastPage(log.getHighPageAddress(highAddress));
        this.highAddress = highAddress;
    }

    public MutablePage allocLastPage(long pageAddress) {
        MutablePage result = currentPage;
        if (pageAddress == result.pageAddress) {
            return result;
        }

        result = new MutablePage(null, logCache.allocPage(), pageAddress, 0);
        currentPage = result;
        return result;
    }

    void write(byte b) {
        final int count = this.count;
        MutablePage currentPage = this.currentPage;
        final int writtenCount = currentPage.writtenCount;
        if (writtenCount < pageSize) {
            currentPage.bytes[writtenCount] = b;
            currentPage.writtenCount = writtenCount + 1;
        } else {
            currentPage = allocNewPage();
            currentPage.bytes[0] = b;
            currentPage.writtenCount = 1;
        }
        this.count = count + 1;
    }

    void write(byte[] b, int len) throws ExodusException {
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
            System.arraycopy(b, off, currentPage.bytes, currentPage.writtenCount, bytesToWrite);
            currentPage.writtenCount += bytesToWrite;
            len -= bytesToWrite;
            off += bytesToWrite;
        }
        this.count = count;
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
                if (cipherProvider == null) {
                    child.write(bytes, off, len);
                } else {
                    child.write(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                            mutablePage.pageAddress, bytes, off, len, LogUtil.LOG_BLOCK_ALIGNMENT), 0, len);
                }
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
            if (cipherProvider == null) {
                child.write(bytes, flushedCount, len);
            } else {
                child.write(EnvKryptKt.cryptBlocksImmutable(cipherProvider, cipherKey, cipherBasicIV,
                        currentPage.pageAddress, bytes, flushedCount, len, LogUtil.LOG_BLOCK_ALIGNMENT), 0, len);
            }
            currentPage.flushedCount = committedCount;
        }
    }

    void openOrCreateBlock(long address, long length) {
        child.openOrCreateBlock(address, length);
    }

    long getHighAddress() {
        return highAddress;
    }

    public void incHighAddress(long delta) {
        this.highAddress += delta;
    }

    public void setLastPageLength(int lastPageLength) {
        currentPage.setCounts(lastPageLength);
    }

    public int getLastPageLength() {
        return currentPage.writtenCount;
    }

    long getLastWrittenFileLength(long fileLengthBound) {
        return getHighAddress() % fileLengthBound;
    }

    @NotNull
    LogTip getStartingTip() {
        return initialPage;
    }

    @NotNull
    LogTip getUpdatedTip() {
        final MutablePage currentPage = this.currentPage;
        final LogFileSet.Immutable fileSetImmutable = fileSetMutable.endWrite();
        return new LogTip(currentPage.bytes, currentPage.pageAddress, currentPage.committedCount, highAddress, highAddress, fileSetImmutable);
    }

    byte getByte(long address, byte max) {
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
        if (!fileSetMutable.contains(fileAddress)) {
            BlockNotFoundException.raise("Address is out of log space, underflow", log, address);
        }

        final byte[] output = new byte[pageSize];

        final Block block = reader.getBlock(fileAddress);
        final int readBytes = block.read(output, pageAddress - fileAddress, output.length);

        if (readBytes < offset) {
            throw new ExodusException("Can't read expected page bytes");
        }

        if (cipherProvider != null) {
            EnvKryptKt.cryptBlocksMutable(cipherProvider, cipherKey, cipherBasicIV, pageAddress, output, 0, readBytes, LogUtil.LOG_BLOCK_ALIGNMENT);
        }

        final byte result = (byte) (output[offset] ^ 0x80);
        if (result < 0 || result > max) {
            throw new IllegalStateException("Unknown written file loggable type: " + result + ", address: " + address);
        }
        return result;
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

    public static class MutablePage {

        @Nullable
        MutablePage previousPage;
        @NotNull
        final byte[] bytes;
        final long pageAddress;
        int flushedCount;
        int committedCount;
        int writtenCount;

        MutablePage(@Nullable final MutablePage previousPage, @NotNull final byte[] page, final long pageAddress, final int count) {
            this.previousPage = previousPage;
            this.bytes = page;
            this.pageAddress = pageAddress;
            flushedCount = committedCount = writtenCount = count;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public int getCount() {
            return writtenCount;
        }

        void setCounts(final int count) {
            flushedCount = committedCount = writtenCount = count;
        }
    }
}
