/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.TransactionalDataWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

class BufferedDataWriter implements TransactionalDataWriter {

    @NotNull
    private final Log log;
    @NotNull
    private final LogCache logCache;
    @NotNull
    private final DataWriter child;
    @NotNull
    private MutablePage currentPage;
    private final int pageSize;
    private int count;
    private int capacity;

    private BufferedDataWriter(@NotNull final Log log,
                               @NotNull final DataWriter child,
                               @NotNull final ArrayByteIterable page,
                               final long pageAddress) {
        this.log = log;
        logCache = log.cache;
        this.child = child;
        currentPage = new MutablePage(null, page, pageAddress);
        pageSize = log.getCachePageSize();
        if (pageSize != page.getBytesUnsafe().length) {
            throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                    pageSize + ", actual page size = " + page.getBytesUnsafe().length);
        }
    }

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter child) {
        this(log, child, log.cache.allocPage(), 0L);
    }

    BufferedDataWriter(@NotNull final Log log,
                       @NotNull final DataWriter child,
                       final long highPageAddress,
                       final byte[] highPageContent,
                       final int highPageSize) {
        this(log, child, new ArrayByteIterable(highPageContent), highPageAddress);
        currentPage.setPageSize(highPageSize);
    }


    @Override
    public DataWriter getChildWriter() {
        return child;
    }

    @Override
    public boolean isOpen() {
        return child.isOpen();
    }

    @Override
    public boolean write(byte b) {
        final int count = this.count;
        MutablePage currentPage = this.currentPage;
        final int writtenCount = currentPage.writtenCount;
        if (writtenCount < pageSize) {
            currentPage.bytes[writtenCount] = b;
            currentPage.writtenCount = writtenCount + 1;
            this.count = count + 1;
            return true;
        }
        if (count >= capacity) {
            return false;
        }
        currentPage = allocNewPage();
        currentPage.bytes[0] = b;
        currentPage.writtenCount = 1;
        this.count = count + 1;
        return true;
    }

    @Override
    public boolean write(byte[] b, int off, int len) throws ExodusException {
        final int count = this.count + len;
        MutablePage currentPage = this.currentPage;
        while (len > 0) {
            int bytesToWrite = pageSize - currentPage.writtenCount;
            if (bytesToWrite == 0) {
                if (count > capacity) {
                    return false;
                }
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
        return true;
    }

    @Override
    public void setMaxBytesToWrite(final int capacity) {
        this.capacity = capacity;
    }

    @Override
    public void commit() {
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
                final int off = mutablePage.flushedCount;
                child.write(mutablePage.bytes, off, pageSize - off);
                logCache.cachePage(log, mutablePage.pageAddress, mutablePage.page);
            }
            currentPage.previousPage = null;
        }
    }

    @Override
    public void rollback() {
        count = 0;
        MutablePage currentPage = this.currentPage;
        MutablePage previousPage = currentPage.previousPage;
        while (previousPage != null) {
            currentPage = previousPage;
            previousPage = previousPage.previousPage;
        }
        currentPage.writtenCount = currentPage.committedCount;
    }

    @Override
    public void flush() {
        if (count > 0) {
            throw new IllegalStateException("Can't flush uncommitted writer: " + count);
        }
        final MutablePage currentPage = this.currentPage;
        final int committedCount = currentPage.committedCount;
        final int flushedCount = currentPage.flushedCount;
        if (committedCount > flushedCount) {
            child.write(currentPage.bytes, flushedCount, committedCount - flushedCount);
            currentPage.flushedCount = committedCount;
        }
    }

    @Override
    public void sync() {
        child.sync();
    }

    @Override
    public void close() {
        if (count > 0) {
            throw new IllegalStateException("Can't close uncommitted writer " + count);
        }
        child.close();
    }

    @Override
    public void openOrCreateBlock(long address, long length) {
        child.openOrCreateBlock(address, length);
    }

    @Override
    public boolean lock(long timeout) {
        return child.lock(timeout);
    }

    @Override
    public boolean release() {
        return child.release();
    }

    @Override
    public String lockInfo() {
        return child.lockInfo();
    }

    @Override
    public ArrayByteIterable getHighPage(final long alignedAddress) {
        MutablePage currentPage = this.currentPage;
        do {
            final long highPageAddress = currentPage.pageAddress;
            if (alignedAddress == highPageAddress) {
                final int committedCount = currentPage.committedCount;
                return committedCount > 0 ? new ArrayByteIterable(currentPage.bytes, committedCount) : null;
            }
            currentPage = currentPage.previousPage;
        } while (currentPage != null);
        return null;
    }

    @Override
    public boolean tryAndUpdateHighAddress(long highAddress) {
        if (count > 0) {
            throw new IllegalStateException("Can't update high address for uncommitted writer: " + count);
        }
        final MutablePage currentPage = this.currentPage;
        final int committed = (int) (highAddress - currentPage.pageAddress);
        if (committed < 0 || committed > pageSize) {
            return false;
        }
        currentPage.setPageSize(committed);
        return true;
    }

    private MutablePage allocNewPage() {
        MutablePage currentPage = this.currentPage;
        return this.currentPage = new MutablePage(currentPage, logCache.allocPage(), currentPage.pageAddress + pageSize);
    }

    private static class MutablePage {

        @Nullable
        MutablePage previousPage;
        @NotNull
        final ArrayByteIterable page;
        @NotNull
        final byte[] bytes;
        final long pageAddress;
        int flushedCount;
        int committedCount;
        int writtenCount;

        MutablePage(@Nullable final MutablePage previousPage,
                    @NotNull final ArrayByteIterable page,
                    final long pageAddress) {
            this.previousPage = previousPage;
            this.page = page;
            bytes = page.getBytesUnsafe();
            this.pageAddress = pageAddress;
            setPageSize(0);
        }

        void setPageSize(final int pageSize) {
            flushedCount = committedCount = writtenCount = pageSize;
        }
    }
}
