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

import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

public final class DataIterator extends ByteIterator {

    @NotNull
    private final Log log;
    private final int cachePageSize;
    private final long pageAddressMask;
    private long pageAddress;
    private byte[] page;
    private int offset;
    private int length;

    public DataIterator(@NotNull final Log log) {
        this(log, -1L);
    }

    public DataIterator(@NotNull final Log log, final long startAddress) {
        this.log = log;
        cachePageSize = log.getCachePageSize();
        pageAddressMask = ~((long) (cachePageSize - 1));
        pageAddress = -1L;
        if (startAddress >= 0) {
            checkPageSafe(startAddress);
        }
    }

    @Override
    public boolean hasNext() {
        if (page == null) {
            return false;
        }
        if (offset >= length) {
            checkPageSafe(getHighAddress());
            return hasNext();
        }
        return true;
    }

    @Override
    public byte next() {
        if (!hasNext()) {
            DataCorruptionException.raise(
                "DataIterator: no more bytes available", log, getHighAddress());
        }
        return page[offset++];
    }

    @Override
    public long skip(final long bytes) {
        long skipped = 0;
        while (page != null && skipped < bytes) {
            final long pageBytesToSkip = Math.min(bytes - skipped, length - offset);
            skipped += pageBytesToSkip;
            offset += pageBytesToSkip;
            if (offset < length) {
                break;
            }
            checkPageSafe(getHighAddress());
        }
        return skipped;
    }

    @Override
    public long nextLong(final int length) {
        if (page == null || this.length - offset < length) {
            return LongBinding.entryToUnsignedLong(this, length);
        }
        final long result = LongBinding.entryToUnsignedLong(page, offset, length);
        offset += length;
        return result;
    }

    public void checkPage(final long address) {
        final long pageAddress = address & pageAddressMask;
        if (this.pageAddress != pageAddress) {
            page = log.cache.getPage(log, pageAddress);
            this.pageAddress = pageAddress;
        }
        length = cachePageSize;
        offset = (int) (address - pageAddress);
    }

    private void checkPageSafe(final long address) {
        try {
            checkPage(address);
            final long pageAddress = address & pageAddressMask;
            length = (int) Math.min(log.getHighAddress() - pageAddress, cachePageSize);
            if (length > offset) {
                return;
            }
        } catch (BlockNotFoundException ignore) {
        }
        pageAddress = -1L;
        page = null;
    }

    byte[] getCurrentPage() {
        return page;
    }

    int getOffset() {
        return offset;
    }

    int getLength() {
        return length;
    }

    long getHighAddress() {
        return pageAddress + offset;
    }

    boolean availableInCurrentPage(final int bytes) {
        return length - offset >= bytes;
    }
}
