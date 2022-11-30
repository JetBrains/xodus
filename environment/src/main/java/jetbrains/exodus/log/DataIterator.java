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

import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

public final class DataIterator implements ByteIteratorWithAddress {

    @NotNull
    private final Log log;
    private final int cachePageSize;
    private final long pageAddressMask;
    private long pageAddress;
    private byte[] page;
    private int pageOffset;
    private int chunkLength;
    private long length;
    private final boolean formatWithHashCodeIsUsed;

    public DataIterator(@NotNull final Log log) {
        this(log, -1L);
    }

    public DataIterator(@NotNull final Log log, final long startAddress) {
        this(log, startAddress, Long.MAX_VALUE);
    }

    public DataIterator(@NotNull final Log log, final long startAddress, final long length) {
        this.log = log;
        this.length = length;

        cachePageSize = log.getCachePageSize();
        pageAddressMask = ~((long) (cachePageSize - 1));
        pageAddress = -1L;
        formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();

        if (startAddress >= 0) {
            checkPageSafe(startAddress);
        }
    }

    @Override
    public boolean hasNext() {
        assert length >= 0;

        if (page == null || length == 0) {
            return false;
        }

        if (pageOffset >= chunkLength) {
            checkPageSafe(getAddress());
            return hasNext();
        }

        return true;
    }

    @Override
    public byte next() {
        if (!hasNext()) {
            DataCorruptionException.raise(
                    "DataIterator: no more bytes available", log, getAddress());
        }

        assert length > 0;
        var current = pageOffset;
        pageOffset++;
        length--;

        assert pageOffset <= chunkLength;

        return page[current];
    }

    @Override
    public long skip(final long bytes) {
        if (bytes <= 0) {
            return 0;
        }

        final long bytesToSkip = Math.min(bytes, length);
        final long pageBytesToSkip = Math.min(bytesToSkip, chunkLength - pageOffset);
        pageOffset += pageBytesToSkip;

        if (bytesToSkip > pageBytesToSkip) {
            long chunkSize = cachePageSize - BufferedDataWriter.LOGGABLE_DATA;

            if (!formatWithHashCodeIsUsed) {
                chunkSize = cachePageSize;
            }

            final long rest = bytesToSkip - pageBytesToSkip;

            final long pagesToSkip = rest / chunkSize;
            final long pageSkip = pagesToSkip * cachePageSize;
            final long offsetSkip = pagesToSkip * chunkSize;


            final int pageOffset = (int) (rest - offsetSkip);
            final long addressDiff = pageSkip + pageOffset;

            checkPageSafe(getAddress() + addressDiff);
        }

        length -= bytesToSkip;
        return bytesToSkip;
    }

    @Override
    public long nextLong(final int length) {
        if (this.length < length) {
            DataCorruptionException.raise(
                    "DataIterator: no more bytes available", log, getAddress());
        }

        if (page == null || this.chunkLength - pageOffset < length) {
            return LongBinding.entryToUnsignedLong(this, length);
        }
        final long result = LongBinding.entryToUnsignedLong(page, pageOffset, length);

        pageOffset += length;
        this.length -= length;

        return result;
    }

    public void checkPage(long address) {
        final long pageAddress = address & pageAddressMask;

        if (this.pageAddress != pageAddress) {
            page = log.getCachedPage(pageAddress);
            this.pageAddress = pageAddress;
        }

        chunkLength = cachePageSize - BufferedDataWriter.LOGGABLE_DATA;
        if (!formatWithHashCodeIsUsed) {
            chunkLength = cachePageSize;
        }

        pageOffset = (int) (address - pageAddress);
    }

    @Override
    public int available() {
        if (length > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException();
        }

        return (int) length;
    }

    @Override
    public long getAddress() {
        assert pageOffset <= chunkLength;

        if (pageOffset < chunkLength) {
            return pageAddress + pageOffset;
        }

        //current page is exhausted and we point to the next page
        return pageAddress + cachePageSize;
    }

    public int getOffset() {
        return pageOffset;
    }

    private void checkPageSafe(final long address) {
        try {
            checkPage(address);
            final long pageAddress = address & pageAddressMask;

            chunkLength = (int) Math.min(log.getHighAddress() - pageAddress,
                    cachePageSize - BufferedDataWriter.LOGGABLE_DATA);
            if (!formatWithHashCodeIsUsed) {
                chunkLength = (int) Math.min(log.getHighAddress() - pageAddress,
                        cachePageSize);
            }

            if (chunkLength > pageOffset) {
                return;
            }
        } catch (BlockNotFoundException ignore) {
        }
        pageAddress = -1L;
        page = null;
    }

    public byte[] getCurrentPage() {
        return page;
    }

    public boolean availableInCurrentPage(final int bytes) {
        return chunkLength - pageOffset >= bytes;
    }
}
