/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import org.jetbrains.annotations.NotNull;

public final class DataIterator extends ByteIterator {

    @NotNull
    private final Log log;
    private long pageAddress;
    private ArrayByteIterable page;
    private int offset;
    private int length;

    public DataIterator(@NotNull final Log log, final long startAddress) {
        this.log = log;
        pageAddress = -1L;
        checkPage(startAddress);
    }

    @Override
    public boolean hasNext() {
        if (page == null) {
            return false;
        }
        if (offset >= length) {
            checkPage(getHighAddress());
            return hasNext();
        }
        return true;
    }

    @Override
    public byte next() {
        if (!hasNext()) {
            throw new ExodusException("DataIterator: no more bytes available" +
                LogUtil.getWrongAddressErrorMessage(getHighAddress(), log.getFileSize()));
        }
        return page.getBytesUnsafe()[offset++];
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
            checkPage(getHighAddress());
        }
        return skipped;
    }

    @Override
    public long nextLong(final int length) {
        if (page == null || this.length - offset < length) {
            return LongBinding.entryToUnsignedLong(this, length);
        }
        final long result = LongBinding.entryToUnsignedLong(page.getBytesUnsafe(), offset, length);
        offset += length;
        return result;
    }

    public void checkPage(final long highAddress) {
        final int offset = ((int) highAddress) & (log.getCachePageSize() - 1);
        final long pageAddress = highAddress - offset;
        if (this.pageAddress != pageAddress) {
            if (loadPage(pageAddress) == null) {
                return;
            }
        }
        int len = page.getLength();
        if (len <= offset) { // offset is >= 0 for sure
            // we should try to reload page since this page can near the right bound of the log (highAddress)
            if (loadPage(pageAddress) == null) {
                return;
            }
            len = page.getLength();
            if (len <= offset) { // offset is >= 0 for sure
                this.pageAddress = -1L;
                page = null;
                return;
            }
        }
        this.length = len;
        this.offset = offset;
    }

    private ArrayByteIterable loadPage(long pageAddress) {
        try {
            page = log.cache.getPage(log, pageAddress);
            this.pageAddress = pageAddress;
        } catch (BlockNotFoundException e) {
            this.pageAddress = -1L;
            this.page = null;
        }
        return page;
    }

    byte[] getCurrentPage() {
        return page.getBytesUnsafe();
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
}
