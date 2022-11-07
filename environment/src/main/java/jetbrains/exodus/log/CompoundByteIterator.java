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

import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;

class CompoundByteIterator extends ByteIteratorWithAddress implements BlockByteIterator {

    @NotNull
    private ArrayByteIterable.Iterator current;
    private boolean hasNext;
    private boolean hasNextValid;
    private long currentAddress;
    private int read;
    private int offset;
    private final Log log;
    private final boolean formatWithHashCodeIsUsed;
    private final int cachePageSize;

    CompoundByteIterator(final long address, final Log log) {
        current = ArrayByteIterable.EMPTY.ITERATOR;
        currentAddress = address;
        read = 0;
        offset = 0;
        this.log = log;
        formatWithHashCodeIsUsed = log.getFormatWithHashCodeIsUsed();
        cachePageSize = log.getCachePageSize();

        assert !formatWithHashCodeIsUsed ||
                getAddress() % cachePageSize < cachePageSize - BufferedDataWriter.LOGGABLE_DATA;
    }

    @Override
    public boolean hasNext() {
        if (!hasNextValid) {
            hasNext = hasNextImpl();
            hasNextValid = true;
        }
        return hasNext;
    }

    @Override
    public long skip(final long bytes) {
        long skipped = current.skip(bytes);
        while (true) {
            hasNextValid = false;
            if (skipped >= bytes || !hasNext()) {
                break;
            }
            skipped += current.skip(bytes - skipped);
        }
        return skipped;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public byte next() {
        if (!hasNext()) {
            DataCorruptionException.raise(
                    "CompoundByteIterator: no more bytes available", log, getAddress());
        }

        final byte result = current.next();
        hasNextValid = false;
        return result;
    }

    private boolean hasNextImpl() {
        while (!current.hasNext()) {
            currentAddress += read;

            final int pageSize = log.getCachePageSize();

            int alignment = ((int) currentAddress) & (pageSize - 1);
            long alignedAddress = currentAddress - alignment;

            if (formatWithHashCodeIsUsed) {
                int metadataDelta = alignment - (pageSize - BufferedDataWriter.LOGGABLE_DATA);

                if (metadataDelta >= 0) {
                    alignedAddress += pageSize;
                    alignment = metadataDelta;

                    currentAddress = alignedAddress + metadataDelta;
                }
            }

            final ArrayByteIterable page = log.cache.getPageIterable(log, alignedAddress, formatWithHashCodeIsUsed);

            int readBytes = page.getLength();
            if (readBytes <= alignment) { // alignment is >= 0 for sure
                read = 0;
                offset = 0;
                return false;
            }

            read = readBytes - alignment;
            current = page.iterator(alignment);
            offset = current.getOffset();
        }

        assert !formatWithHashCodeIsUsed || getAddress() % cachePageSize <
                log.getCachePageSize() - BufferedDataWriter.LOGGABLE_DATA;

        return true;
    }

    @Override
    public long getAddress() {
        if (current.hasNext()) {
            return currentAddress + current.getOffset() - offset;
        }

        //current page is exhausted and we point to the next page
        return (currentAddress & (~(long) (cachePageSize - 1))) + cachePageSize;

    }

    @Override
    public int nextBytes(byte[] array, int off, int len) {
        if (!hasNext()) {
            DataCorruptionException.raise(
                    "CompoundByteIterator: no more bytes available", log, getAddress());
        }
        int read = current.nextBytes(array, off, len);
        hasNextValid = false;
        if (read < len) {
            while (read < len) {
                array[off + read] = next();
                ++read;
            }
        }
        return read;
    }
}
