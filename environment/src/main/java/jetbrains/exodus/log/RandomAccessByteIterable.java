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

import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

class RandomAccessByteIterable extends ByteIterableWithAddress {

    @NotNull
    private final Log log;

    public RandomAccessByteIterable(final long address, @NotNull final Log log) {
        super(address);
        this.log = log;
    }

    @Override
    public ByteIteratorWithAddress iterator() {
        return new CompoundByteIterator(getDataAddress(), log);
    }

    public ByteIteratorWithAddress iterator(final int offset) {
        return new CompoundByteIterator(getDataAddress() + offset, log);
    }

    public int compareTo(final int offset, final int len, @NotNull final ByteIterable right) {
        return compare(offset, len, right, log, getDataAddress());
    }

    @NotNull
    public RandomAccessByteIterable clone(final int offset) {
        return new RandomAccessByteIterable(getDataAddress() + offset, log);
    }

    private static int compare(final int offset, final int len, final ByteIterable data, Log log, final long address) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        final int mask = pageSize - 1;

        long currentAddress = address + offset;
        long startPageAddress = currentAddress - (currentAddress & mask);

        int pageOffset = (int) (currentAddress - startPageAddress);

        long endPageAddress = currentAddress + len;
        endPageAddress += pageSize - (endPageAddress & mask);

        ByteBuffer buffer;
        if (data instanceof ByteBufferIterable) {
            buffer = ((ByteBufferIterable) data).getByteBuffer().duplicate();
        } else {
            var array = data.getBytesUnsafe();
            buffer = ByteBuffer.wrap(array, 0, data.getLength());
        }

        int startBufferLimit = buffer.limit();
        ByteBuffer page = cache.getPage(log, startPageAddress).duplicate().position(pageOffset);
        if (page.remaining() > len) {
            page.limit(pageOffset + len);
        } else if (page.remaining() < len) {
            buffer.limit(Math.min(page.remaining(), startBufferLimit));
        }

        int comparedBytes = 0;

        long pageAddress = startPageAddress;
        while (true) {
            int cmp = ByteBufferComparator.INSTANCE.compare(page, buffer);

            if (cmp != 0) {
                return cmp;
            }

            pageAddress += pageSize;
            comparedBytes += page.remaining();

            if (pageAddress >= endPageAddress || comparedBytes >= len) {
                break;
            }

            page = cache.getPage(log, pageAddress).duplicate();
            buffer.position(comparedBytes);

            int limit = len - comparedBytes;
            if (page.limit() > limit) {
                page.limit(limit);
                buffer.limit(startBufferLimit);
            } else if (page.limit() < limit) {
                buffer.limit(Math.min(limit + comparedBytes, startBufferLimit));
            }
        }

        return len - comparedBytes;
    }
}
