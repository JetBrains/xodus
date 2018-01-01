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

import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;

class CompoundByteIterator extends ByteIteratorWithAddress {

    @NotNull
    private ArrayByteIterable.Iterator current;
    private boolean hasNext;
    private boolean hasNextValid;
    private long currentAddress;
    private int read;
    private int offset;
    private final Log log;

    CompoundByteIterator(final long address, final Log log) {
        current = ArrayByteIterable.EMPTY.ITERATOR;
        currentAddress = address;
        read = 0;
        offset = 0;
        this.log = log;
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
            final int alignment = ((int) currentAddress) & (log.getCachePageSize() - 1);
            final long alignedAddress = currentAddress - alignment;
            final ArrayByteIterable page = log.cache.getPageIterable(log, alignedAddress);
            final int readBytes = page.getLength();
            if (readBytes <= alignment) { // alignment is >= 0 for sure
                read = 0;
                offset = 0;
                return false;
            }
            read = readBytes - alignment;
            current = page.iterator(alignment);
            offset = current.getOffset();
        }
        return true;
    }

    @Override
    public long getAddress() {
        return currentAddress + current.getOffset() - offset;
    }
}
