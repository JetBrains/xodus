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
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

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

    private static int compare(final int offset, final int len, final ByteIterable right, Log log, final long address) {
        final LogCache cache = log.cache;
        final int pageSize = log.getCachePageSize();
        final int mask = pageSize - 1;
        long alignedAddress = address + offset;
        long endAddress = alignedAddress + len;
        endAddress -= ((int) endAddress) & mask;
        int leftStep = ((int) alignedAddress) & mask;
        alignedAddress -= leftStep;
        ArrayByteIterable left = cache.getPageIterable(log, alignedAddress);

        final int leftLen = left.getLength();
        if (leftLen <= leftStep) { // alignment is >= 0 for sure
            BlockNotFoundException.raise(log, alignedAddress);
        }
        byte[] leftArray = left.getBytesUnsafe();
        final byte[] rightArray = right.getBytesUnsafe();
        final int rightLen = right.getLength();
        int rightStep = 0;
        int limit = Math.min(len, Math.min(leftLen - leftStep, rightLen));

        while (true) {
            while (rightStep < limit) {
                byte b1 = leftArray[leftStep++];
                byte b2 = rightArray[rightStep++];
                if (b1 != b2) {
                    return (b1 & 0xff) - (b2 & 0xff);
                }
            }
            if (rightStep == rightLen || alignedAddress >= endAddress) {
                return len - rightLen;
            }
            // move left array to next cache page
            left = cache.getPageIterable(log, alignedAddress += pageSize);
            leftArray = left.getBytesUnsafe();
            leftStep = 0;
            limit = Math.min(len, Math.min(left.getLength() + rightStep, rightLen));
        }
    }
}
