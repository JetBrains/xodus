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
package jetbrains.exodus;

import jetbrains.exodus.util.ArrayBackedByteIterable;
import jetbrains.exodus.util.UTFUtil;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A compound {@link ByteIterable} that can be composed of several sub-iterables.
 */
public final class CompoundByteIterable implements ByteIterable {
    final ByteIterable[] iterables;
    final boolean isArrayBacked;

    final int count;
    int length = -1;

    /*
     * Auxiliary variables introduced to avoid duplication during processing of data
     */
    int currentChunkLength;
    int currentChunkIndex;
    int currentChunkOffset;
    byte[] currentChunk;

    public CompoundByteIterable(ByteIterable... iterables) {
        this(iterables, iterables.length);
    }

    public CompoundByteIterable(ByteIterable[] iterables, int count) {
        if (count < 1) {
            throw new ExodusException("Failed to initialize CompoundByteIterable");
        }

        int currentCount = count;
        ByteIterable[] currentIterables = iterables;

        boolean isArrayBacked = true;
        for (int i = 0; i < currentCount; i++) {
            var iterable = currentIterables[i];
            assert iterable != null;

            isArrayBacked &= iterable instanceof ArrayBackedByteIterable;

            //unwrap recursive compound iterables if there are any
            if (!isArrayBacked && iterable instanceof CompoundByteIterable compoundByteIterable) {
                var iterableCount = compoundByteIterable.count;

                var oldIterables = currentIterables;
                currentIterables = new ByteIterable[currentCount + iterableCount - 1];

                System.arraycopy(oldIterables, 0, currentIterables, 0, i);
                System.arraycopy(compoundByteIterable.iterables, 0, currentIterables, i,
                        iterableCount);
                System.arraycopy(oldIterables, i + 1, currentIterables, i + iterableCount,
                        (currentCount - i - 1));

                currentCount += iterableCount - 1;
                i += iterableCount - 1;
            }
        }

        this.iterables = currentIterables;
        this.count = currentCount;
        this.isArrayBacked = isArrayBacked;
    }

    private CompoundByteIterable(final ByteIterable[] iterables, final boolean isArrayBacked,
                                 final int count) {
        this.iterables = iterables;
        this.isArrayBacked = isArrayBacked;
        this.count = count;
    }

    @Override
    public int getLength() {
        int result = length;

        if (result == -1) {
            result = 0;
            for (int i = 0; i < count; ++i) {
                assert !(iterables[i] instanceof CompoundByteIterable);

                var length = iterables[i].getLength();
                result += length;
            }

            length = result;
        }
        return result;
    }

    @Override
    public byte[] getBytesUnsafe() {
        var result = new byte[getLength()];

        var written = 0;
        for (int i = 0; i < count; i++) {
            var iterable = iterables[i];

            if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                var len = iterable.getLength();
                System.arraycopy(arrayBackedByteIterable.bytes, arrayBackedByteIterable.offset, result,
                        written, len);

                written += len;
            } else {
                var array = iterable.getBytesUnsafe();
                var arrayLength = iterable.getLength();

                System.arraycopy(array, 0, result, written, arrayLength);

                written += arrayLength;
            }
        }

        return result;
    }

    @Override
    public byte getByte(int offset) {
        var iterable = findFirstIterable(offset);
        return iterable.getByte(currentChunkOffset);
    }

    @Override
    public int mismatch(ByteIterable other) {
        int otherOffset;
        byte[] otherArray;

        int otherLength = other.getLength();

        if (other instanceof ArrayBackedByteIterable otherArrayBackedByteIterable) {
            otherOffset = otherArrayBackedByteIterable.offset;
            otherArray = otherArrayBackedByteIterable.bytes;
        } else {
            otherOffset = 0;
            otherArray = other.getBytesUnsafe();
        }

        return mismatchWithArray(otherArray, otherOffset, otherLength);
    }

    @Override
    public @NotNull ByteIterable subIterable(int offset, int length) {
        if (length == 0) {
            return ArrayBackedByteIterable.EMPTY;
        }

        final int currentLength = getLength();

        if (offset < 0 || currentLength < offset + length) {
            throw new IndexOutOfBoundsException();
        }

        if (offset == 0 && currentLength == length) {
            return this;
        }

        final int firstIterableIndex;
        ByteIterable firstIterable;

        final int lastIterableIndex;
        ByteIterable lastIterable;

        firstIterable = findFirstIterable(offset);

        final int firstIterableLength = firstIterable.getLength();
        firstIterable = firstIterable.subIterable(currentChunkOffset,
                Math.min(length, firstIterableLength - currentChunkOffset));

        if (count == 1) {
            return firstIterable;
        } else {
            firstIterableIndex = currentChunkIndex;

            if (currentChunkIndex == count - 1) {
                return firstIterable;
            }

            var iterable = findFirstIterable(this.currentChunkIndex,
                    offset - this.currentChunkOffset,
                    offset + length - 1);

            if (currentChunkIndex > firstIterableIndex) {
                lastIterable = iterable.subIterable(0, currentChunkOffset + 1);
            } else {
                return firstIterable;
            }

            lastIterableIndex = currentChunkIndex;
        }

        var subIterables = new ByteIterable[lastIterableIndex - firstIterableIndex + 1];
        if (subIterables.length > 2) {
            System.arraycopy(iterables, firstIterableIndex + 1, subIterables, 1, subIterables.length - 2);
        }

        subIterables[0] = firstIterable;
        subIterables[subIterables.length - 1] = lastIterable;


        return new CompoundByteIterable(subIterables, isArrayBacked, subIterables.length);
    }

    private int mismatchWithArray(byte[] array, int offset, int length) {
        var compared = 0;

        for (int i = 0; i < count; i++) {
            var iterable = iterables[i];
            assert !(iterable instanceof CompoundByteIterable);

            byte[] iterableArray;
            int iterableOffset;

            int iterableLen = iterable.getLength();

            if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                iterableOffset = arrayBackedByteIterable.offset;
                iterableArray = arrayBackedByteIterable.bytes;
            } else {
                iterableArray = iterable.getBytesUnsafe();
                iterableOffset = 0;
            }


            if (i < count - 1) {
                var mismatchLen = Math.min(iterableLen, length - compared);
                var localMismatch = Arrays.mismatch(iterableArray, iterableOffset,
                        iterableOffset + iterableLen, array, offset + compared,
                        offset + compared + mismatchLen);

                if (localMismatch >= 0) {
                    return localMismatch + compared;
                }

                if (compared + mismatchLen == length) {
                    final int currentLength = getLength();

                    if (currentLength == length) {
                        return -1;
                    } else {
                        return length;
                    }
                }

                compared += mismatchLen;
            } else {
                final int localMismatch = Arrays.mismatch(iterableArray, iterableOffset,
                        iterableOffset + iterableLen, array,
                        offset + compared, offset + length);

                if (localMismatch == -1) {
                    return -1;
                }

                return localMismatch + compared;
            }
        }

        return -1;
    }

    @Override
    public int compareTo(@NotNull ByteIterable right) {
        if (right instanceof ArrayBackedByteIterable rightArrayBackedByteIterable) {
            var rightIterableLen = rightArrayBackedByteIterable.getLength();
            var rightIterableBytes = rightArrayBackedByteIterable.bytes;
            var rightIterableOffset = rightArrayBackedByteIterable.offset;

            return compareWithArray(rightIterableLen, rightIterableOffset, rightIterableBytes);
        } else if (right instanceof CompoundByteIterable compoundByteIterable) {
            return compareCompoundIterables(iterables, count, compoundByteIterable.iterables,
                    compoundByteIterable.count);
        }

        var rightArray = right.getBytesUnsafe();
        var rightLen = right.getLength();

        return compareWithArray(rightLen, 0, rightArray);
    }

    @Override
    public void writeIntoBuffer(ByteBuffer buffer, int bufferPosition) {
        for (var iterable : iterables) {
            assert !(iterable instanceof CompoundByteIterable);

            var len = iterable.getLength();
            iterable.writeIntoBuffer(buffer, bufferPosition);

            bufferPosition += len;
        }
    }

    @Override
    public boolean isEmpty() {
        for (int i = 0; i < count; i++) {
            var iterable = iterables[i];
            assert !(iterable instanceof CompoundByteIterable);

            if (!iterable.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public int reversedCommonPrefix(byte[] array, int offset, int len) {
        final int mismatch = mismatchWithArray(array, offset, len);

        if (mismatch == len) {
            return mismatch;
        }

        if (mismatch + 1 == getLength()) {
            int last = count - 1;


            while (true) {
                var iterable = iterables[last];
                assert !(iterable instanceof CompoundByteIterable);

                if (!iterable.isEmpty()) {
                    var mismatchedByteFirst = array[offset + mismatch];
                    var mismatchedByteSecond = iterable.getByte(iterable.getLength() - 1);

                    if (Byte.toUnsignedInt(mismatchedByteSecond) - Byte.toUnsignedInt(mismatchedByteFirst) == 1) {
                        return mismatch + 1;
                    }

                    return mismatch;
                }

                last--;
            }
        }

        return mismatch;
    }

    @Override
    public int commonPrefix(ByteIterable other) {
        assert compareTo(other) < 0;

        byte[] otherArray;
        int otherOffset;

        var otherLen = other.getLength();

        if (other instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            otherArray = arrayBackedByteIterable.bytes;
            otherOffset = arrayBackedByteIterable.offset;
        } else {
            otherArray = other.getBytesUnsafe();
            otherOffset = 0;
        }

        int mismatch = mismatchWithArray(otherArray, otherOffset, otherLen);
        if (mismatch == getLength()) {
            return mismatch;
        }

        if (mismatch + 1 == otherLen) {
            var mismatchedByteFirst = getByte(mismatch);
            var mismatchedByteSecond = otherArray[otherOffset + mismatch];

            if (Byte.toUnsignedInt(mismatchedByteSecond) - Byte.toUnsignedInt(mismatchedByteFirst) == 1) {
                return mismatch + 1;
            }
        }

        return mismatch;
    }

    private ByteIterable findFirstIterable(final int offset) {
        return findFirstIterable(0, 0, offset);
    }

    private ByteIterable findFirstIterable(final int start, final int sinceLen, final int offset) {
        int len = sinceLen;
        int chunkIndex;
        int chunkLength;

        ByteIterable iterable = null;

        for (chunkIndex = start; chunkIndex < count; chunkIndex++) {
            iterable = iterables[chunkIndex];
            assert !(iterable instanceof CompoundByteIterable);

            chunkLength = iterable.getLength();
            if (len + chunkLength > offset) {
                break;
            }

            len += chunkLength;
        }

        if (chunkIndex == count) {
            throw new IndexOutOfBoundsException();
        }

        assert iterable != null;

        this.currentChunkIndex = chunkIndex;
        this.currentChunkOffset = offset - len;

        return iterable;
    }

    private void findFirstChunk(final int offset) {
        ByteIterable iterable = null;
        int chunkIndex;
        int chunkLength = 0;

        int len = 0;
        for (chunkIndex = 0; chunkIndex < count; chunkIndex++) {
            iterable = iterables[chunkIndex];
            assert !(iterable instanceof CompoundByteIterable);

            chunkLength = iterable.getLength();
            if (len + chunkLength > offset) {
                break;
            }

            len += chunkLength;
        }

        if (chunkIndex == count) {
            throw new IndexOutOfBoundsException();
        }

        byte[] chunk;
        int chunkOffset;

        assert iterable != null;

        if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            chunk = arrayBackedByteIterable.bytes;
            chunkOffset = arrayBackedByteIterable.offset + (offset - len);
        } else {
            chunkOffset = offset - len;
            chunk = iterable.getBytesUnsafe();
        }


        this.currentChunkIndex = chunkIndex;
        this.currentChunkLength = chunkLength;
        this.currentChunk = chunk;
        this.currentChunkOffset = chunkOffset;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof ByteIterable && compareTo((ByteIterable) obj) == 0;
    }

    /**
     * @return hash code computed using all bytes of the iterable.
     */
    @Override
    public int hashCode() {
        final byte[] a = getBytesUnsafe();
        int result = 1;
        final int length = getLength();
        for (int i = 0; i < length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    @Override
    public String toString() {
        return ByteIterableBase.toString(getBytesUnsafe(), 0, getLength());
    }


    private void moveToNextChunk() {
        while (true) {
            currentChunkIndex++;

            if (currentChunkIndex >= count) {
                throw new IndexOutOfBoundsException();
            }

            var iterable = iterables[currentChunkIndex];
            assert !(iterable instanceof CompoundByteIterable);

            assert iterable != null;

            var chunkLength = iterable.getLength();
            if (chunkLength == 0) {
                continue;
            }

            this.currentChunkLength = chunkLength;
            if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                this.currentChunk = arrayBackedByteIterable.bytes;
                this.currentChunkOffset = arrayBackedByteIterable.offset;
            } else {
                this.currentChunkOffset = 0;
                this.currentChunk = iterable.getBytesUnsafe();
            }
            break;
        }
    }

    @Override
    public String getString(final int offset) {
        try {
            findFirstChunk(offset);

            int next = Byte.toUnsignedInt(currentChunk[currentChunkOffset]);
            currentChunkOffset++;

            if (next == UTFUtil.NULL_STRING_UTF_VALUE) {
                if (currentChunkOffset >= currentChunkLength) {
                    moveToNextChunk();
                }

                next = currentChunk[currentChunkOffset];
                if (next == 0) {
                    return null;
                }
                throw new IllegalArgumentException();
            }

            if (next == 0) {
                return "";
            }

            final char[] chars = new char[getLength() - 1]; // minus trailing zero
            int j = 0;
            do {
                if (next < 128) {
                    chars[j++] = (char) next;
                } else {
                    final int high = next >> 4;
                    if (high == 12 || high == 13) {
                        if (currentChunkOffset >= currentChunkLength) {
                            moveToNextChunk();
                        }
                        final int char2 = currentChunk[currentChunkOffset++] & 0xff;

                        if ((char2 & 0xC0) != 0x80) {
                            throw new IllegalArgumentException();
                        }
                        chars[j++] = (char) (((next & 0x1F) << 6) | (char2 & 0x3F));

                    } else if (high == 14) {
                        if (currentChunkOffset >= currentChunkLength) {
                            moveToNextChunk();
                        }
                        final int char2 = currentChunk[currentChunkOffset++] & 0xff;

                        if (currentChunkOffset >= currentChunkLength) {
                            moveToNextChunk();
                        }
                        final int char3 = currentChunk[currentChunkOffset++] & 0xff;

                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                            throw new IllegalArgumentException();
                        }

                        chars[j++] = (char) (((next & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F)));
                    } else {
                        throw new IllegalArgumentException();
                    }


                    if (currentChunkOffset >= currentChunkLength) {
                        moveToNextChunk();
                    }

                    next = currentChunk[currentChunkOffset++] & 0xff;
                }
            } while (next != 0);

            return new String(chars, 0, j);
        } finally {
            currentChunk = null;
        }
    }

    private int compareWithArray(final int rightIterableLen, final int rightIterableOffset,
                                 byte @NotNull [] rightIterableBytes) {
        var compared = 0;

        for (int i = 0; i < count; i++) {
            var iterable = iterables[i];
            assert !(iterable instanceof CompoundByteIterable);

            var iterableLen = iterable.getLength();
            var lenToCompare = Math.min(iterableLen, rightIterableLen - compared);

            var rightOffset = rightIterableOffset + compared;
            int cmp;
            if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                cmp = Arrays.compareUnsigned(arrayBackedByteIterable.bytes, arrayBackedByteIterable.offset,
                        arrayBackedByteIterable.offset + lenToCompare,
                        rightIterableBytes, rightOffset, rightOffset + lenToCompare);
            } else {
                var array = iterable.getBytesUnsafe();
                cmp = Arrays.compareUnsigned(array, 0, lenToCompare, rightIterableBytes,
                        rightOffset, rightOffset + lenToCompare);
            }

            if (cmp != 0) {
                return cmp;
            }

            if (compared + lenToCompare == rightIterableLen || i == count - 1) {
                //length of current iterable is bigger than array
                if (i < count - 1) {
                    return 1;
                }

                return iterableLen - (rightIterableLen - compared);
            }

            compared += lenToCompare;
        }

        return 0;
    }

    private int compareCompoundIterables(ByteIterable[] firstIterables, int firstCount,
                                         ByteIterable[] secondIterables,
                                         int secondCount) {
        ByteIterable firstIterable = firstIterables[0];
        int firstIterableIndex = 0;
        int firstIterableOffset = 0;
        int firstIterableLength = firstIterable.getLength();


        ByteIterable secondIterable = secondIterables[0];
        int secondIterableIndex = 0;
        int secondIterableOffset = 0;
        int secondIterableLength = secondIterable.getLength();


        while (true) {
            int cmp;

            int iterableLength = Math.min(firstIterableLength - firstIterableOffset,
                    secondIterableLength - secondIterableOffset);
            if (firstIterable instanceof ArrayBackedByteIterable firstArrayBacked &&
                    secondIterable instanceof ArrayBackedByteIterable secondArrayBacked) {
                cmp = Arrays.compareUnsigned(firstArrayBacked.bytes,
                        firstArrayBacked.offset + firstIterableOffset,
                        firstArrayBacked.offset + firstIterableOffset + iterableLength,
                        secondArrayBacked.bytes,
                        secondArrayBacked.offset + secondIterableOffset,
                        secondArrayBacked.offset + secondIterableOffset + iterableLength);
            } else {
                cmp = firstIterable.subIterable(firstIterableOffset, iterableLength).compareTo(
                        secondIterable.subIterable(secondIterableOffset, iterableLength));
            }

            if (cmp != 0) {
                return cmp;
            }

            firstIterableOffset += iterableLength;
            secondIterableOffset += iterableLength;

            if (firstIterableOffset == firstIterableLength) {
                firstIterableIndex++;
                firstIterableOffset = 0;

                if (firstIterableIndex == firstCount) {
                    if (secondIterableOffset < secondIterableLength || secondIterableIndex < secondCount) {
                        return -1;
                    }

                    return 0;
                }

                firstIterable = firstIterables[firstIterableIndex];
                firstIterableLength = firstIterable.getLength();
            }

            if (secondIterableOffset == secondIterableLength) {
                secondIterableIndex++;
                secondIterableOffset = 0;

                if (secondIterableIndex == secondCount) {
                    if (firstIterableOffset < secondIterableLength || firstIterableIndex < firstCount) {
                        return 1;
                    }

                    return 0;
                }

                secondIterable = secondIterables[secondIterableIndex];
                secondIterableLength = secondIterable.getLength();
            }
        }
    }

    @Override
    public ByteIterator iterator() {
        if (isArrayBacked) {
            return new ArrayBackedCompoundIterator();
        }

        return new CompoundByteIterator();
    }

    private final class ArrayBackedCompoundIterator extends ByteIterator {
        int currentIterable;
        byte[] currentChunk;
        int currentChunkOffset;
        int currentChunkLimit;

        public ArrayBackedCompoundIterator() {
            var iterable = (ArrayBackedByteIterable) iterables[0];

            currentChunk = iterable.bytes;
            currentChunkOffset = iterable.offset;
            currentChunkLimit = iterable.limit;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentChunkOffset < currentChunkLimit) {
                    return true;
                }

                if (currentIterable == count - 1) {
                    return false;
                }
                currentIterable++;

                var iterable = (ArrayBackedByteIterable) iterables[currentIterable];
                if (iterable.offset < iterable.limit) {
                    currentChunk = iterable.bytes;
                    currentChunkOffset = iterable.offset;
                    currentChunkLimit = iterable.limit;

                    return true;
                }
            }
        }

        @Override
        public byte next() {
            if (hasNext()) {
                currentChunkOffset++;
                return currentChunk[currentChunkOffset];
            }
            throw new NoSuchElementException();
        }

        @Override
        public long skip(long bytes) {
            int skipped = 0;
            while (skipped < bytes && hasNext()) {
                skipped += currentChunkLimit - currentChunkOffset;
                currentChunkOffset = currentChunkLimit;
            }

            return skipped;
        }
    }

    private final class CompoundByteIterator extends ByteIterator {
        int currentIterable;

        ByteIterator currentIterator;

        byte[] currentChunk;
        int currentChunkOffset;
        int currentChunkLimit;

        public CompoundByteIterator() {
            var iterable = iterables[0];

            if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                currentChunk = arrayBackedByteIterable.bytes;
                currentChunkOffset = arrayBackedByteIterable.offset;
                currentChunkLimit = arrayBackedByteIterable.limit;
            } else {
                currentIterator = iterable.iterator();
            }
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentIterator == null) {
                    if (currentChunkOffset < currentChunkLimit) {
                        return true;
                    }
                } else if (currentIterator.hasNext()) {
                    return true;
                }

                if (currentIterable == count - 1) {
                    return false;
                }
                currentIterable++;

                var iterable = iterables[currentIterable];

                if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
                    currentIterator = null;
                    currentChunk = arrayBackedByteIterable.bytes;
                    currentChunkOffset = arrayBackedByteIterable.offset;
                    currentChunkLimit = arrayBackedByteIterable.limit;
                } else {
                    currentIterator = iterable.iterator();
                }
            }
        }

        @Override
        public byte next() {
            if (hasNext()) {
                if (currentIterator != null) {
                    return currentIterator.next();
                } else {
                    currentChunkOffset++;
                    return currentChunk[currentChunkOffset];
                }
            }

            throw new NoSuchElementException();
        }

        @Override
        public long skip(long bytes) {
            int skipped = 0;
            while (skipped < bytes && hasNext()) {
                if (currentIterator == null) {
                    skipped += currentChunkLimit - currentChunkOffset;
                    currentChunkOffset = currentChunkLimit;
                } else {
                    skipped += currentIterator.skip(bytes - skipped);
                }
            }

            return skipped;
        }
    }
}
