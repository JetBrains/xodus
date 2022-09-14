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

/**
 * A compound {@link ByteIterable} that can be composed of several sub-iterables.
 */
public final class CompoundByteIterable extends ByteIterableBase {
    final ByteIterable[] iterables;
    final int count;

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

        this.iterables = iterables;
        this.count = count;
    }


    @Override
    public int getLength() {
        int result = length;

        if (result == -1) {
            result = 0;
            for (int i = 0; i < count; ++i) {
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
    public int mismatch(ByteIterable other) {
        var compared = 0;

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

        int mismatch = -1;
        for (int i = 0; i < count; i++) {
            var iterable = iterables[i];

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
                var mismatchLen = Math.min(iterableLen, otherLength - compared);
                var localMismatch = Arrays.mismatch(iterableArray, iterableOffset,
                        iterableOffset + iterableLen, otherArray, otherOffset + compared,
                        otherOffset + compared + mismatchLen);

                if (localMismatch >= 0 || compared + mismatchLen == otherLength) {
                    return localMismatch + compared;
                }

                compared += mismatchLen;
            } else {
                return Arrays.mismatch(iterableArray, 0, iterableArray.length, otherArray,
                        otherOffset + compared, otherOffset + otherLength - compared)
                        + compared;
            }

        }

        return mismatch;
    }


    @Override
    public int compareTo(@NotNull ByteIterable right) {
        if (right instanceof ArrayBackedByteIterable rightArrayBackedByteIterable) {
            var rightIterableLen = rightArrayBackedByteIterable.getLength();
            var rightIterableBytes = rightArrayBackedByteIterable.bytes;
            var rightIterableOffset = rightArrayBackedByteIterable.offset;

            return compareWithArray(rightIterableLen, rightIterableOffset, rightIterableBytes);
        }

        var rightArray = right.getBytesUnsafe();
        var rightLen = right.getLength();

        return compareWithArray(rightLen, 0, rightArray);
    }

    @Override
    public void writeIntoBuffer(ByteBuffer buffer, int bufferPosition) {
        for (var iterable : iterables) {
            var len = iterable.getLength();
            iterable.writeIntoBuffer(buffer, bufferPosition);

            bufferPosition += len;
        }
    }

    private void findFirstChunk(final int offset) {
        ByteIterable iterable = null;
        int chunkIndex;
        int chunkLength = 0;
        int len = 0;

        for (chunkIndex = 0; chunkIndex < count; chunkIndex++) {
            iterable = iterables[chunkIndex];

            chunkLength = iterable.getLength();
            if (len + chunkLength > offset) {
                break;
            }

            len += chunkLength;
        }

        if (len <= offset) {
            throw new IndexOutOfBoundsException();
        }

        byte[] chunk;
        int chunkOffset;

        assert iterable != null;

        if (iterable instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            chunk = arrayBackedByteIterable.bytes;
            chunkOffset = arrayBackedByteIterable.offset + (len - offset);
        } else {
            chunkOffset = len - offset;
            chunk = iterable.getBytesUnsafe();
        }


        this.currentChunkIndex = chunkIndex;
        this.currentChunkLength = chunkLength;
        this.currentChunk = chunk;
        this.currentChunkOffset = chunkOffset;
    }

    private void moveToNextChunk() {
        while (true) {
            currentChunkIndex++;

            if (currentChunkIndex >= count) {
                throw new IndexOutOfBoundsException();
            }

            var iterable = iterables[currentChunkIndex];
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

    @Override
    protected ByteIterator getIterator() {
        return new CompoundByteIteratorBase(iterables[0].iterator()) {
            int off = 0;

            @Override
            public ByteIterator nextIterator() {
                off++;
                return off < count ? iterables[off].iterator() : null;
            }
        };
    }
}
