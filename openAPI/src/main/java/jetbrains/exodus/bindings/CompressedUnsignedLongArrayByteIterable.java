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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.ByteIterator;

public final class CompressedUnsignedLongArrayByteIterable extends ByteIterableBase {

    private final long[] longs;
    private final int size;
    private final int bytesPerLong;

    private CompressedUnsignedLongArrayByteIterable(long[] longs, int size) {
        if (size > longs.length) {
            throw new IllegalArgumentException();
        }
        this.longs = longs;
        this.size = size;
        int logarithm = 0;
        for (int i = 0; i < size; ++i) {
            long l = longs[i];
            if (l < 0) {
                throw new IllegalArgumentException();
            }
            final int log = logarithm(l);
            if (log > logarithm) {
                logarithm = log;
            }
        }
        bytesPerLong = logarithm;
    }

    public static ByteIterable getIterable(long[] longs) {
        return getIterable(longs, longs.length);
    }

    public static ByteIterable getIterable(long[] longs, int size) {
        return new CompressedUnsignedLongArrayByteIterable(longs, size);
    }

    /**
     * @param output   array of longs to load to.
     * @param iterator read data from.
     * @return number of bytes which occupies each long in iterator.
     */
    public static int loadLongs(long[] output, ByteIterator iterator) {
        return loadLongs(output, iterator, output.length);
    }

    /**
     * @param output   array of longs to load to.
     * @param iterator read data from.
     * @param size     number of longs to load.
     * @return number of bytes which occupies each long in iterator.
     */
    public static int loadLongs(long[] output, ByteIterator iterator, int size) {
        if (size > 0) {
            final int bytesPerLong = iterator.next();
            for (int i = 0; i < size; ++i) {
                output[i] = iterator.nextLong(bytesPerLong);
            }
            return bytesPerLong;
        }
        return 0;
    }

    /**
     * @param output       array of longs to load to.
     * @param iterator     read data from.
     * @param size         number of longs to load.
     * @param bytesPerLong number of bytes which occupies each long.
     * @return number of bytes which occupies each long in iterator.
     */
    public static int loadLongs(long[] output, ByteIterator iterator, int size, int bytesPerLong) {
        if (size > 0) {
            for (int i = 0; i < size; ++i) {
                output[i] = iterator.nextLong(bytesPerLong);
            }
            return bytesPerLong;
        }
        return 0;
    }

    public static int logarithm(long l) {
        int result = 0;
        do {
            ++result;
            l >>= 8;
        } while (l > 0);
        return result;
    }

    @Override
    protected ByteIterator getIterator() {
        return size == 0 ?
                ByteIterable.EMPTY_ITERATOR :
                new ByteIterator() {
                    private int i = -1;
                    private int bits = 8;
                    private long data = bytesPerLong;

                    @Override
                    public boolean hasNext() {
                        return i < size - 1 || bits > 0;
                    }

                    @Override
                    public byte next() {
                        if (bits == 0) {
                            bits = bytesPerLong << 3;
                            data = longs[++i];
                        }
                        return (byte) ((data >> (bits -= 8)) & 0xff);
                    }

                    //TODO: implement
                    @Override
                    public long skip(final long bytes) {
                        throw new UnsupportedOperationException();
                    }
                };
    }
}
