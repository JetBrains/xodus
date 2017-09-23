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

import jetbrains.exodus.*;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 * This ByteIterable cannot be used for representing comparable or signed longs.
 */
public final class CompressedUnsignedLongByteIterable extends ByteIterableBase {

    private static final int ITERABLES_CACHE_SIZE = 65536;
    private static final ByteIterable[] ITERABLES_CACHE;

    static {
        ITERABLES_CACHE = new ByteIterable[ITERABLES_CACHE_SIZE];
        for (int i = 0; i < ITERABLES_CACHE_SIZE; ++i) {
            //noinspection ObjectAllocationInLoop
            ITERABLES_CACHE[i] = new ArrayByteIterable(new CompressedUnsignedLongByteIterable(i));
        }
    }

    private final long l;

    private CompressedUnsignedLongByteIterable(long l) {
        if (l < 0) {
            throw new IllegalArgumentException(String.valueOf(l));
        }
        this.l = l;
    }

    public static ByteIterable getIterable(long l) {
        if (l < ITERABLES_CACHE_SIZE) {
            return ITERABLES_CACHE[((int) l)];
        }
        return new CompressedUnsignedLongByteIterable(l);
    }

    public static void fillBytes(long l, final LightOutputStream output) {
        if (l < 0) {
            throw new IllegalArgumentException(String.valueOf(l));
        }
        while (true) {
            final byte b = (byte) (l & 0x7f);
            if ((l >>= 7) == 0) {
                output.write(b | 0x80);
                break;
            }
            output.write(b);
        }
    }

    public static long getLong(final ByteIterable iterable) {
        return getLong(iterable.iterator());
    }

    public static long getLong(ByteIterator iterator) {
        long result = 0;
        int shift = 0;
        do {
            final byte b = iterator.next();
            result += (long) (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        } while (iterator.hasNext());
        return throwBadCompressedNumber();
    }

    public static int getInt(final ByteIterable iterable) {
        return getInt(iterable.iterator());
    }

    public static int getInt(ByteIterator iterator) {
        int result = 0;
        int shift = 0;
        do {
            final byte b = iterator.next();
            result += (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        } while (iterator.hasNext());
        return throwBadCompressedNumber();
    }

    public static int getInt(@NotNull final DataIterator iterator) {
        byte b = iterator.next();
        if ((b & 0x80) != 0) {
            return b & 0x7f;
        }
        int result = b & 0x7f;
        int shift = 7;
        while (true) {
            b = iterator.next();
            result += (b & 0x7f) << shift;
            if ((b & 0x80) != 0) {
                return result;
            }
            shift += 7;
        }
    }

    public static int getCompressedSize(long l) {
        if (l < 128) {
            return 1;
        }
        if (l < 16384) {
            return 2;
        }
        l >>= 21;
        int result = 3;
        while (l > 0) {
            ++result;
            l >>= 7;
        }
        return result;
    }

    @Override
    protected ByteIterator getIterator() {
        return new ByteIterator() {
            private boolean goon = true;
            private long l = CompressedUnsignedLongByteIterable.this.l;

            @Override
            public boolean hasNext() {
                return goon;
            }

            @Override
            public byte next() {
                byte b = (byte) (l & 0x7f);
                l >>= 7;
                if (!(goon = l > 0)) {
                    b |= 0x80;
                }
                return b;
            }

            @Override
            public long skip(final long bytes) {
                for (long i = 0; i < bytes; i++) {
                    if (goon) {
                        l >>= 7;
                        goon = l > 0;
                    } else {
                        return i;
                    }
                }
                return bytes;
            }
        };
    }

    private static int throwBadCompressedNumber() {
        throw new ExodusException("Bad compressed number");
    }
}
