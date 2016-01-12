/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.util;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

@SuppressWarnings({"UseOfSunClasses"})
public class ByteIterableUtil2 {
    public static final Unsafe UNSAFE;
    public static final int THREE_INTS_LENGTH;

    private ByteIterableUtil2() {
    }

    static {
        Object result = null;
        try {
            //noinspection RawUseOfParameterizedType
            final Class unsafeClass = Class.forName("sun.misc.Unsafe");
            final Field field = unsafeClass.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            result = field.get(unsafeClass);
        } catch (final Throwable t) {
            // ignore
        }
        if (result != null) {
            final byte[] b = {0x34, 0x77, 0x01, 0x22};
            final Unsafe candidate = (Unsafe) result;
            //noinspection IfStatementWithTooManyBranches
            if (candidate.getInt(b, 12L) == 0x22017734) {
                UNSAFE = candidate;
                THREE_INTS_LENGTH = 12;
            } else if (candidate.getInt(b, 16L) == 0x22017734) {
                UNSAFE = candidate;
                THREE_INTS_LENGTH = 16;
            } else if (candidate.getInt(b, 20L) == 0x22017734) {
                UNSAFE = candidate;
                THREE_INTS_LENGTH = 20;
            } else {
                UNSAFE = null;
                THREE_INTS_LENGTH = 0;
            }
        } else {
            UNSAFE = null;
            THREE_INTS_LENGTH = 0;
        }
    }

    public static int compare2(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
        return compare2(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
    }

    public static int compare3(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
        return compare3(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
    }

    public static int compare4(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
        return compare4(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
    }

    public static int compare2(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
        final int min = Math.min(len1, len2) + THREE_INTS_LENGTH;

        for (int i = THREE_INTS_LENGTH; i < min; i++) {
            final long address = (long) i;
            final byte b1 = UNSAFE.getByte(key1, address);
            final byte b2 = UNSAFE.getByte(key2, address);
            if (b1 != b2) {
                return (b1 & 0xff) - (b2 & 0xff);
            }
        }

        return (len1 - len2);
    }

    public static int compare3(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
        final int min = Math.min(len1, len2) + THREE_INTS_LENGTH;
        int p = THREE_INTS_LENGTH;
        if (min >= 16) {
            final int aligned = min & 0xFFFFFFFC;
            while (p < aligned) {
                final long address = (long) p;
                final int i1 = UNSAFE.getInt(key1, address);
                final int i2 = UNSAFE.getInt(key2, address);
                if (i1 != i2) {
                    int b1, b2;
                    b1 = i1 & 0xff;
                    b2 = i2 & 0xff;
                    if (b1 != b2) {
                        return b1 - b2;
                    }
                    b1 = i1 & 0xff00;
                    b2 = i2 & 0xff00;
                    if (b1 != b2) {
                        return b1 - b2;
                    }
                    b1 = i1 & 0xff0000;
                    b2 = i2 & 0xff0000;
                    if (b1 != b2) {
                        return b1 - b2;
                    }
                    return i1 - i2;
                }
                p += 4;
            }
        }

        while (p < min) {
            final long address = (long) p;
            final byte b1 = UNSAFE.getByte(key1, address);
            final byte b2 = UNSAFE.getByte(key2, address);
            if (b1 != b2) {
                return (b1 & 0xff) - (b2 & 0xff);
            }
            p++;
        }


        return (len1 - len2);
    }

    public static int compare4(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
        final int min = Math.min(len1, len2) + THREE_INTS_LENGTH;
        int p = THREE_INTS_LENGTH;
        // if (min >= 16) {
        final int aligned = min & 0xFFFFFFFC;
        while (p < aligned) {
            final long address = (long) p;
            final int i1 = UNSAFE.getInt(key1, address);
            final int i2 = UNSAFE.getInt(key2, address);
            if (i1 != i2) {
                int b1, b2;
                b1 = i1 & 0xff;
                b2 = i2 & 0xff;
                if (b1 != b2) {
                    return b1 - b2;
                }
                b1 = i1 & 0xff00;
                b2 = i2 & 0xff00;
                if (b1 != b2) {
                    return b1 - b2;
                }
                b1 = i1 & 0xff0000;
                b2 = i2 & 0xff0000;
                if (b1 != b2) {
                    return b1 - b2;
                }
                return i1 - i2;
            }
            p += 4;
        }
        //}

        final int remaining = min - p;
        if (remaining > 0) {
            final long address = (long) p;
            // final int mask = 0xffffff >>> (1 << remaining);
            final int i1 = UNSAFE.getInt(key1, address);
            final int i2 = UNSAFE.getInt(key2, address);
            if (i1 != i2) {
                int b1, b2;
                b1 = i1 & 0xff;
                b2 = i2 & 0xff;
                if (b1 != b2) {
                    return b1 - b2;
                }
                b1 = i1 & 0xff00;
                b2 = i2 & 0xff00;
                if (b1 != b2) {
                    return b1 - b2;
                }
                return i1 - i2;
            }
        }

        return (len1 - len2);
    }
}
