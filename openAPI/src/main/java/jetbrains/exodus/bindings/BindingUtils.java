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
package jetbrains.exodus.bindings;

import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.UTFUtil;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public class BindingUtils {
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class,
            ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class,
            ByteOrder.BIG_ENDIAN);
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class,
            ByteOrder.BIG_ENDIAN);

    private BindingUtils() {
    }

    public static short readShort(@NotNull final ByteArrayInputStream stream) {
        return (short) (readUnsignedShort(stream) ^ 0x8000);
    }

    public static short readShort(@NotNull final byte[] data, int offset) {
        return (short) ((short) SHORT_HANDLE.get(data, offset) ^ 0x8000);
    }

    public static int readInt(@NotNull final ByteArrayInputStream stream) {
        return (int) (readUnsignedInt(stream) ^ 0x80000000);
    }

    public static int readInt(@NotNull final byte[] data, int offset) {
        return (int) INT_HANDLE.get(data, offset) ^ 0x80000000;
    }

    public static long readLong(@NotNull final ByteArrayInputStream stream) {
        return readUnsignedLong(stream) ^ 0x8000000000000000L;
    }

    public static long readLong(@NotNull final byte[] data, int offset) {
        return (long) LONG_HANDLE.get(data, offset) ^ 0x8000000000000000L;
    }

    public static float readUnsignedFloat(@NotNull final ByteArrayInputStream stream) {
        return Float.intBitsToFloat((int) readUnsignedInt(stream));
    }

    public static float readSignedFloat(@NotNull final ByteArrayInputStream stream) {
        final int intValue = (int) readUnsignedInt(stream);
        return Float.intBitsToFloat(intValue ^ (intValue < 0 ? 0x80000000 : 0xffffffff));
    }

    public static double readUnsignedDouble(@NotNull final ByteArrayInputStream stream) {
        return Double.longBitsToDouble(readUnsignedLong(stream));
    }

    public static double readSignedDouble(@NotNull final ByteArrayInputStream stream) {
        final long longValue = readUnsignedLong(stream);
        return Double.longBitsToDouble(longValue ^ (longValue < 0 ? 0x8000000000000000L : 0xffffffffffffffffL));
    }

    public static String readString(@NotNull final ByteArrayInputStream stream) {
        int next = stream.read();
        if (next == UTFUtil.NULL_STRING_UTF_VALUE) {
            next = stream.read();
            if (next == 0) {
                return null;
            }
            throw new IllegalArgumentException();
        }
        if (next == 0) {
            return "";
        }

        if (!(stream instanceof final ByteArraySizedInputStream sizedStream)) {
            throw new IllegalArgumentException("ByteArraySizedInputStream is expected");
        }

        final byte[] bytes = sizedStream.toByteArray();
        final char[] chars = new char[sizedStream.size() - 1]; // minus trailing zero
        int i = sizedStream.pos();
        int j = 0;
        do {
            if (next < 128) {
                chars[j++] = (char) next;
            } else {
                final int high = next >> 4;
                if (high == 12 || high == 13) {
                    final int char2 = bytes[i++] & 0xff;
                    if ((char2 & 0xC0) != 0x80) {
                        throw new IllegalArgumentException();
                    }
                    chars[j++] = (char) (((next & 0x1F) << 6) | (char2 & 0x3F));
                } else if (high == 14) {
                    final int char2 = bytes[i] & 0xff;
                    final int char3 = bytes[i + 1] & 0xff;
                    i += 2;
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new IllegalArgumentException();
                    }
                    chars[j++] = (char) (((next & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F)));
                } else {
                    throw new IllegalArgumentException();
                }
            }
            next = bytes[i++] & 0xff;
        } while (next != 0);
        sizedStream.setPos(i);
        return new String(chars, 0, j);
    }

    public static String readString(@NotNull final byte[] data, int offset, int len) {
        int next = Byte.toUnsignedInt(data[offset]);
        offset++;

        if (next == UTFUtil.NULL_STRING_UTF_VALUE) {
            next = data[offset];
            if (next == 0) {
                return null;
            }
            throw new IllegalArgumentException();
        }

        if (next == 0) {
            return "";
        }

        final char[] chars = new char[len - 1]; // minus trailing zero
        int j = 0;
        do {
            if (next < 128) {
                chars[j++] = (char) next;
            } else {
                final int high = next >> 4;
                if (high == 12 || high == 13) {
                    final int char2 = data[offset++] & 0xff;
                    if ((char2 & 0xC0) != 0x80) {
                        throw new IllegalArgumentException();
                    }
                    chars[j++] = (char) (((next & 0x1F) << 6) | (char2 & 0x3F));
                } else if (high == 14) {
                    final int char2 = data[offset] & 0xff;
                    final int char3 = data[offset + 1] & 0xff;
                    offset += 2;
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new IllegalArgumentException();
                    }
                    chars[j++] = (char) (((next & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F)));
                } else {
                    throw new IllegalArgumentException();
                }
            }
            next = data[offset++] & 0xff;
        } while (next != 0);

        return new String(chars, 0, j);
    }

    private static int readUnsignedShort(@NotNull final ByteArrayInputStream stream) {
        final int c1 = stream.read();
        final int c2 = stream.read();
        if ((c1 | c2) < 0) {
            throw new IndexOutOfBoundsException();
        }
        return ((c1 << 8) | c2);
    }

    private static long readUnsignedInt(@NotNull final ByteArrayInputStream stream) {
        final long c1 = stream.read();
        final long c2 = stream.read();
        final long c3 = stream.read();
        final long c4 = stream.read();
        if ((c1 | c2 | c3 | c4) < 0) {
            throw new IndexOutOfBoundsException();
        }
        return ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4);
    }


    private static long readUnsignedLong(@NotNull final ByteArrayInputStream stream) {
        final long c1 = stream.read();
        final long c2 = stream.read();
        final long c3 = stream.read();
        final long c4 = stream.read();
        final long c5 = stream.read();
        final long c6 = stream.read();
        final long c7 = stream.read();
        final long c8 = stream.read();
        if ((c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8) < 0) {
            throw new IndexOutOfBoundsException();
        }
        return ((c1 << 56) | (c2 << 48) | (c3 << 40) | (c4 << 32) |
                (c5 << 24) | (c6 << 16) | (c7 << 8) | c8);
    }
}
