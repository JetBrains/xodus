/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
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

public class BindingUtils {

    private BindingUtils() {
    }

    public static short readShort(@NotNull final ByteArrayInputStream stream) {
        return (short) (readUnsignedShort(stream) ^ 0x8000);
    }

    public static int readInt(@NotNull final ByteArrayInputStream stream) {
        return (int) (readUnsignedInt(stream) ^ 0x80000000);
    }

    public static long readLong(@NotNull final ByteArrayInputStream stream) {
        return readUnsignedLong(stream) ^ 0x8000000000000000L;
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
        if (!(stream instanceof ByteArraySizedInputStream)) {
            throw new IllegalArgumentException("ByteArraySizedInputStream is expected");
        }
        final ByteArraySizedInputStream sizedStream = (ByteArraySizedInputStream) stream;
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

    public static int readInt(byte[] data, int offset) {
        final int c1 = Byte.toUnsignedInt(data[offset]);
        final int c2 = Byte.toUnsignedInt(data[offset + 1]);
        final int c3 = Byte.toUnsignedInt(data[offset + 2]);
        final int c4 = Byte.toUnsignedInt(data[offset + 3]);

        return ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4);
    }

    public static void writeInt(int value, byte[] data, int offset) {
        final byte c4 = (byte) value;
        final byte c3 = (byte) (value >>> 8);
        final byte c2 = (byte) (value >>> 16);
        final byte c1 = (byte) (value >>> 24);

        data[offset] = c1;
        data[offset + 1] = c2;
        data[offset + 2] = c3;
        data[offset + 3] = c4;
    }

    public static long readLong(byte[] data, int offset) {
        final long c1 = Byte.toUnsignedInt(data[offset]);
        final long c2 = Byte.toUnsignedInt(data[offset + 1]);
        final long c3 = Byte.toUnsignedInt(data[offset + 2]);
        final long c4 = Byte.toUnsignedInt(data[offset + 3]);
        final long c5 = Byte.toUnsignedInt(data[offset + 4]);
        final long c6 = Byte.toUnsignedInt(data[offset + 5]);
        final long c7 = Byte.toUnsignedInt(data[offset + 6]);
        final long c8 = Byte.toUnsignedInt(data[offset + 7]);

        return ((c1 << 56) | (c2 << 48) | (c3 << 40) | (c4 << 32) |
                (c5 << 24) | (c6 << 16) | (c7 << 8) | c8);
    }

    public static void writeLong(long value, byte[] data, int offset) {
        final byte c8 = (byte) value;
        final byte c7 = (byte) (value >>> 8);
        final byte c6 = (byte) (value >>> 16);
        final byte c5 = (byte) (value >>> 24);
        final byte c4 = (byte) (value >>> 32);
        final byte c3 = (byte) (value >>> 40);
        final byte c2 = (byte) (value >>> 48);
        final byte c1 = (byte) (value >>> 56);

        data[offset] = c1;
        data[offset + 1] = c2;
        data[offset + 2] = c3;
        data[offset + 3] = c4;
        data[offset + 4] = c5;
        data[offset + 5] = c6;
        data[offset + 6] = c7;
        data[offset + 7] = c8;
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
