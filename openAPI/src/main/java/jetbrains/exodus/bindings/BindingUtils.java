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

    public static float readFloat(@NotNull final ByteArrayInputStream stream) {
        return Float.intBitsToFloat((int) readUnsignedInt(stream));
    }

    public static double readDouble(@NotNull final ByteArrayInputStream stream) {
        return Double.longBitsToDouble(readUnsignedLong(stream));
    }

    public static String readString(@NotNull final ByteArrayInputStream stream) {
        int next = stream.read();
        if (next == UTFUtil.NULL_STRING_UTF_VALUE) {
            next = stream.read();
            if (next == 0) {
                return null;
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            final StringBuilder sb = new StringBuilder();
            int char1, char2, char3;
            while (true) {
                if (next == 0) break;
                char1 = next & 0xff;
                switch ((char1 & 0xff) >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        sb.append((char) char1);
                        break;
                    case 12:
                    case 13:
                        char2 = stream.read();
                        if ((char2 & 0xC0) != 0x80) {
                            throw new IllegalArgumentException();
                        }
                        sb.append((char) (((char1 & 0x1F) << 6) | (char2 & 0x3F)));
                        break;
                    case 14:
                        char2 = stream.read();
                        char3 = stream.read();
                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                            throw new IllegalArgumentException();
                        sb.append((char) (((char1 & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F))));
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
                next = stream.read();
            }
            return sb.toString();
        }
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
