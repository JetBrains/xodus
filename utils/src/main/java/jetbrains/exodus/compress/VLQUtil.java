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
package jetbrains.exodus.compress;

import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Variable-length quantity universal code (http://en.wikipedia.org/wiki/VLQ).
 */
public class VLQUtil {

    private VLQUtil() {
    }

    public static void writeLong(long l, @NotNull final OutputStream output) throws IOException {
        if (l < 0) {
            throw new IllegalArgumentException(String.valueOf(l));
        }
        final byte[] stack = new byte[10];
        int bytes = 0;
        do {
            final byte b = (byte) (l & 0x7f);
            stack[bytes++] = b;
            l >>= 7;
        } while (l > 0);
        while (--bytes >= 0) {
            int b = stack[bytes] & 0xff;
            if (bytes > 0) {
                b |= 0x80;
            }
            output.write(b);
        }
    }

    public static void writeInt(int i, @NotNull final OutputStream output) throws IOException {
        if (i < 0) {
            throw new IllegalArgumentException(String.valueOf(i));
        }
        final byte[] stack = new byte[5];
        int bytes = 0;
        do {
            final byte b = (byte) (i & 0x7f);
            stack[bytes++] = b;
            i >>= 7;
        } while (i > 0);
        while (--bytes >= 0) {
            int b = stack[bytes] & 0xff;
            if (bytes > 0) {
                b |= 0x80;
            }
            output.write(b);
        }
    }

    public static long readLong(@NotNull final InputStream input) throws IOException {
        long result = 0;
        while (true) {
            final int b = input.read();
            if (b == -1) {
                throw new EOFException();
            }
            result = (result << 7) + (b & 0x7f);
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return result;
    }

    public static int readInt(@NotNull final InputStream input) throws IOException {
        int result = 0;
        while (true) {
            final int b = input.read();
            if (b == -1) {
                throw new EOFException();
            }
            result = (result << 7) + (b & 0x7f);
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return result;
    }

    public static void writeCount(int count, int totalValues, @NotNull final OutputStream output) throws IOException {
        do {
            if (totalValues <= 0x100) {
                output.write(count & 0xff);
                break;
            }
            output.write(count < 0x80 ? count & 0x7f : (count & 0x7f) | 0x80);
            count >>= 7;
            totalValues >>= 7;
        } while (count > 0);
    }

    public static int readCount(int totalValues, @NotNull final InputStream input) throws IOException {
        int result = 0;
        int bites = 0;
        while (true) {
            if (totalValues <= 0x100) {
                return result + ((input.read() & 0xff) << bites);
            }
            final int b = input.read();
            result += ((b & 0x7f) << bites);
            if ((b & 0x80) == 0) {
                return result;
            }
            totalValues >>= 7;
            bites += 7;
        }
    }
}
