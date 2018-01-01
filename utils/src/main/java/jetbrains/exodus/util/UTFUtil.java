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
package jetbrains.exodus.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;

public class UTFUtil {

    public static final int NULL_STRING_UTF_VALUE = 0xff;

    private static final int SINGLE_UTF_CHUNK_SIZE = 0x10000 / 3;

    private UTFUtil() {
    }

    /**
     * Writes long strings to output stream as several chunks.
     *
     * @param stream stream to write to.
     * @param str    string to be written.
     * @throws IOException if something went wrong
     */
    public static void writeUTF(@NotNull final OutputStream stream, @NotNull final String str) throws IOException {
        try (DataOutputStream dataStream = new DataOutputStream(stream)) {
            int len = str.length();
            if (len < SINGLE_UTF_CHUNK_SIZE) {
                dataStream.writeUTF(str);
            } else {
                int startIndex = 0;
                int endIndex;
                do {
                    endIndex = startIndex + SINGLE_UTF_CHUNK_SIZE;
                    if (endIndex > len) {
                        endIndex = len;
                    }
                    dataStream.writeUTF(str.substring(startIndex, endIndex));
                    startIndex += SINGLE_UTF_CHUNK_SIZE;
                } while (endIndex < len);
            }
        }
    }

    /**
     * Reads a string from input stream saved as a sequence of UTF chunks.
     *
     * @param stream stream to read from.
     * @return output string
     * @throws IOException if something went wrong
     */
    @Nullable
    public static String readUTF(@NotNull final InputStream stream) throws IOException {
        final DataInputStream dataInput = new DataInputStream(stream);
        if (stream instanceof ByteArraySizedInputStream) {
            final ByteArraySizedInputStream sizedStream = (ByteArraySizedInputStream) stream;
            final int streamSize = sizedStream.size();
            if (streamSize >= 2) {
                sizedStream.mark(Integer.MAX_VALUE);
                final int utfLen = dataInput.readUnsignedShort();
                if (utfLen == streamSize - 2) {
                    boolean isAscii = true;
                    final byte[] bytes = sizedStream.toByteArray();
                    for (int i = 0; i < utfLen; ++i) {
                        if ((bytes[i + 2] & 0xff) > 127) {
                            isAscii = false;
                            break;
                        }
                    }
                    if (isAscii) {
                        return fromAsciiByteArray(bytes, 2, utfLen);
                    }
                }
                sizedStream.reset();
            }
        }
        try {
            String result = null;
            StringBuilder builder = null;
            for (; ; ) {
                final String temp;
                try {
                    temp = dataInput.readUTF();
                    if (result != null && result.length() == 0 &&
                            builder != null && builder.length() == 0 && temp.length() == 0) {
                        break;
                    }
                } catch (EOFException e) {
                    break;
                }
                if (result == null) {
                    result = temp;
                } else {
                    if (builder == null) {
                        builder = new StringBuilder();
                        builder.append(result);
                    }
                    builder.append(temp);
                }
            }
            return (builder != null) ? builder.toString() : result;
        } finally {
            dataInput.close();
        }
    }

    public static String fromAsciiByteArray(@NotNull final byte[] bytes, final int off, final int len) {
        return new String(bytes, 0, off, len);
    }

    public static int getUtfByteLength(@NotNull final String value) {
        int len = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                len++;
            } else if (c > 0x07FF) {
                len += 3;
            } else {
                len += 2;
            }

        }
        return len;
    }

    public static void utfCharsToBytes(@NotNull final String value, byte[] bytes, int offset) {
        for (int i = 0; i < value.length(); i++) {
            final int c = value.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytes[offset++] = (byte) c;
            } else if (c > 0x07FF) {
                bytes[offset++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytes[offset++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytes[offset++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytes[offset++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytes[offset++] = (byte) (0x80 | (c & 0x3F));
            }
        }
    }
}
