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

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;

public final class HexUtil {

    private HexUtil() {
    }

    public static String byteArrayToString(@NotNull final byte[] array) {
        return byteArrayToString(array, 0, array.length);
    }

    public static String byteArrayToString(@NotNull final byte[] array, int off, int len) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < len; ++i) {
            final byte b = array[off + i];
            int digit = (b >> 4) & 0xf;
            builder.append((char) ((digit < 10) ? (digit + '0') : (digit - 10 + 'a')));
            digit = b & 0xf;
            builder.append((char) ((digit < 10) ? (digit + '0') : (digit - 10 + 'a')));
        }
        return builder.toString();
    }

    public static byte[] stringToByteArray(@NotNull final String str) {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int i = 0;
        while (i < str.length()) {
            char c = str.charAt(i);
            int b = Character.digit(c, 16) << 4;
            if (i < str.length() - 1) {
                c = str.charAt(++i);
                b |= Character.digit(c, 16);
            }
            stream.write(b);
            ++i;
        }
        return stream.toByteArray();
    }
}
