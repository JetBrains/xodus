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
        final int strLen = str.length();
        if ((strLen & 1) == 1) {
            throw new IllegalArgumentException("Odd hex string length");
        }
        final byte[] result = new byte[strLen / 2];
        int i = 0;
        int j = 0;
        while (i < strLen) {
            result[j++] = (byte) ((hexChar(str.charAt(i)) << 4) | hexChar(str.charAt(i + 1)));
            i += 2;
        }
        return result;
    }

    private static int hexChar(final char c) {
        final int result = Character.digit(c, 16);
        if (result < 0 || result > 15) {
            throw new IllegalArgumentException("Bad hex digit: " + c);
        }
        return result;
    }
}