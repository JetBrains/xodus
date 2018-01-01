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

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

public class ByteIterableUtil {

    private ByteIterableUtil() {
    }

    public static int compare(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
        return compare(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
    }

    public static int compare(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
        return compare(key1, len1, 0, key2, len2, 0);
    }

    public static int compare(@NotNull final byte[] key1, final int len1, final int offset1, @NotNull final byte[] key2, final int len2) {
        return compare(key1, len1, offset1, key2, len2, 0);
    }

    public static int compare(@NotNull final byte[] key1, final int len1, final int offset1,
                              @NotNull final byte[] key2, final int len2, final int offset2) {
        final int min = Math.min(len1, len2);

        for (int i = 0; i < min; i++) {
            final byte b1 = key1[i + offset1];
            final byte b2 = key2[i + offset2];
            if (b1 != b2) {
                return (b1 & 0xff) - (b2 & 0xff);
            }
        }

        return len1 - len2;
    }
}
