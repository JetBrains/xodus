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
package jetbrains.exodus.util;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ByteIterableUtil {

    private ByteIterableUtil() {
    }

    public static int compare(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
        return compare(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
    }

    public static int compare(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
        return Arrays.compareUnsigned(key1, 0, len1, key2, 0, len2);
    }

    public static int compare(@NotNull final byte[] key1, final int len1, final int offset1, @NotNull final byte[] key2, final int len2) {
        return Arrays.compareUnsigned(key1, offset1, offset1 + len1, key2, 0, len2);
    }

    public static int compare(@NotNull final byte[] key1, final int len1, final int offset1,
                              @NotNull final byte[] key2, final int len2, final int offset2) {
        return Arrays.compareUnsigned(key1, offset1, offset1 + len1, key2, offset2, offset2 + len2);
    }
}
