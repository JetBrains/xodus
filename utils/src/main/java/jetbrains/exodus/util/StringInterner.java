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

import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import org.jetbrains.annotations.NotNull;

public class StringInterner {

    private static final int NUMBER_OF_GENERATIONS = 2;
    private static final int INTERNER_SIZE = 3089 * NUMBER_OF_GENERATIONS;

    private static final ConcurrentObjectCache<String, String> cache = new ConcurrentObjectCache<>(INTERNER_SIZE, NUMBER_OF_GENERATIONS);

    private StringInterner() {
    }

    @SuppressWarnings({"RedundantStringConstructorCall"})
    public static String intern(final String s) {
        if (s == null) return null;
        final String result = cache.tryKey(s);
        if (result != null) {
            return result;
        }
        final String copy = new String(s);
        cache.cacheObject(copy, copy);
        return copy;
    }

    public static String intern(@NotNull final StringBuilder builder, final int maxLen) {
        final String result = builder.toString();
        if (builder.length() <= maxLen) {
            final String cached = cache.tryKey(result);
            if (cached != null) {
                return cached;
            }
            cache.cacheObject(result, result);
        }
        return result;
    }
}
