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
package jetbrains.exodus;

import jetbrains.exodus.bindings.LongBinding;

/**
 * Iterator of {@link ByteIterable}. Enumerates bytes without boxing.
 */
public abstract class ByteIterator {
    /**
     * @return {@code true} if the iterator has more bytes
     */
    public abstract boolean hasNext();

    /**
     * @return next byte
     */
    public abstract byte next();

    /**
     * Skips {@code bytes} bytes. Result is undefined for negative {@code bytes}.
     *
     * @param bytes bytes to skip.
     * @return number of skipped bytes, zero if no bytes left ({@linkplain #hasNext()} returns {@code false}).
     */
    public abstract long skip(long bytes);

    /**
     * Returns next long value of specified {@code length} in bytes.
     *
     * @param length number of bytes which the long consist of
     * @return next long value of specified {@code length} in bytes
     */
    public long nextLong(final int length) {
        return LongBinding.entryToUnsignedLong(this, length);
    }
}
