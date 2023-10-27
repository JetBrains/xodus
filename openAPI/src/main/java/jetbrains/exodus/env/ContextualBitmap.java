/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.env;

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import org.jetbrains.annotations.NotNull;

public interface ContextualBitmap extends Bitmap {

    /**
     * @return {@linkplain ContextualEnvironment environment} which the bitmap was opened for
     */
    @NotNull
    ContextualEnvironment getEnvironment();

    /**
     * Returns {@code true} if the specified bit is set.
     *
     * @param bit bit number
     * @return {@code true} if specified bit is set
     */
    boolean get(long bit);

    /**
     * Sets {@code value} of the specified bit.
     *
     * @param bit   bit number
     * @param value boolean value which will be set
     * @return {@code true} if value was changed
     */
    boolean set(long bit, boolean value);

    /**
     * Sets the specified bit to {@code false}.
     *
     * @param bit bit number
     * @return {@code true} if value was changed
     */
    boolean clear(long bit);

    /**
     * Creates new instance of {@linkplain LongIterator} which iterates bit numbers with the value {@code true}
     * in ascending order.
     *
     * @return new instance of {@linkplain LongIterator}
     * @see LongIterator
     */
    @NotNull
    LongIterator iterator();

    /**
     * Creates new instance of {@linkplain LongIterator} which iterates bit numbers with the value {@code true}
     * in descending order.
     *
     * @return new instance of {@linkplain LongIterator}
     * @see LongIterator
     */
    @NotNull
    LongIterator reverseIterator();

    /**
     * Returns the first (the least) bit with the value {@code true}.
     *
     * @return number of the first set bit
     */
    Long getFirst();

    /**
     * Returns the last (the greatest) bit with the value {@code true}.
     *
     * @return number of the last set bit
     */
    Long getLast();

    /**
     * Returns total number of bits with the value {@code true}.
     *
     * @return number of set bits
     */
    long count();

    /**
     * Returns  number of bits with the value {@code true} in the range {@code [firstBit, lastBit]}. The range includes
     * both {@code firstBit} and {@code lastBit}.
     *
     * @param firstBit the first (the least) bit of the range
     * @param lastBit  the last (the greatest) bit of the range
     * @return number of set bits in the range
     */
    long count(long firstBit, long lastBit);
}
