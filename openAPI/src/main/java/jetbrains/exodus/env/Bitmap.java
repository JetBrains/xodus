/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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

public interface Bitmap {

    /**
     * @return {@linkplain Environment environment} which the bitmap was opened for
     */
    @NotNull
    Environment getEnvironment();

    /**
     * Returns {@code true} if the specified bit is set.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param bit bit number
     * @return {@code true} if specified bit is set
     */
    boolean get(@NotNull Transaction txn, long bit);

    /**
     * Sets {@code value} of the specified bit.
     *
     * @param txn   {@linkplain Transaction transaction} instance
     * @param bit   bit number
     * @param value boolean value which will be set
     * @return {@code true} if value was changed
     */
    boolean set(@NotNull Transaction txn, long bit, boolean value);

    /**
     * Sets the specified bit to {@code false}.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param bit bit number
     * @return {@code true} if value was changed
     */
    boolean clear(@NotNull Transaction txn, long bit);

    /**
     * Creates new instance of {@linkplain LongIterator} which iterates bit numbers with the value {@code true}
     * in ascending order.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return new instance of {@linkplain LongIterator}
     * @see LongIterator
     */
    @NotNull
    LongIterator iterator(@NotNull Transaction txn);

    /**
     * Creates new instance of {@linkplain LongIterator} which iterates bit numbers with the value {@code true}
     * in descending order.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return new instance of {@linkplain LongIterator}
     * @see LongIterator
     */
    @NotNull
    LongIterator reverseIterator(@NotNull Transaction txn);

    /**
     * Returns the first (the least) bit with the value {@code true}.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return number of the first set bit
     */
    long getFirst(@NotNull Transaction txn);

    /**
     * Returns the last (the greatest) bit with the value {@code true}.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return number of the last set bit
     */
    long getLast(@NotNull Transaction txn);

    /**
     * Returns total number of bits with the value {@code true}.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return number of set bits
     */
    long count(@NotNull Transaction txn);

    /**
     * Returns  number of bits with the value {@code true} in the range {@code [firstBit, lastBit]}. The range includes
     * both {@code firstBit} and {@code lastBit}.
     *
     * @param txn      {@linkplain Transaction transaction} instance
     * @param firstBit the first (the least) bit of the range
     * @param lastBit  the last (the greatest) bit of the range
     * @return number of set bits in the range
     */
    long count(@NotNull Transaction txn, long firstBit, long lastBit);
}
