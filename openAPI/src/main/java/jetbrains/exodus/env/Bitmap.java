/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import org.jetbrains.annotations.NotNull;

public interface Bitmap{

    /**
     * @return {@linkplain Environment environment} which the bitmap was opened for.
     */
    @NotNull
    Environment getEnvironment();

    /**
     * Returns boolean value according to the value of bit with handled number.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param bit requested bit number
     * @return boolean value
     */
    boolean get(@NotNull Transaction txn, long bit);

    /**
     * Sets value to bit with handled number.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param bit requested bit number
     * @param value boolean value which will be set
     * @return {@code true} if value was changed and false otherwise
     */
    boolean set(@NotNull Transaction txn, long bit, boolean value);

    /**
     * Specifies the requested bit by {@code false} value.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param bit requested bit number
     * @return {@code true} if value was changed and false otherwise
     */
    boolean clear(@NotNull Transaction txn, long bit);


}
