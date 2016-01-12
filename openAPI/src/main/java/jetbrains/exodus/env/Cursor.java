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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Newly created cursor has position prior to first record.
 */
public interface Cursor extends AutoCloseable {

    /**
     * Move to next record.
     *
     * @return
     */
    boolean getNext();

    /**
     * Move to next record.
     *
     * @return
     */
    boolean getNextDup();

    boolean getNextNoDup();

    boolean getLast();

    /**
     * Move to previous record.
     *
     * @return
     */
    boolean getPrev();

    /**
     * Move to previous record.
     *
     * @return
     */
    boolean getPrevDup();

    boolean getPrevNoDup();

    @NotNull
    ByteIterable getKey();

    @NotNull
    ByteIterable getValue();

    @Nullable
    ByteIterable getSearchKey(final @NotNull ByteIterable key);

    @Nullable
    ByteIterable getSearchKeyRange(final @NotNull ByteIterable key);

    boolean getSearchBoth(final @NotNull ByteIterable key, final @NotNull ByteIterable value);

    @Nullable
    ByteIterable getSearchBothRange(final @NotNull ByteIterable key, final @NotNull ByteIterable value);

    //TODO: must be the same type as Store.count()

    int count();

    void close();

    boolean deleteCurrent();

}
