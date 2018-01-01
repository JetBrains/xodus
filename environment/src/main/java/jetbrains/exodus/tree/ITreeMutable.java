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
package jetbrains.exodus.tree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.ExpiredLoggableInfo;
import jetbrains.exodus.log.RandomAccessLoggable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;

public interface ITreeMutable extends ITree {

    MutableTreeRoot getRoot();

    boolean isAllowingDuplicates();

    @Nullable
    Iterable<ITreeCursorMutable> getOpenCursors();

    void cursorClosed(@NotNull ITreeCursorMutable cursor);

    /**
     * If tree supports duplicates, then add key/value pair.
     * If tree doesn't support duplicates and key already exists, then overwrite value.
     * If tree doesn't support duplicates and key doesn't exists, then add key/value pair.
     *
     * @param key   key.
     * @param value value.
     */
    boolean put(@NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * Add key/value pair with greatest (rightmost) key.
     * In duplicates tree, value must be greatest too.
     *
     * @param key   key.
     * @param value value.
     */
    void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    /**
     * If tree supports duplicates and key already exists, then return false.
     * If tree supports duplicates and key doesn't exists, then add key/value pair, return true.
     * If tree doesn't support duplicates and key already exists, then return false.
     * If tree doesn't support duplicates and key doesn't exists, then add key/value pair, return true.
     *
     * @param key   key.
     * @param value value.
     * @return false if key exists and tree is not supports duplicates
     */
    boolean add(@NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * If tree supports duplicates, then add key/value pair.
     * If tree doesn't support duplicates and key already exists, then overwrite value.
     * If tree doesn't support duplicates and key doesn't exists, then add key/value pair.
     *
     * @param ln leaf node
     */
    void put(@NotNull INode ln);

    /**
     * Add key/value pair with greatest (rightmost) key.
     * In duplicates tree, value must be greatest too.
     *
     * @param ln leaf node, its key and values should be subtypes of ArrayByteIterable
     */
    void putRight(@NotNull INode ln);

    /**
     * If tree support duplicates and key already exists, then return false.
     * If tree support duplicates and key doesn't exists, then add key/value pair, return true.
     * If tree doesn't support duplicates and key already exists, then return false.
     * If tree doesn't support duplicates and key doesn't exists, then add key/value pair, return true.
     *
     * @param ln leaf node
     * @return true if succeed
     */
    boolean add(@NotNull INode ln);

    /**
     * Delete key/value pairs for given key. If duplicate values exist for given key, all they will be removed.
     *
     * @param key key.
     * @return false if key wasn't found
     */
    boolean delete(@NotNull final ByteIterable key);

    /**
     * Delete key/value pair, should be supported only by tree which allows duplicates if value is not null.
     *
     * @param key          key.
     * @param value        value.
     * @param cursorToSkip mutable cursor to skip.
     * @return false if key/value pair wasn't found
     */
    boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value, @Nullable ITreeCursorMutable cursorToSkip);

    /**
     * Save changes to log.
     *
     * @return address of new root page
     */
    long save();

    /**
     * @return set of infos about loggables that were changed by put or delete methods.
     */
    @NotNull
    Collection<ExpiredLoggableInfo> getExpiredLoggables();

    /**
     * Same as reclaim with expirationChecker, but takes all loggables into account
     *
     * @param loggable  a candidate to reclaim.
     * @param loggables loggables following the candidate.
     * @return true if any loggable (the candidate or any among loggables) was reclaimed.
     */
    boolean reclaim(@NotNull RandomAccessLoggable loggable, @NotNull Iterator<RandomAccessLoggable> loggables);
}
