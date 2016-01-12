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

public interface Store {
    @NotNull
    Environment getEnvironment();

    @Nullable
    ByteIterable get(@NotNull Transaction txn, @NotNull ByteIterable key);

    boolean exists(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable data);

    /**
     * <p>If tree supports duplicates, then add key/value pair, return true iff pair not existed.</p>
     * <p>If tree doesn't support duplicates and key already exists, then overwrite value, return true iff value differs.</p>
     * <p>If tree doesn't support duplicates and key doesn't exists, then add key/value pair, return true.</p>
     * <table border=1>
     * <tr><td/><th>Dup</th><th>No Dup</th></tr>
     * <tr><th>Exists</th><td>add</td><td>overwrite</td></tr>
     * <tr><th>Not Exists</th><td>add</td><td>add</td></tr>
     * </table>
     *
     * @param txn   a transaction required
     * @param key   not null store key
     * @param value not null store value
     */
    boolean put(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    void putRight(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * <p>If tree support duplicates and key already exists, then return false.</p>
     * <p>If tree support duplicates and key doesn't exists, then add key/value pair, return true.</p>
     * <p>If tree doesn't support duplicates and key already exists, then return false.</p>
     * <p>If tree doesn't support duplicates and key doesn't exists, then add key/value pair, return true.</p>
     * <table border=1>
     * <tr><td/><th>Dup</th><th>No Dup</th></tr>
     * <tr><th>Exists</th><td>ret false</td><td>ret false</td></tr>
     * <tr><th>Not Exists</th><td>add, ret true</td><td>add, ret true</td></tr>
     * </table>
     *
     * @param txn   a transaction required
     * @param key   not null store key
     * @param value not null store value
     * @return true if key/value pair was added
     */
    boolean add(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * Delete key/value pair for given key. If duplicate values exists for given key, all them will be removed.
     *
     * @param txn a transaction required
     * @param key a key to delete pairs with.
     * @return false if key wasn't found
     */
    boolean delete(@NotNull Transaction txn, @NotNull ByteIterable key);

    long count(@NotNull Transaction txn);

    /**
     * Opens cursor over the store associated with a transaction.
     *
     * @param txn a transaction required
     * @return Cursor object
     */
    Cursor openCursor(@NotNull Transaction txn);

    @Deprecated
    void close();

    boolean isNew(@NotNull Transaction txn);

    void persistCreation(@NotNull Transaction txn);

    @NotNull
    String getName();

    @NotNull
    StoreConfig getConfig();
}
