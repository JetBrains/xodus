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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code ContextualStore} is a {@link Store} created by {@link ContextualEnvironment}.
 * Just like {@link ContextualEnvironment}, it is aware of {@linkplain Transaction transaction}
 * started in current thread. {@code ContextualStore} overloads all {@link Store}'s methods with the ones that don't
 * accept {@linkplain Transaction transaction} instance.
 *
 * @see ContextualEnvironment
 * @see Transaction
 * @see Store
 */
public interface ContextualStore extends Store {

    /**
     * @return {@linkplain ContextualEnvironment environment} which the store was opened for
     */
    @NotNull
    ContextualEnvironment getEnvironment();

    /**
     * For stores without key duplicates, it returns not-null value or null if the key doesn't exist. For stores
     * with key duplicates, it returns the smallest not-null value associated with the key or null if no one exists.
     *
     * @param key requested key
     * @return not-null value if key.value pair with the specified key exists, otherwise null
     */
    @Nullable
    ByteIterable get(@NotNull final ByteIterable key);

    /**
     * Checks if specified key/value pair exists in the {@code ContextualStore}.
     *
     * @param key   key
     * @param value value
     * @return {@code true} if the key/value pair exists in the {@code ContextualStore}
     */
    boolean exists(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    /**
     * Puts specified key/value pair into the {@code ContextualStore} and returns the result. For stores with key duplicates,
     * it returns {@code true} if the pair didn't exist in the {@code ContextualStore}. For stores without key duplicates,
     * it returns {@code true} if the key didn't exist or the new value differs from the existing one.
     * <table border=1>
     * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
     * <tr><th>The key exists</th><td>Adds pair, if the value didn't exist</td><td>Overwrites value</td></tr>
     * <tr><th>The key doesn't exist</th><td>Adds pair</td><td>Adds pair</td></tr>
     * </table>
     *
     * @param key   not null key
     * @param value not null value
     * @return {@code true} if specified pair was added or value by the key was overwritten.
     */
    boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    /**
     * Can be used if it is a priori known that the key is definitely greater than any other key in the {@code ContextualStore}.
     * In that case, no search is been done before insertion, so {@code putRight()} can perform several times faster
     * than {@link #put(ByteIterable, ByteIterable)}. It can be useful for auto-generated keys.
     *
     * @param key   key
     * @param value value
     */
    void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    /**
     * Adds key/value pair to the {@code ContextualStore} if the key doesn't exist. For stores with and without key duplicates,
     * it returns {@code true} if and only if the key doesn't exists. So it never overwrites value of existing key.
     * <table border=1>
     * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
     * <tr><th>The key exists</th><td>Returns {@code false}</td><td>Returns {@code false}</td></tr>
     * <tr><th>The key doesn't exist</th><td>Adds pair, returns {@code true}</td><td>Adds pair, returns {@code true}</td></tr>
     * </table>
     *
     * @param key   key
     * @param value value
     * @return {@code true} if key/value pair was added
     */
    boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value);

    /**
     * For stores without key duplicates, deletes single key/value pair and returns {@code true} if a pair was deleted.
     * For stores with key duplicates, it deletes all pairs with the given key and returns {@code true} if any was deleted.
     * To delete particular key/value pair, use {@link Cursor cursors}.
     *
     * @param key key
     * @return {@code true} if a key/value pair was deleted.
     */
    boolean delete(@NotNull final ByteIterable key);

    /**
     * @return the number of key/value pairs in the {@code ContextualStore}
     */
    long count();

    /**
     * Opens cursor over the @{code Store}. Returned instance can only be used in current thread.
     *
     * @return {@linkplain Cursor cursor}
     */
    Cursor openCursor();

}
