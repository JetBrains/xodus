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
 * Store is a named collection of key/value pairs. {@code Store} can be opened using any of {@linkplain Environment
 * Environment.openStore()} methods. If a Store is opened using {@linkplain StoreConfig#WITHOUT_DUPLICATES}
 * or {@linkplain StoreConfig#WITHOUT_DUPLICATES_WITH_PREFIXING} then it is map, otherwise it is a multi-map.
 * Also {@code Store} can be thought as a table with two columns, one for keys and another for values.
 * Both keys and values are managed using instances of {@linkplain ByteIterable}. You can use
 * {@linkplain Cursor cursors} to iterate over a {@code Store}, to find nearest key or key/value pair, etc.
 * All operations can only be performed within a {@linkplain Transaction transaction}.
 *
 * <p>Stores with and without key prefixing are implemented by different types of search trees.
 * If {@code Store} is opened using {@linkplain StoreConfig StoreConfig.WITH_DUPLICATES} or
 * {@linkplain StoreConfig StoreConfig.WITHOUT_DUPLICATES} then <a href="https://en.wikipedia.org/wiki/B%2B_tree">B+ tree</a>
 * is used, otherwise <a href="https://en.wikipedia.org/wiki/Radix_tree">Patricia trie</a> is used. Search tree types
 * differ in performance characteristics: stores with key prefixing has better random key access, whereas stores without
 * key prefixing are preferable for sequential access in order of keys.
 *
 * <p>Stores are rather stateless objects, so they can be used without any limitations in multi-threaded environments.
 * Opening {@code Store} for each database operation is ok, but it will result in some performance overhead.
 * A {@linkplain Store} instance cannot be re-used after any of {@linkplain Environment#truncateStore(String, Transaction)},
 * {@linkplain Environment#removeStore(String, Transaction)} or {@linkplain Environment#clear()} methods is called.
 * After truncating, any {@code Store} should be re-opened, after removing or clearing the environment it just cannot be used.
 *
 * @see Environment
 * @see Transaction
 * @see ContextualStore
 * @see Environment#openStore(String, StoreConfig, Transaction)
 * @see Environment#openStore(String, StoreConfig, Transaction, boolean)
 */
public interface Store {

    /**
     * @return {@linkplain Environment environment} which the store was opened for
     */
    @NotNull
    Environment getEnvironment();

    /**
     * For stores without key duplicates, it returns not-null value or null if the key doesn't exist. For stores
     * with key duplicates, it returns the smallest not-null value associated with the key or null if no one exists.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param key requested key
     * @return not-null value if key.value pair with the specified key exists, otherwise null
     */
    @Nullable
    ByteIterable get(@NotNull Transaction txn, @NotNull ByteIterable key);

    /**
     * Checks if specified key/value pair exists in the {@code Store}.
     *
     * @param txn   {@linkplain Transaction transaction} instance
     * @param key   key
     * @param value value
     * @return {@code true} if the key/value pair exists in the {@code Store}
     */
    boolean exists(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * Puts specified key/value pair into the {@code Store} and returns the result. For stores with key duplicates,
     * it returns {@code true} if the pair didn't exist in the {@code Store}. For stores without key duplicates,
     * it returns {@code true} if the key didn't exist or the new value differs from the existing one.
     * <table border=1>
     * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
     * <tr><th>The key exists</th><td>Adds pair, if the value didn't exist</td><td>Overwrites value</td></tr>
     * <tr><th>The key doesn't exist</th><td>Adds pair</td><td>Adds pair</td></tr>
     * </table>
     *
     * @param txn   {@linkplain Transaction transaction} instance
     * @param key   not null key
     * @param value not null value
     * @return {@code true} if specified pair was added or value by the key was overwritten.
     */
    boolean put(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);


    /**
     * Can be used if it is a priori known that the key is definitely greater than any other key in the {@code Store}.
     * In that case, no search is been done before insertion, so {@code putRight()} can perform several times faster
     * than {@link #put(Transaction, ByteIterable, ByteIterable)}. It can be useful for auto-generated keys.
     *
     * @param txn   {@linkplain Transaction transaction} instance
     * @param key   key
     * @param value value
     */
    void putRight(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * Adds key/value pair to the {@code Store} if the key doesn't exist. For stores with and without key duplicates,
     * it returns {@code true} if and only if the key doesn't exists. So it never overwrites value of existing key.
     * <table border=1>
     * <tr><td/><th>With duplicates</th><th>Without duplicates</th></tr>
     * <tr><th>The key exists</th><td>Returns {@code false}</td><td>Returns {@code false}</td></tr>
     * <tr><th>The key doesn't exist</th><td>Adds pair, returns {@code true}</td><td>Adds pair, returns {@code true}</td></tr>
     * </table>
     *
     * @param txn   {@linkplain Transaction transaction} instance
     * @param key   key
     * @param value value
     * @return {@code true} if key/value pair was added
     */
    boolean add(@NotNull Transaction txn, @NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * For stores without key duplicates, deletes single key/value pair and returns {@code true} if a pair was deleted.
     * For stores with key duplicates, it deletes all pairs with the given key and returns {@code true} if any was deleted.
     * To delete particular key/value pair, use {@link Cursor cursors}.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @param key key
     * @return {@code true} if a key/value pair was deleted.
     */
    boolean delete(@NotNull Transaction txn, @NotNull ByteIterable key);

    /**
     * @param txn {@linkplain Transaction transaction} instance
     * @return the number of key/value pairs in the {@code Store}
     */
    long count(@NotNull Transaction txn);

    /**
     * Opens cursor over the @{code Store} associated with a transaction.
     *
     * @param txn {@linkplain Transaction transaction} instance
     * @return {@linkplain Cursor cursor}
     */
    Cursor openCursor(@NotNull Transaction txn);

    /**
     * Deprecated method left only for compatibility with Oracle Berkeley DB JE {@code Database.close()} method.
     */
    @Deprecated
    void close();

    /**
     * @return name of the {@code Store}
     */
    @NotNull
    String getName();

    /**
     * @return {@link StoreConfig} using which the {@code Store} was opened.
     */
    @NotNull
    StoreConfig getConfig();
}
