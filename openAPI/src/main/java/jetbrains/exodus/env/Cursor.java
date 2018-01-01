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

import java.io.Closeable;

/**
 * {@code Cursor} allows to access key/value pairs of a {@linkplain Store} in both successive (ascending and descending)
 * and random order. {@code Cursor} can be opened for a {@linkplain Store} in a {@linkplain Transaction}. Both key
 * ({@linkplain #getKey()}) and value ({@linkplain #getValue()}) are accessible as {@linkplain ByteIterable}
 * instances. Finally, any cursor should always be closed.
 *
 * <p>Each newly created cursor points to a "virtual" key/value pair which is prior to the first (leftmost) pair. Use
 * {@linkplain #getNext()} to move to the first and {@linkplain #getPrev()} to last (rightmost) key/value pair. You
 * can move {@code Cursor} to the last (rightmost) position from any other position using {@linkplain #getLast()}.
 *
 * @see Store
 * @see Store#openCursor(Transaction)
 */
public interface Cursor extends Closeable {

    /**
     * Moves the {@code Cursor} to the next key/value pair. If the {@code Cursor} is just created the method moves
     * it to the first (leftmost) key/value pair.
     *
     * <p>E.g., traversing all key/value pairs of a {@linkplain Store} in ascending order looks as follows:
     * <pre>
     * try (Cursor cursor = store.openCursor(txn)) {
     *     while (cursor.getNext()) {
     *         cursor.getKey();   // current key
     *         cursor.getValue(); // current value
     *     }
     * }
     * </pre>
     *
     * @return {@code true} if next pair exists
     * @see #getNextDup()
     * @see #getNextNoDup()
     * @see #getPrev()
     * @see #getPrevDup()
     * @see #getPrevNoDup()
     */
    boolean getNext();

    /**
     * Moves the {@code Cursor} to the next key/value pair with the same key.
     *
     * @return {@code true} if next pair with the same key exists
     * @see #getNext()
     * @see #getNextNoDup()
     * @see #getPrev()
     * @see #getPrevDup()
     * @see #getPrevNoDup()
     */
    boolean getNextDup();

    /**
     * Moves the {@code Cursor} to the next key/value pair with the nearest key different from current one.
     *
     * @return {@code true} if next pair exists
     * @see #getNext()
     * @see #getNextDup()
     * @see #getPrev()
     * @see #getPrevDup()
     * @see #getPrevNoDup()
     */
    boolean getNextNoDup();

    /**
     * Moves the {@code Cursor} to the last (rightmost) key/value pair if any exists. Returns {@code true} if the
     * {@linkplain Store} has at least one key/value pair.
     *
     * @return {@code true} if the {@code Cursor} points to the last key/value pair
     * @see #getNext()
     * @see #getNextDup()
     * @see #getNextNoDup()
     * @see #getPrev()
     * @see #getPrevDup()
     * @see #getPrevNoDup()
     */
    boolean getLast();

    /**
     * Moves the {@code Cursor} to the previous key/value pair. If the {@code Cursor} is just created the method moves
     * it to the last (rightmost) key/value pair.
     *
     * <p>E.g., traversing all key/value pairs of a {@linkplain Store} in descending order looks as follows:
     * <pre>
     * try (Cursor cursor = store.openCursor(txn)) {
     *     while (cursor.getPrev()) {
     *         cursor.getKey();   // current key
     *         cursor.getValue(); // current value
     *     }
     * }
     * </pre>
     *
     * @return {@code true} if previous pair exists
     * @see #getNext()
     * @see #getNextDup()
     * @see #getNextNoDup()
     * @see #getPrevDup()
     * @see #getPrevNoDup()
     */
    boolean getPrev();

    /**
     * Moves the {@code Cursor} to the previous key/value pair with the same key.
     *
     * @return {@code true} if previous pair with the same key exists
     * @see #getNext()
     * @see #getNextDup()
     * @see #getNextNoDup()
     * @see #getPrev()
     * @see #getPrevNoDup()
     */
    boolean getPrevDup();

    /**
     * Moves the {@code Cursor} to the previous key/value pair with the nearest key different from current one.
     *
     * @return {@code true} if previous pair exists
     * @see #getNext()
     * @see #getNextDup()
     * @see #getNextNoDup()
     * @see #getPrev()
     * @see #getPrevDup()
     */
    boolean getPrevNoDup();

    /**
     * @return current key
     * @see ByteIterable
     * @see #getValue()
     */
    @NotNull
    ByteIterable getKey();

    /**
     * @return current value
     * @see ByteIterable
     * @see #getKey()
     */
    @NotNull
    ByteIterable getValue();

    /**
     * Moves the {@code Cursor} to the specified key, and returns the value associated with the key. In
     * {@linkplain Store stores} with key duplicates, if the matching key has duplicate values, the {@code Cursor} moves
     * to the first (leftmost) key/value pair.
     *
     * <p>If the method fails for any reason, it returns {@code null}, and the position of the {@code Cursor} will be unchanged.
     *
     * @param key the key to search for
     * @return first (leftmost) value associated with the key or {@code null} if no such value exists
     * @see #getSearchKeyRange(ByteIterable)
     * @see #getSearchBoth(ByteIterable, ByteIterable)
     * @see #getSearchBothRange(ByteIterable, ByteIterable)
     */
    @Nullable
    ByteIterable getSearchKey(final @NotNull ByteIterable key);

    /**
     * Moves the {@code Cursor} to the first pair in the {@linkplain Store} whose key is equal to or greater than the
     * specified key. It returns not-null value if it succeeds or {@code null} if nothing is found, and the position of
     * the {@code Cursor} will be unchanged.
     *
     * @param key the key to search for
     * @return not-null value if it succeeds or {@code null} if nothing is found
     * @see #getSearchKey(ByteIterable)
     * @see #getSearchBoth(ByteIterable, ByteIterable)
     * @see #getSearchBothRange(ByteIterable, ByteIterable)
     */
    @Nullable
    ByteIterable getSearchKeyRange(final @NotNull ByteIterable key);

    /**
     * Moves the {@code Cursor} to the key/value pair with the specified key and value and returns {@code true} if
     * the pair exists in the {@linkplain Store}. Otherwise the position of the {@code Cursor} will be unchanged.
     *
     * @param key   the key to search for
     * @param value the value to search for
     * @return {@code true} if the pair with the specified key and value exists in the {@linkplain Store}
     * @see #getSearchKey(ByteIterable)
     * @see #getSearchKeyRange(ByteIterable)
     * @see #getSearchBothRange(ByteIterable, ByteIterable)
     */
    boolean getSearchBoth(final @NotNull ByteIterable key, final @NotNull ByteIterable value);

    /**
     * Moves the {@code Cursor} to the first pair in the {@linkplain Store} whose key matches the specified key and
     * whose value is equal to or greater than the specified value. If the {@code Store} supports key duplicates, then
     * on matching the key, the {@code Cursor} is moved to the duplicate pair with the smallest value that is equal to
     * or greater than the specified value. Like the {@linkplain #getSearchKeyRange(ByteIterable)} method, it returns
     * not-null value if it succeeds or {@code null} if nothing is found, and the position of the {@code Cursor} will
     * be unchanged.
     *
     * @param key   the key to search for
     * @param value the value to search for
     * @return not-null value if it succeeds or {@code null} if nothing is found
     * @see #getSearchKey(ByteIterable)
     * @see #getSearchKeyRange(ByteIterable)
     * @see #getSearchBoth(ByteIterable, ByteIterable)
     */
    @Nullable
    ByteIterable getSearchBothRange(final @NotNull ByteIterable key, final @NotNull ByteIterable value);

    /**
     * @return the number of values in the {@linkplain Store} associated with current key
     */
    int count();

    /**
     * Cursor is mutable if it is created in a transaction which has dirty (not flushed/committed) modifications.
     * Mutable cursors automatically pick up changes made using {@linkplain Store#add(Transaction, ByteIterable, ByteIterable)},
     * {@linkplain Store#put(Transaction, ByteIterable, ByteIterable)} and {@linkplain Store#delete(Transaction, ByteIterable)}
     * methods.
     *
     * @return {@code true} if cursor is mutable
     */
    boolean isMutable();

    /**
     * This method should always be called as immediately as possible to avoid unnecessary performance degradation.
     */
    void close();

    /**
     * Deletes current key/value pair in the {@linkplain Store} and returns {@code true} if deletion succeeded.
     *
     * @return {@code true} if current key/value pair was deleted
     */
    boolean deleteCurrent();

}
