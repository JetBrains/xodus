/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
import org.jetbrains.annotations.Nullable;

/**
 * Transaction is required for any access to data in database. Any transaction holds a database snapshot (version
 * of the database), thus providing <a href="https://en.wikipedia.org/wiki/Snapshot_isolation">snapshot isolation</a>.
 * <p>All changes made in a transaction are atomic and consistent if they are successfully flushed or committed.
 * Along with snapshot isolation and configurable durability, transaction are fully
 * <a href="https://en.wikipedia.org/wiki/ACID">ACID-compliant</a>.
 * By default, transaction durability is turned off since it significantly slows down {@linkplain #flush()}and
 * {@linkplain #commit()}. To turn it on, pass {@code true} to {@linkplain EnvironmentConfig#setLogDurableWrite(boolean)}.
 * <p>Transactions can be read-only or not, exclusive or not. Read-only transactions are used to only read data.
 * Exclusive transactions are used to have successive access to database. If you have an exclusive transaction,
 * no other transaction (except read-only) can be started against the same {@linkplain Environment} unless you
 * finish (commit or abort) your exclusive one.
 * <p>Given you have an instance of {@linkplain Environment} you can start new transaction:
 * <pre>
 * final Transaction txn = environment.beginTransaction();
 * </pre>
 * Starting read-only transaction:
 * <pre>
 * final Transaction txn = environment.beginReadonlyTransaction();
 * </pre>
 * Starting exclusive transaction:
 * <pre>
 * final Transaction txn = environment.beginExclusiveTransaction();
 * </pre>
 *
 * @see Environment
 * @see Environment#beginTransaction()
 * @see Environment#beginReadonlyTransaction()
 * @see Environment#beginExclusiveTransaction()
 * @see EnvironmentConfig#setLogDurableWrite(boolean)
 */
public interface Transaction {

    /**
     * Idempotent transaction changes nothing in database. It doesn't matter whether you flush it or revert,
     * commit or abort. Result will be the same - nothing will be added, modified or deleted. Flushing idempotent
     * transaction is trivial, {@linkplain #flush()} just does nothing and returns {@code true}. Each newly created
     * transaction is idempotent.
     *
     * @return {@code true} if transaction is idempotent
     * @see #flush()
     * @see #commit()
     */
    boolean isIdempotent();

    /**
     * Drops all changes and finishes the transaction.
     *
     * @see #commit()
     * @see #flush()
     * @see #revert()
     * @see #isFinished()
     */
    void abort();

    /**
     * Tries to flush all changes accumulated to the moment in the transaction and finish the transaction. If it
     * returns {@code true} all changes are flushed and transaction is finished, otherwise nothing is flushed and
     * transaction is not finished.
     *
     * @return {@code true} if transaction is committed
     * @see #flush()
     * @see #abort()
     * @see #revert()
     * @see #isFinished()
     */
    boolean commit();

    /**
     * Tries to flush all changes accumulated to the moment in the transaction. Returns {@code true} if flush succeeded.
     * In that case, transaction remains unfinished and holds the newest database snapshot.
     *
     * @return {@code true} if transaction is flushed
     * @see #commit()
     * @see #abort()
     * @see #revert()
     * @see #isFinished()
     */
    boolean flush();

    /**
     * Drops all changes without finishing the transaction and holds the newest database snapshot.
     *
     * @see #commit()
     * @see #abort()
     * @see #flush()
     * @see #isFinished()
     */
    void revert();

    /**
     * Creates new transaction holding the same database snapshot as this one holds.
     *
     * @return new transaction holding the same database snapshot
     * @see Environment#beginTransaction()
     */
    Transaction getSnapshot();

    /**
     * Creates new transaction with specified begin hook holding the same database snapshot as this one holds.
     *
     * @param beginHook begin hook
     * @return new transaction holding the same database snapshot
     * @see Environment#beginTransaction(Runnable)
     */
    Transaction getSnapshot(@Nullable Runnable beginHook);

    /**
     * Creates new read-only transaction holding the same database snapshot as this one holds.
     *
     * @return new read-only transaction holding the same database snapshot
     * @see Environment#beginReadonlyTransaction()
     */
    Transaction getReadonlySnapshot();

    /**
     * @return {@linkplain Environment} instance against which the transaction was created.
     */
    @NotNull
    Environment getEnvironment();

    /**
     * Provides transaction with commit hook. Commit hook is called if and only if the transaction is going to be
     * successfully flushed or committed. That is, {@code hook.run()} is called when you call {@linkplain #flush()} or
     * {@linkplain #commit()}, and any of these methods will definitely succeed and return {@code true}.
     *
     * @param hook commit hook
     * @see #flush()
     * @see #commit()
     */
    void setCommitHook(@Nullable Runnable hook);

    /**
     * Time when the transaction acquired its database snapshot, i.e. time when it was started,
     * reverted or successfully flushed (committed).
     *
     * @return the difference (in milliseconds) between current time and midnight, January 1, 1970 UTC.
     */
    long getStartTime();

    /**
     * @return the value of Log.getHighAddress() that was actual when the transaction was started.
     */
    long getHighAddress();

    /**
     * @return true if the transaction is read-only.
     */
    boolean isReadonly();

    /**
     * @return true if the transaction was started as exclusive one
     * @see Environment#beginExclusiveTransaction()
     */
    boolean isExclusive();

    /**
     * @return true if the transaction is finished, committed or aborted
     * @see #commit()
     * @see #abort()
     */
    boolean isFinished();

    /**
     * Returns an user object identified by the specified key and bound to the transaction, or {@code null} if no
     * object is bound to the transaction by the specified key.
     *
     * @param key a key identifying the user object
     * @return an user object identified by the specified key and bound to the transaction
     */
    @Nullable
    Object getUserObject(@NotNull final Object key);

    /**
     * Bind an user object ({@code value}) identified by a key to the transaction.
     *
     * @param key   a key identifying the user object
     * @param value user object bound to the transaction
     */
    void setUserObject(@NotNull final Object key, @NotNull final Object value);
}
