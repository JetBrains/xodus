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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.backup.Backupable;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.management.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.List;

/**
 * Environment encapsulates one or more {@linkplain Store stores} that contain data.
 * It allows to perform read/modify operations against multiple {@linkplain Store stores} within a single
 * {@linkplain Transaction transaction}. In short, Environment is a transactional key-value storage.
 * <br><br>Instance of {@code Environment} can be created with the help of the {@code Environments} utility class:
 * <pre>
 *     Environment env = Environments.newInstance("/home/me/.myAppData");
 * </pre>
 * Environment will be created if specified database directory doesn't contain database files. It's impossible to share
 * single database directory amongst different {@linkplain Environment} instances. An attempt to do this (from within
 * any process, current or not) will fail.
 * <br><br>To open Environment with custom settings, use {@linkplain EnvironmentConfig} class. E.g., opening environment
 * with disabled garbage collector looks as follows:
 * <pre>
 *     Environment env = Environments.newInstance("/home/me/.myAppData", new EnvironmentConfig().setGcEnabled(false));
 * </pre>
 * After finishing working with the Environment you should {@linkplain #close()} it.
 */
public interface Environment extends Closeable, Backupable {

    /**
     * Returns the value of {@linkplain System#currentTimeMillis()} when this environment was created. If your
     * application works constantly with a single environment and closes it on exit, the environment's creation
     * time can be used to get the applications' up-time.
     *
     * @return the time when this {@code Environment} instance was created.
     */
    long getCreated();

    /**
     * Returns location of database files on storage device. Can be used as unique key for environment instance.
     *
     * @return location of database files
     */
    @NotNull
    String getLocation();

    /**
     * Opens existing or creates new {@linkplain Store store} with specified {@code name} and
     * {@linkplain StoreConfig config} inside a {@code transaction}. {@linkplain StoreConfig} provides meta-information
     * used to create store. If it is known that the store with specified name exists, then
     * {@linkplain StoreConfig#USE_EXISTING} can be used.
     *
     * @param name        name of store
     * @param config      {@linkplain StoreConfig} used to create store
     * @param transaction {@linkplain Transaction} used to create store
     * @return {@linkplain Store} instance
     */
    @NotNull
    Store openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction);

    /**
     * Opens existing or creates new {@linkplain Store store} with specified {@code name} and
     * {@linkplain StoreConfig config} inside a {@code transaction}. {@linkplain StoreConfig} provides meta-information
     * used to create the store. If it is known that the store with specified name exists, then
     * {@linkplain StoreConfig#USE_EXISTING} can be used.
     * <br><br>Pass {@code true} as {@code creationRequired} if creating new store is required or allowed. In that case,
     * the method will do the same as {@linkplain #openStore(String, StoreConfig, Transaction)}. If you pass
     * {@code false} the method will return {@code null} for non-existing store.
     *
     * @param name             name of store
     * @param config           {@linkplain StoreConfig} used to create store
     * @param transaction      {@linkplain Transaction} used to create store
     * @param creationRequired pass {@code false} if you wish to get {@code null} for non-existing store
     *                         rather than create it
     * @return {@linkplain Store} instance
     */
    @Nullable
    Store openStore(@NotNull String name, @NotNull StoreConfig config, @NotNull Transaction transaction, boolean creationRequired);

    /**
     * Executes a task after all currently started transactions finish.
     *
     * @param task task to execute
     */
    void executeTransactionSafeTask(@NotNull Runnable task);

    /**
     * Clears all the data in the environment. It is safe to clear environment with lots of parallel transactions,
     * though it can't proceed if there is a {@linkplain Transaction} (even read-only) started in current thread.
     *
     * @throws ExodusException if there is a {@linkplain Transaction} in current thread
     * @see Transaction
     */
    void clear();

    /**
     * Closes environment instance. Make sure there are no unfinished transactions, otherwise the method fails with
     * {@linkplain ExodusException}. This behaviour can be changed by calling
     * {@linkplain EnvironmentConfig#setEnvCloseForcedly(boolean)} with {@code true}. After environment is closed
     * another instance of {@code Environment} by the same location can be created.
     *
     * @throws ExodusException            if there are unfinished transactions
     * @throws EnvironmentClosedException if environment is already closed
     */
    void close();

    /**
     * @return {@code false} is the instance is closed
     */
    boolean isOpen();

    /**
     * @param txn {@linkplain Transaction transaction} instance
     * @return the list of names of all {@linkplain Store stores} created in the environment.
     */
    @NotNull
    List<String> getAllStoreNames(@NotNull Transaction txn);

    /**
     * @param storeName name of store
     * @param txn       {@linkplain Transaction transaction} instance
     * @return {@code true} if {@linkplain Store store} with specified name exists
     */
    boolean storeExists(@NotNull String storeName, @NotNull Transaction txn);

    /**
     * Truncates {@linkplain Store store} with specified name. The store becomes empty. All earlier opened
     * {@linkplain Store} instances should be invalidated and re-opened.
     *
     * @param storeName name of store
     * @param txn       {@linkplain Transaction transaction} instance
     */
    void truncateStore(@NotNull String storeName, @NotNull Transaction txn);

    /**
     * Removes {@linkplain Store store} with specified name. All earlier opened
     * {@linkplain Store} instances become unusable.
     *
     * @param storeName name of store
     * @param txn       {@linkplain Transaction transaction} instance
     */
    void removeStore(@NotNull String storeName, @NotNull Transaction txn);

    /**
     * Says environment to quicken background database garbage collector activity. Invocation of this method
     * doesn't have immediate consequences like freeing disk space, deleting particular files, etc.
     */
    void gc();

    /**
     * Suspends database garbage collector activity unless {@linkplain #resumeGC()} is called. Has no effect if it is
     * already suspended. Environment instance can created with disabled GC.
     *
     * @see EnvironmentConfig#setGcEnabled(boolean)
     */
    void suspendGC();

    /**
     * Resumes earlier suspended database garbage collector activity. Has no effect if it si not suspended.
     */
    void resumeGC();

    /**
     * Starts new transaction which can be used to read and write data.
     *
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     */
    @NotNull
    Transaction beginTransaction();

    /**
     * Starts new transaction which can be used to read and write data. Specified {@code beginHook} is called each time
     * when the transaction holds the new database snapshot. First time it is called during {@code beginTransaction()}
     * execution, then during each call to {@linkplain Transaction#flush()} or {@linkplain Transaction#revert()}.
     *
     * @param beginHook begin hook
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     */
    @NotNull
    Transaction beginTransaction(Runnable beginHook);

    /**
     * Starts new exclusive transaction which can be used to read and write data. For given exclusive transaction,
     * it is guaranteed that no other transaction (except read-only ones) can be started on the environment before the
     * given one finishes.
     *
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     * @see Transaction#isExclusive()
     */
    @NotNull
    Transaction beginExclusiveTransaction();

    /**
     * Starts new exclusive transaction which can be used to read and write data. For given exclusive transaction,
     * it is guaranteed that no other transaction (except read-only ones) can be started on the environment before the
     * given one finishes. Specified {@code beginHook} is called each time when the transaction holds the new database
     * snapshot. First time it is called during {@code beginTransaction()} execution, then during each call to
     * {@linkplain Transaction#flush()} or {@linkplain Transaction#revert()}.
     *
     * @param beginHook begin hook
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     */
    @NotNull
    Transaction beginExclusiveTransaction(Runnable beginHook);

    /**
     * Starts new transaction which can be used to only read data.
     *
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     * @see Transaction#isReadonly()
     */
    @NotNull
    Transaction beginReadonlyTransaction();

    /**
     * Starts new transaction which can be used to only read data. Specified {@code beginHook} is called each time
     * when the transaction holds the new database snapshot. First time it is called during {@code beginTransaction()}
     * execution, then during each call to {@linkplain Transaction#revert()}.
     *
     * @param beginHook begin hook
     * @return new {@linkplain Transaction transaction} instance
     * @see Transaction
     * @see Transaction#isReadonly()
     */
    @NotNull
    Transaction beginReadonlyTransaction(Runnable beginHook);

    /**
     * Executes specified executable in a new transaction. If transaction cannot be flushed after
     * {@linkplain TransactionalExecutable#execute(Transaction)} is called, the executable is executed once more until
     * the transaction is finally flushed.
     *
     * @param executable transactional executable
     * @see TransactionalExecutable
     */
    void executeInTransaction(@NotNull TransactionalExecutable executable);

    /**
     * Executes specified executable in a new exclusive transaction.
     * {@linkplain TransactionalExecutable#execute(Transaction)} is called once since the transaction is exclusive,
     * and its flush should always succeed.
     *
     * @param executable transactional executable
     * @see TransactionalExecutable
     */
    void executeInExclusiveTransaction(@NotNull TransactionalExecutable executable);

    /**
     * Executes specified executable in a new read-only transaction.
     * {@linkplain TransactionalExecutable#execute(Transaction)} is called once since the transaction is read-only,
     * and it is never flushed.
     *
     * @param executable transactional executable
     * @see TransactionalExecutable
     * @see Transaction#isReadonly()
     */
    void executeInReadonlyTransaction(@NotNull TransactionalExecutable executable);

    /**
     * Computes and returns a value by calling specified computable in a new transaction. If transaction cannot be
     * flushed after{@linkplain TransactionalComputable#compute(Transaction)} is called, the computable is computed
     * once more until the transaction is finally flushed.
     *
     * @param computable transactional computable
     * @see TransactionalComputable
     */
    <T> T computeInTransaction(@NotNull TransactionalComputable<T> computable);

    /**
     * Computes and returns a value by calling specified computable in a new exclusive transaction.
     * {@linkplain TransactionalComputable#compute(Transaction)} is called once since the transaction is exclusive,
     * and its flush should always succeed.
     *
     * @param computable transactional computable
     * @see TransactionalComputable
     */
    <T> T computeInExclusiveTransaction(@NotNull TransactionalComputable<T> computable);

    /**
     * Computes and returns a value by calling specified computable in a new read-only transaction.
     * {@linkplain TransactionalComputable#compute(Transaction)} is called once since the transaction is read-only,
     * and it is never flushed.
     *
     * @param computable transactional computable
     * @see TransactionalComputable
     * @see Transaction#isReadonly()
     */
    <T> T computeInReadonlyTransaction(@NotNull TransactionalComputable<T> computable);

    /**
     * Returns {@linkplain EnvironmentConfig} instance used during creation of the environment. If no config
     * was specified and no setting was mutated, then returned config has the same settings as
     * {@linkplain EnvironmentConfig#DEFAULT}.
     *
     * @return {@linkplain EnvironmentConfig} instance
     */
    @NotNull
    EnvironmentConfig getEnvironmentConfig();

    /**
     * @return statistics of this {@code Environment} instance
     */
    @NotNull
    Statistics getStatistics();

    /**
     * Returns instance of {@linkplain StreamCipherProvider} if the environment is configured to be encrypted,
     * or {@code null}.
     *
     * @return instance of {@linkplain StreamCipherProvider} if the environment is configured to be encrypted,
     * or {@code null}
     * @see StreamCipherProvider
     * @see EnvironmentConfig#getCipherId()
     */
    @Nullable
    StreamCipherProvider getCipherProvider();

    /**
     * Returns cipher key to use for database encryption, or {@code null} if the environment is not configured
     * to be encrypted.
     *
     * @return cipher key to use for database encryption, or {@code null} if the environment is not configured
     * to be encrypted
     * @see StreamCipher#init(byte[], long)
     * @see EnvironmentConfig#getCipherKey()
     */
    @Nullable
    byte[] getCipherKey();

    /**
     * Returns cipher basic IV (initialization vector) to use for database encryption.
     *
     * @return cipher basic IV (initialization vector) to use for database encryption
     * to be encrypted
     * @see StreamCipher#init(byte[], long)
     * @see EnvironmentConfig#getCipherBasicIV()
     */
    long getCipherBasicIV();
}
