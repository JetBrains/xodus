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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.backup.Backupable;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.management.Statistics;
import org.jetbrains.annotations.NotNull;

/**
 * {@code PersistentEntityStore} is an {@linkplain EntityStore} operating above {@linkplain Environment} instance.
 * {@code PersistentEntityStore} uses {@linkplain BlobVault} to store blobs that have size greater than
 * {@linkplain PersistentEntityStoreConfig#getMaxInPlaceBlobSize()} value which is by default equal to {@code 10000}.
 * Default {@linkplain BlobVault} stores blobs as separate files in the {@code blobs} subdirectory of
 * {@code PersistentEntityStore}'s location ({@linkplain EntityStore#getLocation()}). {@code PersistentEntityStore}'s
 * location is equal to underlying {@linkplain Environment}'s location. So on storage device, the database for
 * {@code PersistentEntityStore} consists of underlying {@linkplain Environment}'s database files plus
 * the {@code blobs} subdirectory.
 *
 * @see EntityStore
 * @see Environment
 * @see BlobVault
 */
public interface PersistentEntityStore extends EntityStore, Backupable {

    /**
     * @return underlying {@linkplain Environment} instance
     */
    @NotNull
    Environment getEnvironment();

    /**
     * Clears all the data in the {@code PersistentEntityStore}. It is safe to clear {@code PersistentEntityStore}
     * with lots of parallel transactions. Make sure all {@linkplain java.io.InputStream} instances got from
     * the {@linkplain #getBlobVault() blob vault} are closed.
     */
    void clear();

    /**
     * Executes specified executable in a new {@linkplain StoreTransaction transaction}. If transaction cannot be
     * flushed after {@linkplain StoreTransactionalExecutable#execute(StoreTransaction)} is called, the executable
     * is executed once more until the transaction is finally flushed.
     *
     * @param executable transactional executable
     * @see StoreTransactionalExecutable
     */
    void executeInTransaction(@NotNull StoreTransactionalExecutable executable);

    /**
     * Executes specified executable in a new exclusive {@linkplain StoreTransaction transaction}. Exclusive
     * transaction guarantees that no one other transaction can even start before this one is finished, so
     * the executable is executed once.
     *
     * @param executable transactional executable
     * @see StoreTransactionalExecutable
     */
    void executeInExclusiveTransaction(@NotNull StoreTransactionalExecutable executable);

    /**
     * Executes specified executable in a new read-only {@linkplain StoreTransaction transaction}.
     * {@linkplain StoreTransactionalExecutable#execute(StoreTransaction)} is called once since the transaction is
     * read-only, and it is never flushed.
     *
     * @param executable transactional executable
     * @see StoreTransactionalExecutable
     * @see StoreTransaction#isReadonly()
     */
    void executeInReadonlyTransaction(@NotNull StoreTransactionalExecutable executable);

    /**
     * Computes and returns a value by calling specified computable in a new {@linkplain StoreTransaction transaction}.
     * If transaction cannot be flushed after {@linkplain StoreTransactionalComputable#compute(StoreTransaction)} is
     * called, the computable is computed once more until the transaction is finally flushed.
     *
     * @param computable transactional computable
     * @see StoreTransactionalComputable
     */
    <T> T computeInTransaction(@NotNull StoreTransactionalComputable<T> computable);

    /**
     * Computes and returns a value by calling specified computable in a new exclusive
     * {@linkplain StoreTransaction transaction}. Exclusive transaction guarantees that no one other transaction can
     * even start before this one is finished, so the computable is computed once.
     *
     * @param computable transactional computable
     * @see StoreTransactionalComputable
     */
    <T> T computeInExclusiveTransaction(@NotNull StoreTransactionalComputable<T> computable);

    /**
     * Computes and returns a value by calling specified computable in a new read-only transaction.
     * {@linkplain StoreTransactionalComputable#compute(StoreTransaction)} is called once since the transaction is
     * read-only, and it is never flushed.
     *
     * @param computable transactional computable
     * @see StoreTransactionalComputable
     * @see StoreTransaction#isReadonly()
     */
    <T> T computeInReadonlyTransaction(@NotNull StoreTransactionalComputable<T> computable);

    /**
     * @return {@linkplain BlobVault} which is used for managing blobs
     */
    @NotNull
    BlobVault getBlobVault();

    /**
     * Registers custom property type extending {@linkplain Comparable}. Values of specified {@code clazz} can be
     * passed then to {@linkplain Entity#setProperty(String, Comparable)}. {@code ComparableBinding} describes the
     * way property values are serialized/deserialized to/from raw presentation as
     * {@linkplain jetbrains.exodus.ByteIterable} instances.
     *
     * @param txn     {@linkplain StoreTransaction} instance
     * @param clazz   class of property values extending {@linkplain Comparable}
     * @param binding {@code ComparableBinding}
     * @see Entity#setProperty(String, Comparable)
     * @see ComparableBinding
     * @see jetbrains.exodus.ByteIterable
     */
    void registerCustomPropertyType(@NotNull final StoreTransaction txn,
                                    @NotNull final Class<? extends Comparable> clazz,
                                    @NotNull final ComparableBinding binding);

    /**
     * Returns {@linkplain Entity} by specified {@linkplain EntityId}.
     *
     * @param id entity id
     * @return {@linkplain Entity} instance
     * @throws EntityRemovedInDatabaseException entity by specified if doesn't exist in the database
     * @see Entity#getId()
     */
    Entity getEntity(@NotNull EntityId id);

    /**
     * Returns integer id of specified entity type.
     *
     * @param entityType entity type
     * @return integer id of entity type
     * @see EntityId#getTypeId()
     */
    int getEntityTypeId(@NotNull final String entityType);

    /**
     * Returns entity type of specified entity type id.
     *
     * @param entityTypeId entity type id
     * @return entity type of specified entity type id
     * @see EntityId#getTypeId()
     */
    @NotNull
    String getEntityType(int entityTypeId);

    /**
     * Renames entity type. {@linkplain StoreTransaction} should started in current thread.
     *
     * @param oldEntityTypeName old entity type name
     * @param newEntityTypeName new entity type name
     */
    void renameEntityType(@NotNull String oldEntityTypeName, @NotNull String newEntityTypeName);

    /**
     * @return The number of available bytes on the partition where the database is located
     */
    long getUsableSpace();

    /**
     * Returns {@linkplain PersistentEntityStoreConfig} instance used during creation of the
     * {@code PersistentEntityStore}. If no config was specified and no setting was mutated, then returned config has
     * the same settings as {@linkplain PersistentEntityStoreConfig#DEFAULT}.
     *
     * @return {@linkplain PersistentEntityStoreConfig} instance
     */
    @NotNull
    PersistentEntityStoreConfig getConfig();

    /**
     * {@linkplain MultiThreadDelegatingJobProcessor Job processor} used by the {@code PersistentEntityStore} for
     * background caching activities. Allows to indirectly estimate load of the {@code PersistentEntityStore}. E.g.,
     * if it has numerous {@linkplain JobProcessor#pendingJobs() pending caching jobs} (say, thousands) then most
     * likely caching doesn't work well and the {@code PersistentEntityStore} looks overloaded.
     *
     * @return job processor used for background caching activities
     */
    @NotNull
    MultiThreadDelegatingJobProcessor getAsyncProcessor();

    /**
     * @return statistics of this {@code PersistentEntityStore} instance
     * @see PersistentEntityStoreConfig#GATHER_STATISTICS
     */
    @NotNull
    Statistics getStatistics();
}
