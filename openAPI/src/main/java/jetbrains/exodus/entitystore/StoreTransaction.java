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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.List;

/**
 * {@code StoreTransaction} is a transaction started against {@linkplain EntityStore} using
 * {@linkplain EntityStore#beginTransaction()} or {@linkplain EntityStore#beginReadonlyTransaction()} methods.
 * It's being implicitly created in {@linkplain PersistentEntityStore#executeInTransaction(StoreTransactionalExecutable)},
 * {@linkplain PersistentEntityStore#executeInReadonlyTransaction(StoreTransactionalExecutable)},
 * {@linkplain PersistentEntityStore#computeInTransaction(StoreTransactionalComputable)} and
 * {@linkplain PersistentEntityStore#computeInReadonlyTransaction(StoreTransactionalComputable)} methods, and it's
 * being passed as a parameter to {@linkplain StoreTransactionalExecutable} and {@linkplain StoreTransactionalComputable}
 * functions.
 *
 * <p>{@code StoreTransaction} is used to create new {@linkplain Entity entities} and query {@linkplain EntityIterable
 * entity iterables}.
 *
 * @see EntityStore#beginTransaction()
 * @see EntityStore#beginReadonlyTransaction()
 * @see StoreTransactionalExecutable
 * @see StoreTransactionalComputable
 * @see PersistentEntityStore#executeInTransaction(StoreTransactionalExecutable)
 * @see PersistentEntityStore#executeInReadonlyTransaction(StoreTransactionalExecutable)
 * @see PersistentEntityStore#computeInTransaction(StoreTransactionalComputable)
 * @see PersistentEntityStore#computeInReadonlyTransaction(StoreTransactionalComputable)
 * @see Entity
 * @see EntityIterable
 */
public interface StoreTransaction {

    /**
     * Returns the {@linkplain EntityStore} instance which the {@code StoreTransaction} is started against. For
     * out-of-the-box implementations of {@code StoreTransaction}, it always returns the
     * {@linkplain PersistentEntityStore} instance.
     *
     * @return {@linkplain EntityStore} instance
     */
    @NotNull
    EntityStore getStore();

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
     * @return {@code true} if the transaction is read-only
     */
    boolean isReadonly();

    /**
     * @return true if the transaction is finished, committed or aborted
     * @see #commit()
     * @see #abort()
     */
    boolean isFinished();

    /**
     * Tries to commits all changes and finish the {@code StoreTransaction}. If the method succeed, it returns
     * {@code true}. Use {@link #abort()} method to finish transaction ignoring changes.
     *
     * <p> Typical pattern for committing changes can look like this:
     * <pre>
     * final StoreTransaction transaction = entityStore.beginTransaction();
     * while(true) {
     *     try {
     *         // do your changes here
     *     } catch(Throwable t) {
     *         transaction.abort();
     *         throw ExodusException.toRuntime(t);
     *     }
     *     if (transaction.commit()) {
     *         break;
     *     }
     * }
     * </pre>
     *
     * @return {@code true} if the {@code StoreTransaction} is committed
     * @see #flush()
     * @see #abort()
     * @see #revert()
     */
    boolean commit();

    /**
     * Ignores all changes and finishes the {@code StoreTransaction}.
     * <p> Typical pattern for committing changes ({@code abort()} is called in {@code catch} block) can look like this:
     * <pre>
     * final StoreTransaction transaction = entityStore.beginTransaction();
     * while(true) {
     *     try {
     *         // do your changes here
     *     } catch(Throwable t) {
     *         transaction.abort();
     *         throw ExodusException.toRuntime(t);
     *     }
     *     if (transaction.commit()) {
     *         break;
     *     }
     * }
     * </pre>
     *
     * @see #commit()
     * @see #flush()
     * @see #revert()
     */
    void abort();

    /**
     * Tries to flush all changes without finishing the {@code StoreTransaction}. If the method succeed, it returns
     * {@code true}. After successful flush, the {@code StoreTransaction} moves to the latest database snapshot.
     *
     * <p/> Typical pattern for flushing changes can look like this:
     * <pre>
     *     final StoreTransaction transaction = entityStore.beginTransaction();
     *     while(true) {
     *         try {
     *             // do your changes here
     *         } catch(Throwable t) {
     *             transaction.abort();
     *             throw ExodusException.toRuntime(t);
     *         }
     *         if (transaction.flush()) {
     *             break;
     *         }
     *     }
     *     // here continue working with transaction
     * </pre>
     *
     * @see #commit()
     * @see #abort()
     * @see #revert()
     */
    boolean flush();

    /**
     * Reverts all changes without finishing the {@code StoreTransaction}. After revert, the {@code StoreTransaction}
     * moves to the latest database snapshot.
     *
     * @see #commit()
     * @see #abort()
     * @see #flush()
     */
    void revert();

    /**
     * Gets <i>read-only</i> {@code StoreTransaction} that holds snapshot of this {@code StoreTransaction}.
     */
    StoreTransaction getSnapshot();

    /**
     * Creates new {@linkplain Entity} of specified type. Created entity gets its unique {@linkplain EntityId}.
     *
     * @param entityType entity type
     * @return new {@linkplain Entity} instance
     * @see #saveEntity(Entity)
     * @see Entity
     * @see EntityId
     */
    @NotNull
    Entity newEntity(@NotNull final String entityType);

    /**
     * Saves new {@linkplain Entity} which earlier was created by {@linkplain #newEntity(String)} but
     * {@code StoreTransaction} failed to {@linkplain #flush()} or {@linkplain #commit()}.
     * <pre>
     * final Entity user = txn.newEntity("User"|;
     * if (!txn.flush()) {
     *     // if flush didn't succeed you don't need to create one more new entity since <b>user</b> has already got its
     *     // unique id, and it is enough to save it against the new database snapshot
     *     txn.saveEntity(user);
     *     // ...
     * }
     * </pre>
     *
     * @param entity entity earlier returned by {@linkplain #newEntity(String)}
     * @see #newEntity(String)
     * @see Entity
     * @see EntityId
     */
    void saveEntity(@NotNull final Entity entity);

    /**
     * Loads up-to-date version of entity by its {@linkplain EntityId} if it exists in the database.
     * No properties and links are loaded with the entity.
     *
     * @param id unique {@linkplain EntityId}
     * @return entity instance
     * @throws jetbrains.exodus.entitystore.EntityRemovedInDatabaseException if no entity exists with specified unique id
     * @see EntityId
     */
    @NotNull
    Entity getEntity(@NotNull final EntityId id);

    /**
     * Returns all entity types available in the {@linkplain EntityStore}.
     *
     * @return list of entity types
     * @see EntityStore
     */
    @NotNull
    List<String> getEntityTypes();

    /**
     * Gets all entities of specified type.
     *
     * @param entityType entity type
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable getAll(@NotNull final String entityType);

    /**
     * Returns singleton {@linkplain EntityIterable} with the single specified {@linkplain Entity}.
     *
     * @param entity entity
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     * @see Entity
     */
    @NotNull
    EntityIterable getSingletonIterable(@NotNull final Entity entity);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type with specified property equal to specified value.
     *
     * @param entityType   entity type
     * @param propertyName name of the property to search for
     * @param value        value of the property to search for
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable find(@NotNull final String entityType,
                        @NotNull final String propertyName,
                        @NotNull final Comparable value);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type with specified property equal to a value in
     * specified range {@code [minValue, maxValue]}.
     *
     * @param entityType   entity type
     * @param propertyName name of the property to search for
     * @param minValue     minimum value of the property (inclusively, >=).
     * @param maxValue     maximum value of the property (inclusively <=).
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable find(@NotNull final String entityType,
                        @NotNull final String propertyName,
                        @NotNull final Comparable minValue,
                        @NotNull final Comparable maxValue);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which have {@linkplain String} values of
     * specified property starting with specified {@code value}.
     *
     * @param entityType   entity type
     * @param propertyName name of the property to search for
     * @param value        {@linkplain String} value which searched properties are starting with
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findStartingWith(@NotNull final String entityType,
                                    @NotNull final String propertyName,
                                    @NotNull final String value);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type with {@linkplain EntityId ids} having their
     * local ids in specified range [minValue, maxValue].
     *
     * @param entityType entity type
     * @param minValue   minimum value of {@linkplain EntityId#getLocalId()} amongst all returned entities
     * @param maxValue   maximum value of {@linkplain EntityId#getLocalId()} amongst all returned entities
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     * @see EntityId
     */
    @NotNull
    EntityIterable findIds(@NotNull final String entityType, final long minValue, final long maxValue);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which have not-null property values with
     * specified name.
     *
     * @param entityType   entity type
     * @param propertyName property name
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findWithProp(@NotNull final String entityType, @NotNull final String propertyName);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which have not-null blob values with
     * specified name.
     *
     * @param entityType entity type
     * @param blobName   blob name
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findWithBlob(@NotNull final String entityType, @NotNull final String blobName);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which are linked to specified entity by
     * specified link.
     *
     * @param entityType entity type
     * @param entity     entity which resulting entities are linked to
     * @param linkName   link name to search for
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findLinks(@NotNull final String entityType,
                             @NotNull final Entity entity,
                             @NotNull final String linkName);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which are linked to an entity in the
     * specified entity set by specified link.
     *
     * @param entityType entity type
     * @param entities   entity set which entities from resulting {@linkplain EntityIterable} are linked to (each
     *                   resulting entity is linked to at least to one entity from the set)
     * @param linkName   link name to search for
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findLinks(@NotNull final String entityType,
                             @NotNull final EntityIterable entities,
                             @NotNull final String linkName);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type which have links with specified name.
     *
     * @param entityType entity type
     * @param linkName   link name
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findWithLinks(@NotNull final String entityType, @NotNull final String linkName);

    /**
     * Returns the same {@linkplain EntityIterable} as {@linkplain #findWithLinks(String, String)} does, but faster
     * if extra indices are available in the database.
     *
     * <p>This method can be used in the following case. Let {@code t1} be an {@linkplain Entity} of the {@code "T1"}
     * type and {@code t2} - an {@linkplain Entity} of the {@code "T2"} type.
     * If they are mutually linked so that {@code t1.getLink("linkName") == t2} and
     * {@code t2.getLink("oppositeLinkName") == t1}, then it worth to use this method.
     *
     * @param entityType         entity type ({@code "T1"})
     * @param linkName           link name
     * @param oppositeEntityType opposite entity type ({@code "T@"})
     * @param oppositeLinkName   opposite link name
     * @return {@linkplain EntityIterable} instance
     * @see EntityIterable
     */
    @NotNull
    EntityIterable findWithLinks(@NotNull final String entityType,
                                 @NotNull final String linkName,
                                 @NotNull final String oppositeEntityType,
                                 @NotNull final String oppositeLinkName);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type sorted by values of specified property.
     *
     * @param entityType   entity type
     * @param propertyName property name
     * @param ascending    {@code true} is sorting order is ascending
     * @return {@linkplain EntityIterable} instance
     * @see #sort(String, String, EntityIterable, boolean)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable, String, String)
     * @see #mergeSorted(List, Comparator)
     * @see EntityIterable
     * @see EntityIterable#isSortResult()
     * @see EntityIterable#asSortResult()
     */
    @NotNull
    EntityIterable sort(@NotNull final String entityType, @NotNull final String propertyName, final boolean ascending);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type from {@code rightOrder} sorted by
     * values of specified property. Stable sort is used if {@code rightOrder} is
     * {@linkplain EntityIterable#isSortResult() is sort result}.
     *
     * @param entityType   entity type
     * @param propertyName property name
     * @param rightOrder   {@linkplain EntityIterable} to sort by
     * @param ascending    {@code true} is sorting order is ascending
     * @return {@linkplain EntityIterable} instance
     * @see #sort(String, String, boolean)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable, String, String)
     * @see #mergeSorted(List, Comparator)
     * @see EntityIterable
     * @see EntityIterable#isSortResult()
     * @see EntityIterable#asSortResult()
     */
    @NotNull
    EntityIterable sort(@NotNull final String entityType,
                        @NotNull final String propertyName,
                        @NotNull final EntityIterable rightOrder,
                        final boolean ascending);

    /**
     * Returns {@linkplain EntityIterable} with entities of specified type from {@code rightOrder} sorted by links with
     * specified name. Order of links is defined by the {@code sortedLinks} iterable.
     * <pre>
     * // creation of user
     * final Entity user = txn.newEntity("User"); // entityType is "User"
     * user.setProperty("login", ...);
     * user.setProperty("password", ...);
     * user.setProperty("fullName", ...);
     * final Entity emailAccount = txn.newEntity("EmailAccount");
     * emailAccount.setProperty("email", ...);
     * user.addLink("emailAccount", emailAccount); // linkName is "emailAccount"
     * // and so on
     * // ...
     * // given we have some EntityIterable users, sorting users by e-mail looks like this:
     * final EntityIterable linkedEmails = users.selectManyDistinct("emailAccount");
     * // linked email accounts sorted by email:
     * final EntityIterable sortedLinks = txn.sort("EmailAccount", "email", linkedEmails, true);
     * // and finally, given users sorted by email:
     * final EntityIterable result = txn.<b>sortLinks</b>("User",
     *                             sortedLinks,
     *                             true,  // link "emailAccount" is multiple since user can have more than one account
     *                             "emailAccount",
     *                             users);
     * </pre>
     *
     * @param entityType  entity type
     * @param sortedLinks some custom order of links
     * @param isMultiple  {@code true} if the link is multiple
     * @param linkName    link name
     * @param rightOrder  {@linkplain EntityIterable} to sort by
     * @return {@linkplain EntityIterable} instance
     * @see #sort(String, String, boolean)
     * @see #sort(String, String, EntityIterable, boolean)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable, String, String)
     * @see #mergeSorted(List, Comparator)
     * @see EntityIterable
     * @see EntityIterable#isSortResult()
     * @see EntityIterable#asSortResult()
     */
    @NotNull
    EntityIterable sortLinks(@NotNull final String entityType,
                             @NotNull final EntityIterable sortedLinks,
                             final boolean isMultiple,
                             @NotNull final String linkName,
                             @NotNull final EntityIterable rightOrder);

    /**
     * Returns the same {@linkplain EntityIterable} as {@linkplain #sortLinks(String, EntityIterable, boolean, String, EntityIterable)}
     * does, but faster if extra indices are available in the database.
     * <pre>
     * // creation of user
     * final Entity user = txn.newEntity("User"); // entityType is "User"
     * user.setProperty("login", ...);
     * user.setProperty("password", ...);
     * user.setProperty("fullName", ...);
     * final Entity emailAccount = txn.newEntity("EmailAccount"); // oppositeEntityType is "EmailAccount"
     * emailAccount.setProperty("email", ...);
     * user.addLink("emailAccount", emailAccount); // linkName is "emailAccount"
     * emailAccount.setLink("user", user);         // oppositeLinkName is "user"
     * // and so on
     * // ...
     * // given we have some EntityIterable users, sorting users by e-mail looks like this:
     * final EntityIterable linkedEmails = users.selectManyDistinct("emailAccount");
     * // linked email accounts sorted by email:
     * final EntityIterable sortedLinks = txn.sort("EmailAccount", "email", linkedEmails, true);
     * // and finally, given users sorted by email:
     * final EntityIterable result = txn.<b>sortLinks</b>("User",
     *                             sortedLinks,
     *                             true,  // link "emailAccount" is multiple since user can have more than one account
     *                             "emailAccount",
     *                             users,
     *                             "EmailAccount",
     *                             "user");
     * </pre>
     *
     * @param entityType         entity type
     * @param sortedLinks        some custom order of links
     * @param isMultiple         {@code true} if the link is multiple
     * @param linkName           link name
     * @param rightOrder         {@linkplain EntityIterable} to sort by
     * @param oppositeEntityType oppositeEntityType
     * @param oppositeLinkName   oppositeLinkName
     * @return {@linkplain EntityIterable} instance
     * @see #sort(String, String, boolean)
     * @see #sort(String, String, EntityIterable, boolean)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable, String, String)
     * @see #mergeSorted(List, Comparator)
     * @see EntityIterable
     * @see EntityIterable#isSortResult()
     * @see EntityIterable#asSortResult()
     */
    @NotNull
    EntityIterable sortLinks(@NotNull final String entityType,
                             @NotNull final EntityIterable sortedLinks,
                             final boolean isMultiple,
                             @NotNull final String linkName,
                             @NotNull final EntityIterable rightOrder,
                             @NotNull final String oppositeEntityType,
                             @NotNull final String oppositeLinkName);

    /**
     * Returns merged {@linkplain EntityIterable} with entities from several specified sorted iterables using specified
     * comparator. The method is useful to sort entities of different types: at first, the list of sorted
     * {@code EntityIterables} for each entity type should be prepared, then this method merges the list.
     *
     * @param sorted     list of sorted {@code EntityIterables}
     * @param comparator comparator that defines order of entities
     * @return {@linkplain EntityIterable} instance
     * @see #sort(String, String, boolean)
     * @see #sort(String, String, EntityIterable, boolean)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable)
     * @see #sortLinks(String, EntityIterable, boolean, String, EntityIterable, String, String)
     * @see EntityIterable
     * @see EntityIterable#isSortResult()
     * @see EntityIterable#asSortResult()
     */
    @Deprecated
    @NotNull
    EntityIterable mergeSorted(@NotNull final List<EntityIterable> sorted,
                               @NotNull final Comparator<Entity> comparator);

    /**
     * Parses string representation of an entity id and returns corresponding {@linkplain EntityId} instance.
     *
     * @param representation {@linkplain EntityId} string representation
     * @return {@linkplain EntityId} instance
     * @see EntityId
     */
    @NotNull
    EntityId toEntityId(@NotNull final String representation);

    /**
     * Returns existing or creates the new named {@linkplain Sequence}.
     *
     * @param sequenceName name of sequence, unique in the {@linkplain EntityStore}
     * @return {@linkplain Sequence} instance having specified name
     * @see Sequence
     */
    @NotNull
    Sequence getSequence(@NotNull final String sequenceName);

    /**
     * Sets a query cancelling policy for the {@code StoreTransaction}.
     *
     * @param policy query cancelling policy
     * @see #getQueryCancellingPolicy()
     * @see QueryCancellingPolicy
     */
    void setQueryCancellingPolicy(QueryCancellingPolicy policy);

    /**
     * @return query cancelling policy set by {@linkplain #setQueryCancellingPolicy(QueryCancellingPolicy)} or {@code null}
     * @see #setQueryCancellingPolicy(QueryCancellingPolicy)
     * @see QueryCancellingPolicy
     */
    @Nullable
    QueryCancellingPolicy getQueryCancellingPolicy();
}