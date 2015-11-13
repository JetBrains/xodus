/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.metadata.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;

public interface StoreTransaction {

    /**
     * Entity store which the transaction is started against.
     *
     * @return entity store object.
     */
    @NotNull
    EntityStore getStore();

    /**
     * Answers on the question: does the transaction actually contain any changes that can be flushed or committed.
     * @return true if there are no changes that will be saved on {@link #flush() flush} or {@link #commit() commit}.
     */
    boolean isIdempotent();

    /**
     * @return true if the transaction is read-only.
     */
    boolean isReadonly();

    /**
     * Commits all changes and finishes this transaction. Use {@link #abort() abort} to finish transaction ignoring changes.
     * <p/> Typical pattern for committing changes can look like the following:
     * <pre>
     *     final StoreTransaction transaction = entityStore.beginTransaction();
     *     while(true) {
     *         try {
     *             // do your changes here
     *         } catch(Throwable t) {
     *             transaction.abort();
     *             throw ExodusException.toRuntime(t);
     *         }
     *         if (transaction.commit()) {
     *             break;
     *         }
     *     }
     * </pre>
     */
    boolean commit();

    /**
     * Ignores all changes and finishes this transaction.
     * <p/> Typical pattern for committing changes can look like the following:
     * <pre>
     *     final StoreTransaction transaction = entityStore.beginTransaction();
     *     while(true) {
     *         try {
     *             // do your changes here
     *         } catch(Throwable t) {
     *             transaction.abort();
     *             throw ExodusException.toRuntime(t);
     *         }
     *         if (transaction.commit()) {
     *             break;
     *         }
     *     }
     * </pre>
     */
    void abort();

    /**
     * Flushes all changes not finishing this transaction. After flush the transaction moves on the latest database snapshot,
     * i.e. there is no repeatable-read before and after flush.
     * <p/> Typical pattern for flushing changes can look like the following:
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
     */
    boolean flush();

    /**
     * Reverts all changes not finishing this transaction. After revert the transaction moves on the latest database snapshot,
     * i.e. there is no repeatable-read before and after revert.
     */
    void revert();

    /**
     * Creates new entity of specified type. Created entity gets its unique id and version equal to 0.
     * No properties are set and links are added to the entity.
     *
     * @param entityType type of the entity.
     * @return new entity instance.
     */
    @NotNull
    Entity newEntity(@NotNull final String entityType);

    void saveEntity(@NotNull final Entity entity);

    /**
     * Loads up-to-date version of entity by its unique id if it exists.
     * No properties and links are loaded with the entity.
     *
     * @param id entity unique id.
     * @return entity instance.
     * @throws jetbrains.exodus.entitystore.EntityRemovedInDatabaseException if no entity exists with specified unique id.
     */
    @NotNull
    Entity getEntity(@NotNull final EntityId id);

    /**
     * Returns all entity types available.
     *
     * @return list of entity types.
     */
    @NotNull
    List<String> getEntityTypes();

    /**
     * Gets all entities of specified type.
     *
     * @param entityType entity type.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable getAll(@NotNull final String entityType);

    @NotNull
    EntityIterable getSingletonIterable(@NotNull final Entity entity);

    /**
     * Finds entities of specified type with a property equal to a value.
     *
     * @param entityType   entity type.
     * @param propertyName name of the property to search for.
     * @param value        value of the property to search for.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable find(@NotNull final String entityType, @NotNull final String propertyName,
                        @NotNull final Comparable value);

    /**
     * Finds entities of specified type with property values in range [minValue, maxValue].
     *
     * @param entityType   entity type.
     * @param propertyName name of the property to search for.
     * @param minValue     minimum acceptable value of the property (inclusively, >=).
     * @param maxValue     maximum value of the property (inclusively <=).
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable find(@NotNull final String entityType, @NotNull final String propertyName,
                        @NotNull final Comparable minValue, @NotNull final Comparable maxValue);

    /**
     * Finds entities of specified type which have property values starting with the value.
     *
     * @param entityType   entity type.
     * @param propertyName name of the property to search for.
     * @param value        string which searched properties are starting with.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findStartingWith(@NotNull final String entityType, @NotNull final String propertyName,
                                    @NotNull final String value);

    @NotNull
    EntityIterable findIds(@NotNull final String entityType, final long minValue, final long maxValue);

    /**
     * Finds entities of specified type which have properties with specified name.
     *
     * @param entityType   type of entities.
     * @param propertyName property name.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findWithProp(@NotNull final String entityType, @NotNull final String propertyName);

    /**
     * Finds entities of specified type which have blobs with specified name.
     *
     * @param entityType   type of entities.
     * @param propertyName property name.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findWithBlob(@NotNull final String entityType, @NotNull final String propertyName);

    /**
     * Finds entities of specified type which are linked to specified entity by specified link.
     *
     * @param entityType entity type.
     * @param entity     entity which resulting entities are linked to.
     * @param linkName   name of the link to search for.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findLinks(@NotNull final String entityType, @NotNull final Entity entity,
                             @NotNull final String linkName);

    /**
     * Finds entities of specified type which are linked to an entity in the specified entity set.
     *
     * @param entityType entity type.
     * @param entities   entity set which resulting entities are linked to (at least to one from the set).
     * @param linkName   name of the link to search for.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findLinks(@NotNull final String entityType, @NotNull final EntityIterable entities,
                             @NotNull final String linkName);

    /**
     * Finds entities of entityType with links named linkName.
     *
     * @param entityType entity type.
     * @param linkName   link name.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findWithLinks(@NotNull final String entityType, @NotNull final String linkName);

    /**
     * Does the same as findWithLinks(entityType, linkName) but faster since more indicies are available in database.
     * This method can be used in the following case. Let t1 be an entity of T1 type and t2 - an entity of T2.
     * If they are mutually linked so that t1.getLink("linkName") == t2 and t2.getLink("oppositeLinkName") == t1,
     * use this method.
     *
     * @param entityType         entity type (T1).
     * @param linkName           link name.
     * @param oppositeEntityType opposite entity type (T2)
     * @param oppositeLinkName   opposite link name.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable findWithLinks(@NotNull final String entityType,
                                 @NotNull final String linkName,
                                 @NotNull final String oppositeEntityType,
                                 @NotNull final String oppositeLinkName);

    /**
     * Returns entities of specified type sorted by specified property.
     *
     * @param entityType   entity type.
     * @param propertyName name of the property to sort entities by.
     * @param ascending    sorting order
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable sort(@NotNull final String entityType, @NotNull final String propertyName, final boolean ascending);

    /**
     * Returns entities of specified type sorted by specified property in respect to right (previous) order.
     *
     * @param entityType   entity type.
     * @param propertyName name of the property to sort entities by.
     * @param rightOrder   iterable which order is kept for same property values (with propertyName name).
     * @param ascending    sorting order
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable sort(@NotNull final String entityType, @NotNull final String propertyName,
                        @NotNull final EntityIterable rightOrder, final boolean ascending);

    @NotNull
    EntityIterable sortLinks(@NotNull final String entityType,
                             @NotNull final EntityIterable sortedLinks,
                             final boolean isMultiple,
                             @NotNull final String linkName,
                             @NotNull final EntityIterable rightOrder);

    @NotNull
    EntityIterable sortLinks(@NotNull final String entityType,
                             @NotNull final EntityIterable sortedLinks,
                             final boolean isMultiple,
                             @NotNull final String linkName,
                             @NotNull final EntityIterable rightOrder,
                             @NotNull final String oppositeEntityType,
                             @NotNull final String oppositeLinkName);

    /**
     * Merge several sorted iterators.
     *
     * @param sorted     list of sorted iterators.
     * @param comparator defines order of elements.
     * @return EntityIterable object.
     */
    @NotNull
    EntityIterable mergeSorted(@NotNull final List<EntityIterable> sorted,
                               @NotNull final Comparator<Entity> comparator);

    /**
     * Parses string representation of an entity id and returns corresponding id instance.
     *
     * @param representation entity id representation.
     * @return entity id instance.
     */
    @NotNull
    EntityId toEntityId(@NotNull final String representation);

    /**
     * Gets existing or creates a new named sequence.
     *
     * @param sequenceName name of sequence, unique in the entity store.
     * @return sequence object.
     */
    @NotNull
    Sequence getSequence(@NotNull final String sequenceName);

    /**
     * Clears history of all entities of specified type.
     *
     * @param entityType type of entities.
     */
    void clearHistory(@NotNull final String entityType);

    /**
     * Sets a query cancelling policy for the transaction.
     *
     * @param policy query cancelling policy.
     */
    void setQueryCancellingPolicy(QueryCancellingPolicy policy);

    /**
     * Gets current query cancelling policy.
     *
     * @return query cancelling policy.
     */
    QueryCancellingPolicy getQueryCancellingPolicy();

    void insertUniqueKey(@NotNull final Index index,
                         @NotNull final List<Comparable> propValues,
                         @NotNull final Entity entity);

    void deleteUniqueKey(@NotNull final Index index,
                         @NotNull final List<Comparable> propValues);

    void enableReplayData();

    void disableReplayData();
}