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
 * {@code EntityIterable} allows to lazily iterate over {@linkplain Entity entities}. It's a result of all
 * {@linkplain StoreTransaction} methods that query, sort and filter entities.
 *
 * <p>{@code EntityIterable} is valid only against particular database snapshot, so finishing transaction
 * ({@linkplain StoreTransaction#commit() commit()} or {@linkplain StoreTransaction#abort() abort}) or moving it to
 * the newest snapshot ({@linkplain StoreTransaction#flush() flush()} or {@linkplain StoreTransaction#revert() revert()})
 * breaks iteration.
 *
 * @see StoreTransaction
 * @see EntityIterator
 * @see Entity
 */
public interface EntityIterable extends Iterable<Entity> {

    /**
     * Creates new instance of {@linkplain EntityIterator}.
     *
     * @return new instance of {@linkplain EntityIterator}
     * @see EntityIterator
     */
    @Override
    EntityIterator iterator();

    /**
     * @return {@linkplain StoreTransaction} instance in which the {@code EntityIterable} was created
     * @see StoreTransaction
     */
    @NotNull
    StoreTransaction getTransaction();

    /**
     * @return {@code true} if the {@code EntityIterable} has no elements
     * @see #size()
     * @see #count()
     * @see #getRoughCount()
     * @see #getRoughSize()
     */
    boolean isEmpty();

    /**
     * Returns the number of entities in the {@code EntityIterable}. This method can be slow.
     *
     * @return the number of entities in the {@code EntityIterable}
     * @see #isEmpty()
     * @see #count()
     * @see #getRoughCount()
     * @see #getRoughSize()
     */
    long size();

    /**
     * Returns the number of entities in the {@code EntityIterable}, or {@code -1} if it cannot be calculated
     * immediately. If it returns {@code -1}, it starts background calculation of {@linkplain #size()}, so deferred
     * call to this method should eventually return a value different from {@code -1} and equal to {@linkplain #size()}.
     *
     * @return the number of entities in the {@code EntityIterable}, or {@code -1} if it cannot be calculated immediately
     * @see #isEmpty()
     * @see #size()
     * @see #getRoughCount()
     * @see #getRoughSize()
     */
    long count();

    /**
     * Returns <i>rough</i> number of entities in the {@code EntityIterable}, or {@code -1} if the size of the
     * {@code EntityIterable} was never calculated. In other words, it returns the same value as {@linkplain #size()}
     * if it can be calculated immediately, or last known size, or {@code -1} in the worst case. This method is
     * <i>eventually consistent</i>, i.e. its result (different form {@code -1}) can differ from result of
     * {@linkplain #size()} and {@linkplain #count()}.
     *
     * @return rough number of entities in the {@code EntityIterable}
     * @see #isEmpty()
     * @see #size()
     * @see #count()
     * @see #getRoughSize()
     */
    long getRoughCount();

    /**
     * Returns the same value as {@linkplain #getRoughCount()}, but in the worst case it returns {@linkplain #size()}
     * instead of {@code -1}. So it never returns {@code -1}, but it is still  <i>eventually consistent</i> as its
     * result can differ from the result of {@linkplain #size()}.
     *
     * @return rough number of entities in the {@code EntityIterable}
     * @see #isEmpty()
     * @see #size()
     * @see #count()
     * @see #getRoughCount()
     */
    long getRoughSize();

    /**
     * Returns index of specified entity in the {@code EntityIterable}, or {@code -1} if there is no such entity.
     * The index can be defined as follows: if {@code int i = indexOf(entity)} and {@code i} is not equal to {@code -1}
     * then the value of {@code skip(i).getFirst()} should be not-null and equal to {@code entity}.
     *
     * @param entity entity to search for
     * @return index of an entity in the {@code EntityIterable}, or {@code -1} if there is no such entity
     * @see #contains(Entity)
     */
    int indexOf(@NotNull final Entity entity);

    /**
     * Returns {@code true} if the {@code EntityIterable} contains specified entity. In short, it just returns
     * value of {@code indexOf(entity) != -1}.
     *
     * @param entity entity whose presence in the {@code EntityIterable} is to be tested
     * @return {@code true} if the {@code EntityIterable} contains specified entity
     * @see #indexOf(Entity)
     */
    boolean contains(@NotNull final Entity entity);

    /**
     * Returns intersection of this {@code EntityIterable} and the right {@code EntityIterable}.
     *
     * @param right {@code EntityIterable} that should be intersected with this {@code EntityIterable}
     * @return intersected {@code EntityIterable}
     * @see #union(EntityIterable)
     * @see #minus(EntityIterable)
     * @see #concat(EntityIterable)
     */
    @NotNull
    EntityIterable intersect(@NotNull final EntityIterable right);

    /**
     * Returns same result as {@linkplain #intersect(EntityIterable)}, but also guarantees that the order in the
     * result corresponds to the order of the right {@code EntityIterable}.
     *
     * @param right {@code EntityIterable} which order is preserved in the result
     * @return intersected {@code EntityIterable}
     * @see #intersect(EntityIterable)
     */
    @NotNull
    EntityIterable intersectSavingOrder(@NotNull final EntityIterable right);

    /**
     * Returns union of this {@code EntityIterable} and the right {@code EntityIterable}.
     *
     * @param right {@code EntityIterable} that should be joined with this {@code EntityIterable}
     * @return joined {@code EntityIterable}
     * @see #intersect(EntityIterable)
     * @see #minus(EntityIterable)
     * @see #concat(EntityIterable)
     */
    @NotNull
    EntityIterable union(@NotNull final EntityIterable right);

    /**
     * Returns relative complement of the right {@code EntityIterable} in this {@code EntityIterable}, i.e.
     * set-theoretic difference of this {@code EntityIterable} and the right {@code EntityIterable}.
     *
     * @param right {@code EntityIterable} which entities will excluded (subtracted) from this {@code EntityIterable}
     * @return relative complement of the right {@code EntityIterable} in this {@code EntityIterable}
     * @see #intersect(EntityIterable)
     * @see #union(EntityIterable)
     * @see #concat(EntityIterable)
     */
    @NotNull
    EntityIterable minus(@NotNull final EntityIterable right);

    /**
     * Returns concatenation of this {@code EntityIterable} and the right {@code EntityIterable}. Unlike other binary
     * operations, result can have duplicate {@linkplain Entity entities}.
     *
     * @param right {@code EntityIterable} that should be concatenated to this {@code EntityIterable}
     * @return concatenated {@code EntityIterable}
     * @see #intersect(EntityIterable)
     * @see #union(EntityIterable)
     * @see #minus(EntityIterable)
     */
    @NotNull
    EntityIterable concat(@NotNull final EntityIterable right);

    /**
     * Returns {@code EntityIterable} that skips specified number of entities from the beginning of this {@code EntityIterable}.
     *
     * @param number entities to skip
     * @return {@code EntityIterable} with the {@code number} of skipped entities
     * @see #take(int)
     */
    @NotNull
    EntityIterable skip(final int number);

    /**
     * Returns {@code EntityIterable} that returns specified number of entities from this {@code EntityIterable}.
     *
     * @param number number of entities
     * @return {@code EntityIterable} with the {@code number} of entities
     * @see #skip(int)
     */
    @NotNull
    EntityIterable take(final int number);

    /**
     * @return only distinct entities from this {@code EntityIterable}
     */
    @NotNull
    EntityIterable distinct();

    /**
     * Returns entities which the source entities (this {@code EntityIterable}) are linked with by the link with
     * specified name. In order words, if {@code target = source.getLink(linkName)} for each {@code source} from
     * this {@code EntityIterable}, then {@code target} is a part of the result. It is implied that the link should be
     * <i>single</i>, i.e. targets should be set using method {@linkplain Entity#setLink(String, Entity)}, so only
     * one {@code target} for each {@code source} can be. Order of linked entities is undefined.
     *
     * @param linkName name of the link
     * @return entities linked by the link
     * @see Entity#getLink(String)
     * @see Entity#setLink(String, Entity)
     */
    @NotNull
    EntityIterable selectDistinct(@NotNull final String linkName);

    /**
     * Returns entities which the source entities (this {@code EntityIterable}) are linked with by the link with
     * specified name. In order words, if {@code targets = source.getLinks(linkName)} for each {@code source} from
     * this {@code EntityIterable}, then each {@code target} amongst {@code targets} is a part of the result.
     * It is implied that the link can me multiple, i.e. targets can be set using any method updating links,
     * {@linkplain Entity#setLink(String, Entity)}or {@linkplain Entity#addLink(String, Entity)}, so there can be
     * arbitrary number of {@code targets} for each {@code source}. Order of linked entities is undefined.
     *
     * @param linkName name of the link
     * @return entities linked by the link
     * @see Entity#getLink(String)
     * @see Entity#addLink(String, Entity)
     */
    @NotNull
    EntityIterable selectManyDistinct(@NotNull final String linkName);

    /**
     * @return the first entity of the {@code EntityIterable}, or {@code null} is it is empty
     */
    @Nullable
    Entity getFirst();

    /**
     * @return the last entity of the {@code EntityIterable}, or {@code null} is it is empty
     */
    @Nullable
    Entity getLast();

    /**
     * @return the same entities as the {@code EntityIterable} returns, but in reverse order
     */
    @NotNull
    EntityIterable reverse();


    /**
     * Custom sorting algorithms can mark their results as sort results. Built-in sort engine recognizes intermediate
     * results as sort results in order to make sorting stable when sorting by several fields, or even using
     * custom sorting algorithms.
     *
     * @return {@code true} if this {@code EntityIterable} is a result of sorting
     * @see #asSortResult()
     * @see StoreTransaction#sort(String, String, EntityIterable, boolean)
     * @see StoreTransaction#sortLinks(String, EntityIterable, boolean, String, EntityIterable)
     * @see StoreTransaction#mergeSorted(List, Comparator)
     */
    boolean isSortResult();

    /**
     * Marks this {@code EntityIterable} as sort result, so built-in sort engine can recognize custom sorting for stable sorting.
     * E.g., in the following sample custom sorting will be lost:
     * <pre>
     *     final EntityIterable customSorting = myCustomSorting();
     *     return txn.sort("User", "loginTime", customSorting, true);
     * </pre>
     * To fix it, just mark custom sorting result as sort result:
     * <pre>
     *     final EntityIterable customSorting = myCustomSorting();
     *     return txn.sort("User", "loginTime", customSorting.asSortResult(), true);
     * </pre>
     *
     * @return {@code EntityIterable} marked as sort result
     * @see #isSortResult()
     * @see StoreTransaction#sort(String, String, EntityIterable, boolean)
     * @see StoreTransaction#sortLinks(String, EntityIterable, boolean, String, EntityIterable)
     * @see StoreTransaction#mergeSorted(List, Comparator)
     */
    @NotNull
    EntityIterable asSortResult();
}
