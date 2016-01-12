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
package jetbrains.exodus.entitystore;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface EntityIterable extends Iterable<Entity> {

    @Override
    EntityIterator iterator();

    @NotNull
    StoreTransaction getTransaction();

    boolean isEmpty();

    long size();

    /**
     * Returns size() if it is calculated, -1 otherwise.
     *
     * @return size() if it is calculated, -1 otherwise.
     */
    long count();

    long getRoughCount();

    long getRoughSize();

    /**
     * Returns index of an entity in the iterable, or -1 if there is no such entity.
     *
     * @param entity entity
     * @return index of an entity in the iterable, or -1 if there is no such entity.
     */
    int indexOf(@NotNull final Entity entity);

    boolean contains(@NotNull final Entity entity);

    @NotNull
    EntityIterableHandle getHandle();

    @NotNull
    EntityIterable intersect(@NotNull final EntityIterable right);

    /**
     * Same as intersect, but also guarantees that the order in the result corresponds to the order of the right iterable.
     *
     * @param right iterable which order is preserved in the result.
     * @return intersected iterable.
     */
    @NotNull
    EntityIterable intersectSavingOrder(@NotNull final EntityIterable right);

    @NotNull
    EntityIterable union(@NotNull final EntityIterable right);

    @NotNull
    EntityIterable minus(@NotNull final EntityIterable right);

    @NotNull
    EntityIterable concat(@NotNull final EntityIterable right);

    @NotNull
    EntityIterable skip(final int number);

    @NotNull
    EntityIterable take(final int number);

    /**
     * Returns only distinct entities from this iterable.
     */
    @NotNull
    EntityIterable distinct();

    @NotNull
    EntityIterable selectDistinct(@NotNull final String linkName);

    @NotNull
    EntityIterable selectManyDistinct(@NotNull final String linkName);

    @Nullable
    Entity getFirst();

    @Nullable
    Entity getLast();

    @NotNull
    EntityIterable reverse();


    /**
     * Custom sorting algorithms may mark their results as sort results. This is be used when sorting successively
     * by several fields in order to make sorting stable.
     *
     * @return whether the iterable is a result of sorting.
     */
    boolean isSortResult();

    /**
     * Marks the iterable as sort result.
     *
     * @return entity iterable which recognized as sort result.
     */
    @NotNull
    EntityIterable asSortResult();

    /**
     * For iterables on higher layers (various wrappers, etc.), returns the source type
     * iterable which this one was constructed from. The common case is if PersistentEntityIterableWrapper
     * is asked to return a database iterable in order to effectively calculate a query. Moreover,
     * PersistentEntityIterableWrapper cannot perform intersect, union or minus by itself. A database
     * iterable (which is actually the source one) return itself.
     *
     * @return source EntityIterable
     */
    @NotNull
    EntityIterable getSource();

}
