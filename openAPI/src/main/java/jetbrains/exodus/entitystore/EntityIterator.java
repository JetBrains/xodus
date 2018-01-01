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

import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * {@code EntityIterator} is an iterator of {@linkplain EntityIterable}. It is an {@code Iterator<Entity>}, but it
 * also allows to enumerate {@linkplain EntityId entity ids} instead of {@linkplain Entity entities} using method
 * {@linkplain #nextId()}. Getting only ids provides better iteration performance.
 *
 * <p>Method {@linkplain #dispose()} releases all resources which the iterator possibly consumes. Method
 * {@linkplain #shouldBeDisposed()} definitely says if it does. Method {@linkplain #dispose()} can be called implicitly
 * in two cases: if iteration finishes and {@linkplain #hasNext()} returns {@code false} and if the transaction
 * finishes or moves to the latest snapshot (any of {@linkplain StoreTransaction#commit()},
 * {@linkplain StoreTransaction#abort()}, {@linkplain StoreTransaction#flush()} or
 * {@linkplain StoreTransaction#revert()} is called).
 *
 * @see EntityIterable
 * @see EntityId
 * @see Entity
 * @see StoreTransaction
 */
public interface EntityIterator extends Iterator<Entity> {

    /**
     * Skips specified number of entities and returns the value of {@linkplain #hasNext()}.
     *
     * @param number number of entities to skip
     * @return {@code true} if there are more entities available
     */
    boolean skip(final int number);

    /**
     * Returns next entity id.
     *
     * @return next entity id
     * @throws java.util.NoSuchElementException if the {@code EntityIterator} has no more elements
     */
    @Nullable
    EntityId nextId();

    /**
     * Disposes the {@code EntityIterator} and frees all resources possibly consumed by it.
     *
     * @return {@code true} if the {@code EntityIterator} was actually disposed
     */
    boolean dispose();

    /**
     * @return {@code true} if method {@linkplain #dispose()} should be called in order to avoid leaks.
     */
    boolean shouldBeDisposed();
}
