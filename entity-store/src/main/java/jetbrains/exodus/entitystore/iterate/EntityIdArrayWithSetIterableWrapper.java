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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class EntityIdArrayWithSetIterableWrapper extends CachedInstanceIterable {
    @NotNull
    private final CachedInstanceIterable source;
    @NotNull
    private final IntArrayList propIds;
    @NotNull
    private final IntHashMap<String> linkNames;

    EntityIdArrayWithSetIterableWrapper(@Nullable PersistentStoreTransaction txn, @NotNull CachedInstanceIterable source,
                                        @NotNull IntArrayList propIds, @NotNull IntHashMap<String> linkNames) {
        super(txn, source);
        this.source = source;
        this.propIds = propIds;
        this.linkNames = linkNames;
    }

    @Override
    protected CachedInstanceIterable orderById() {
        return this;
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.countImpl(txn);
    }

    @Override
    public boolean contains(@NotNull Entity entity) {
        return source.contains(entity);
    }

    @Override
    protected int indexOfImpl(@NotNull EntityId entityId) {
        return source.indexOfImpl(entityId);
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIdArrayWithSetIteratorWrapper(this, (EntityIteratorBase) source.getIteratorImpl(txn), propIds, linkNames);
    }

    @NotNull
    @Override
    public EntityIterator getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return source.getReverseIteratorImpl(txn);
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull PersistentStoreTransaction txn) {
        return source.toSet(txn);
    }
}
