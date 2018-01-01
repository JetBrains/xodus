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
package jetbrains.exodus.entitystore.iterate.cached;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.entitystore.iterate.cached.iterator.OrderedEntityIdCollectionIterator;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseOrderedEntityIdCollectionIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SingleTypeSortedSetEntityIdCachedInstanceIterable extends CachedInstanceIterable {
    private final int typeId;
    @NotNull
    private SortedEntityIdSet localIds;

    public SingleTypeSortedSetEntityIdCachedInstanceIterable(@Nullable PersistentStoreTransaction txn, @NotNull EntityIterableBase source,
                                                             int typeId, @NotNull SortedEntityIdSet localIds) {
        super(txn, source);
        this.typeId = typeId;
        this.localIds = localIds;
    }

    @Override
    public int getEntityTypeId() {
        return typeId;
    }

    @Override
    public boolean isSortedById() {
        return true;
    }

    @Override
    protected CachedInstanceIterable orderById() {
        return this;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return localIds.count();
    }

    @Override
    public boolean contains(@NotNull Entity entity) {
        return localIds.contains(entity.getId());
    }

    @Override
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        return localIds.indexOf(entityId);
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new OrderedEntityIdCollectionIterator(this, localIds);
    }

    @NotNull
    @Override
    public EntityIterator getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new ReverseOrderedEntityIdCollectionIterator(this, localIds);
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull PersistentStoreTransaction txn) {
        return localIds;
    }
}
