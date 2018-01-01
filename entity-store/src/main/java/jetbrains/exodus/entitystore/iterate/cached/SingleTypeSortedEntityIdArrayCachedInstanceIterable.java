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

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable;
import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase;
import jetbrains.exodus.entitystore.iterate.cached.iterator.EntityIdArrayIteratorNullTypeId;
import jetbrains.exodus.entitystore.iterate.cached.iterator.OrderedEntityIdCollectionIterator;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseEntityIdArrayIteratorNullTypeId;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseOrderedEntityIdCollectionIterator;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class SingleTypeSortedEntityIdArrayCachedInstanceIterable extends CachedInstanceIterable {
    private final int typeId;
    private final ImmutableSingleTypeEntityIdCollection localIds;
    @Nullable
    private EntityIdSet idSet;

    public SingleTypeSortedEntityIdArrayCachedInstanceIterable(@NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source,
                                                               int typeId, long[] localIds, @Nullable EntityIdSet idSet) {
        super(txn, source);
        this.typeId = typeId;
        this.localIds = new ImmutableSingleTypeEntityIdCollection(typeId, localIds);
        this.idSet = idSet;
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
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        if (typeId == entityId.getTypeId()) {
            final int result = Arrays.binarySearch(localIds.getIdArray(), entityId.getLocalId());
            if (result >= 0) {
                return result;
            }
        }
        return -1;
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        if (typeId == NULL_TYPE_ID) {
            return new EntityIdArrayIteratorNullTypeId(this, localIds.count());
        }
        return new OrderedEntityIdCollectionIterator(this, localIds);
    }

    @NotNull
    @Override
    public EntityIteratorBase getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        if (typeId == NULL_TYPE_ID) {
            return new ReverseEntityIdArrayIteratorNullTypeId(this, localIds.count());
        }
        return new ReverseOrderedEntityIdCollectionIterator(this, localIds);
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull PersistentStoreTransaction txn) {
        EntityIdSet result = idSet;
        if (result != null) {
            return result;
        }
        result = typeId == NULL_TYPE_ID ? EntityIdSetFactory.newSet().add(null) : toSetImpl();
        idSet = result;
        return result;
    }

    @NotNull
    private EntityIdSet toSetImpl() {
        EntityIdSet result = EntityIdSetFactory.newSet();
        for (long localId : localIds.getIdArray()) {
            result = result.add(typeId, localId);
        }
        return result;
    }
}
