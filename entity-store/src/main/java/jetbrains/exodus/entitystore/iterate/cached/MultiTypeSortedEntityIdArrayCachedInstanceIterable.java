/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import jetbrains.exodus.entitystore.iterate.cached.iterator.EntityIdArrayIteratorMultiTypeIdPacked;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseEntityIdArrayIteratorMultiTypeIdPacked;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class MultiTypeSortedEntityIdArrayCachedInstanceIterable extends CachedInstanceIterable {
    private final int[] typeIds;
    private final long[] localIds;
    @Nullable
    private EntityIdSet idSet;

    public MultiTypeSortedEntityIdArrayCachedInstanceIterable(@NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source,
                                                              int[] typeIds, long[] localIds, @Nullable EntityIdSet idSet) {
        super(txn, source);
        this.typeIds = typeIds;
        this.localIds = localIds;
        this.idSet = idSet;
    }

    @Override
    protected CachedInstanceIterable orderById() {
        return this;
    }

    @Override
    public boolean isSortedById() {
        return true;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return localIds.length;
    }

    @Override
    protected boolean containsImpl(@NotNull EntityId entityId) {
        final EntityIdSet ids = idSet;
        if (ids != null) {
            return ids.contains(entityId);
        }
        return super.containsImpl(entityId);
    }

    @Override
    protected int indexOfImpl(@NotNull EntityId entityId) {
        final long localId = entityId.getLocalId();
        final int typeId = entityId.getTypeId();
        int prevBound = 0;
        final int length = typeIds.length;
        for (int i = 0; i < length; ++i) {
            if (typeIds[i] == typeId) {
                ++i;
                final int result = Arrays.binarySearch(localIds, prevBound, typeIds[i], localId);

                if (result >= 0) {
                    return result;
                }
                break;
            } else {
                ++i;
                prevBound = typeIds[i];
            }
        }
        return -1;
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new EntityIdArrayIteratorMultiTypeIdPacked(this, typeIds, localIds);
    }

    @NotNull
    @Override
    public EntityIteratorBase getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new ReverseEntityIdArrayIteratorMultiTypeIdPacked(this, typeIds, localIds);
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull PersistentStoreTransaction txn) {
        EntityIdSet result = idSet;
        if (result != null) {
            return result;
        }
        final int count = typeIds.length;
        result = EntityIdSetFactory.newSet();
        int j = 0;
        for (int i = 0; i < count; ++i) {
            final int typeId = typeIds[i];
            ++i;
            final int upperBound = typeIds[i];
            if (typeId == NULL_TYPE_ID) {
                while (j < upperBound) {
                    result = result.add(null);
                    ++j;
                }
            } else {
                while (j < upperBound) {
                    result = result.add(typeId, localIds[j++]);
                }
            }
        }
        idSet = result;
        return result;
    }
}
