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
import jetbrains.exodus.entitystore.iterate.cached.iterator.EntityIdArrayIteratorMultiTypeIdUnpacked;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseEntityIdArrayIteratorMultiTypeIdUnpacked;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MultiTypeUnsortedEntityIdArrayCachedInstanceIterable extends CachedInstanceIterable {
    private final int[] typeIds;
    private final long[] localIds;
    @Nullable
    private EntityIdSet idSet;

    public MultiTypeUnsortedEntityIdArrayCachedInstanceIterable(@NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source,
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
        return false;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return localIds.length;
    }

    @Override
    protected int indexOfImpl(@NotNull EntityId entityId) {
        final long localId = entityId.getLocalId();
        int result = 0;
        do {
            if (localIds[result] == localId && typeIds[result] == entityId.getTypeId()) {
                return result;
            }
            ++result;
        } while (result < localIds.length);
        return -1;
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new EntityIdArrayIteratorMultiTypeIdUnpacked(this, typeIds, localIds);
    }

    @NotNull
    @Override
    public EntityIteratorBase getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return new ReverseEntityIdArrayIteratorMultiTypeIdUnpacked(this, typeIds, localIds);
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
        for (int i = 0; i < count; ++i) {
            final int typeId = typeIds[i];
            if (typeId == NULL_TYPE_ID) {
                result = result.add(null);
            } else {
                result = result.add(typeId, localIds[i]);
            }
        }
        idSet = result;
        return result;
    }
}
