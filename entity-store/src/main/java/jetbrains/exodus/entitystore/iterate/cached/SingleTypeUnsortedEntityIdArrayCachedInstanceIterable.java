/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.entitystore.iterate.cached.iterator.EntityIdArrayIteratorNullTypeId;
import jetbrains.exodus.entitystore.iterate.cached.iterator.EntityIdArrayIteratorSingleTypeId;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseEntityIdArrayIteratorNullTypeId;
import jetbrains.exodus.entitystore.iterate.cached.iterator.ReverseEntityIdArrayIteratorSingleTypeId;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static jetbrains.exodus.entitystore.iterate.EntityIdArrayCachedInstanceIterableFactory.MAX_COMPRESSED_SET_LOAD_FACTOR;

public class SingleTypeUnsortedEntityIdArrayCachedInstanceIterable extends CachedInstanceIterable {
    private final int typeId;
    private final long[] localIds;
    @Nullable
    private EntityIdSet idSet;

    public SingleTypeUnsortedEntityIdArrayCachedInstanceIterable(@NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source,
                                                                 int typeId, long[] localIds, @Nullable EntityIdSet idSet) {
        super(txn, source);
        this.typeId = typeId;
        this.localIds = localIds;
        this.idSet = idSet;
    }

    @Override
    public int getEntityTypeId() {
        return typeId;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    protected CachedInstanceIterable orderById() {
        Arrays.sort(localIds);
        if (USE_BIT_SETS && typeId != NULL_TYPE_ID) {
            final int length = localIds.length;
            if (length > 1) {
                final long min = localIds[0];
                if (min >= 0) {
                    final long range = localIds[length - 1] - min + 1;
                    if (range < Integer.MAX_VALUE
                            && range <= ((long) MAX_COMPRESSED_SET_LOAD_FACTOR * length)) {
                        final SortedEntityIdSet set = new ImmutableSingleTypeEntityIdBitSet(
                                typeId, localIds, length
                        );
                        return new SingleTypeSortedSetEntityIdCachedInstanceIterable(txnGetter.getTxn(this), getSource(), typeId, set);
                    }
                }
            }
        }
        return new SingleTypeSortedEntityIdArrayCachedInstanceIterable(txnGetter.getTxn(this), getSource(), typeId, localIds, idSet);
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return localIds.length;
    }

    @Override
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        if (typeId == entityId.getTypeId()) {
            return LongArrayList.indexOf(localIds, entityId.getLocalId());
        }
        return -1;
    }

    @NotNull
    @Override
    public EntityIteratorBase getIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        if (typeId == NULL_TYPE_ID) {
            return new EntityIdArrayIteratorNullTypeId(this, localIds.length);
        }
        return new EntityIdArrayIteratorSingleTypeId(this, typeId, localIds);
    }

    @NotNull
    @Override
    public EntityIteratorBase getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        if (typeId == NULL_TYPE_ID) {
            return new ReverseEntityIdArrayIteratorNullTypeId(this, localIds.length);
        }
        return new ReverseEntityIdArrayIteratorSingleTypeId(this, typeId, localIds);
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull PersistentStoreTransaction txn) {
        EntityIdSet result = idSet;
        if (result != null) {
            return result;
        }
        if (typeId == NULL_TYPE_ID) {
            result = EntityIdSetFactory.newSet().add(null);
        } else {
            result = EntityIdSetFactory.newSet();
            for (long localId : localIds) {
                result = result.add(typeId, localId);
            }
        }
        idSet = result;
        return result;
    }
}
