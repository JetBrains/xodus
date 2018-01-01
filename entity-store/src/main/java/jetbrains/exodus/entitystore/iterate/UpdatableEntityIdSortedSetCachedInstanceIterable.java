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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.cached.iterator.OrderedEntityIdCollectionIterator;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class UpdatableEntityIdSortedSetCachedInstanceIterable extends UpdatableCachedInstanceIterable {

    private static final PersistentLongSet EMPTY_IDS = new PersistentBitTreeLongSet();

    private final int entityTypeId;
    @NotNull
    private final PersistentLongSet localIds;
    @Nullable
    private PersistentLongSet.MutableSet mutableLocalIds;
    @Nullable
    private EntityIdSet idSet;
    @Nullable
    private OrderedEntityIdCollection idCollection;

    public UpdatableEntityIdSortedSetCachedInstanceIterable(@NotNull final PersistentStoreTransaction txn,
                                                            @NotNull final EntityIterableBase source) {
        super(txn, source);
        entityTypeId = source.getEntityTypeId();
        final EntityIteratorBase it = (EntityIteratorBase) source.getIteratorImpl(txn);
        try {
            if (!it.hasNext()) {
                localIds = EMPTY_IDS;
            } else {
                localIds = EMPTY_IDS.getClone();
                final PersistentLongSet.MutableSet mutableLocalIds = localIds.beginWrite();
                do {
                    final EntityId entityId = it.nextId();
                    if (entityId == null) {
                        throw new NullPointerException("EntityIteratorBase.nextId() returned null!");
                    }
                    mutableLocalIds.add(entityId.getLocalId());
                } while (it.hasNext());
                mutableLocalIds.endWrite();
            }
        } finally {
            it.disposeIfShouldBe();
        }
        mutableLocalIds = null;
        idSet = null;
        idCollection = null;
    }

    // constructor for mutating
    protected UpdatableEntityIdSortedSetCachedInstanceIterable(@NotNull final UpdatableEntityIdSortedSetCachedInstanceIterable source) {
        super(source.getTransaction(), source);
        entityTypeId = source.entityTypeId;
        localIds = source.localIds.getClone();
        mutableLocalIds = localIds.beginWrite();
        idSet = null;
        idCollection = null;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        if (localIds == EMPTY_IDS && mutableLocalIds == null) {
            return EntityIteratorBase.EMPTY;
        }
        final PersistentLongSet.MutableSet mutableSet = mutableLocalIds;
        if (mutableSet == null) {
            return new OrderedEntityIdCollectionIterator(this, getOrCreateIdCollection());
        }
        return new NonDisposableEntityIterator(this) {

            private final LongIterator it = mutableSet.longIterator();

            @Override
            protected boolean hasNextImpl() {
                return it.hasNext();
            }

            @Nullable
            @Override
            protected EntityId nextIdImpl() {
                return new PersistentEntityId(entityTypeId, it.next());
            }
        };
    }

    @Override
    public long size() {
        return getCurrentMap().size();
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull final PersistentStoreTransaction txn) {
        if (idSet == null) {
            final boolean isImmutable = mutableLocalIds == null;
            if (isImmutable) {
                final EntityIdCollection idCollection = getOrCreateIdCollection();
                if (idCollection instanceof EntityIdSet) {
                    final EntityIdSet result = (EntityIdSet) idCollection;
                    idSet = result;
                    return result;
                }
            }
            final EntityIterator it = getIteratorImpl(txn);
            EntityIdSet result = EntityIdSetFactory.newSet();
            while (it.hasNext()) {
                result = result.add(it.nextId());
            }
            if (!isImmutable) {
                return result;
            }
            idSet = result;
            return result;
        }
        return idSet;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return size();
    }

    @Override
    public UpdatableEntityIdSortedSetCachedInstanceIterable beginUpdate() {
        return new UpdatableEntityIdSortedSetCachedInstanceIterable(this);
    }

    @Override
    public boolean isMutated() {
        return mutableLocalIds != null;
    }

    @Override
    public void endUpdate() {
        checkMutableIds().endWrite();
        mutableLocalIds = null;
    }

    final void addEntity(final EntityId id) {
        checkEntityType(id);
        checkMutableIds().add(id.getLocalId());
    }

    final void removeEntity(final EntityId id) {
        checkEntityType(id);
        checkMutableIds().remove(id.getLocalId());
    }

    @NotNull
    private OrderedEntityIdCollection getOrCreateIdCollection() {
        OrderedEntityIdCollection collection = this.idCollection;
        if (collection == null) {
            PersistentLongSet.ImmutableSet currentSet = localIds.beginRead();
            final long[] result = new long[currentSet.size()];
            final LongIterator it = currentSet.longIterator();
            int i = 0;
            while (it.hasNext()) {
                result[i++] = it.next();
            }
            collection = EntityIdArrayCachedInstanceIterableFactory.makeIdCollection(entityTypeId, result);
            this.idCollection = collection;
        }
        return collection;
    }

    private PersistentLongSet.ImmutableSet getCurrentMap() {
        return mutableLocalIds == null ? localIds.beginRead() : mutableLocalIds;
    }

    private PersistentLongSet.MutableSet checkMutableIds() {
        PersistentLongSet.MutableSet mutableLocalIds = this.mutableLocalIds;
        if (mutableLocalIds == null) {
            throw new IllegalStateException("UpdatableEntityIdSortedSetCachedInstanceIterable was not mutated");
        }
        return mutableLocalIds;
    }

    private void checkEntityType(EntityId id) {
        if (id.getTypeId() != entityTypeId) {
            throw new IllegalStateException("Unexpected entity type id: " + id.getTypeId());
        }
    }
}
