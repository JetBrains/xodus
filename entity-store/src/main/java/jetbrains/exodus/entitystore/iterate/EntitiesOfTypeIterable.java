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

import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates all entities of specified entity type.
 */
public class EntitiesOfTypeIterable extends EntityIterableBase {

    private final int entityTypeId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntitiesOfTypeIterable(txn, Integer.valueOf((String) parameters[0]));
            }
        });
    }

    public EntitiesOfTypeIterable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        super(txn);
        this.entityTypeId = entityTypeId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ALL_ENTITIES;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntitiesOfTypeIterator(this, getStore().getEntitiesIndexCursor(txn, entityTypeId));
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesOfTypeIterableHandle(this);
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getEntitiesTable(txn, entityTypeId).count(txn.getEnvironmentTransaction());
    }

    @Override
    public boolean isEmptyImpl(@NotNull final PersistentStoreTransaction txn) {
        return countImpl(txn) == 0;
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull final PersistentStoreTransaction txn) {
        return new UpdatableEntityIdSortedSetCachedInstanceIterable(txn, this);
    }

    public static final class EntitiesOfTypeIterator extends EntityIteratorBase {

        private boolean hasNext;
        private boolean hasNextValid;
        protected final int entityTypeId;

        public EntitiesOfTypeIterator(@NotNull final EntitiesOfTypeIterable iterable,
                                      @NotNull final Cursor index) {
            super(iterable);
            entityTypeId = iterable.entityTypeId;
            setCursor(index);
        }

        @Override
        public boolean hasNextImpl() {
            if (!hasNextValid) {
                hasNext = getCursor().getNext();
                hasNextValid = true;
            }
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                getIterable().explain(getType());
                final EntityId result = getEntityId();
                hasNextValid = false;
                return result;
            }
            return null;
        }

        @Nullable
        @Override
        public EntityId getLast() {
            if (!getCursor().getPrev()) {
                return null;
            }
            return getEntityId();
        }

        private EntityId getEntityId() {
            return new PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(getCursor().getKey()));
        }
    }

    public static class EntitiesOfTypeIterableHandle extends ConstantEntityIterableHandle {
        protected final int entityTypeId;

        public EntitiesOfTypeIterableHandle(@NotNull final EntitiesOfTypeIterable source) {
            super(source.getStore(), EntitiesOfTypeIterable.getType());
            this.entityTypeId = source.entityTypeId;
        }

        @Override
        public void toString(@NotNull final StringBuilder builder) {
            super.toString(builder);
            builder.append(entityTypeId);
        }

        @Override
        public void hashCode(@NotNull final EntityIterableHandleHash hash) {
            hash.apply(entityTypeId);
        }

        @Override
        public int getEntityTypeId() {
            return entityTypeId;
        }

        @NotNull
        @Override
        public int[] getTypeIdsAffectingCreation() {
            return new int[]{entityTypeId};
        }

        @Override
        public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
            return added.getTypeId() == entityTypeId;
        }

        @Override
        public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
            return deleted.getTypeId() == entityTypeId;
        }

        @Override
        public boolean onEntityAdded(@NotNull EntityAddedOrDeletedHandleChecker handleChecker) {
            UpdatableEntityIdSortedSetCachedInstanceIterable iterable
                    = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatableEntityIdSortedSetCachedInstanceIterable.class);
            if (iterable != null) {
                iterable.addEntity(handleChecker.getId());
                return true;
            }
            return false;
        }

        @Override
        public boolean onEntityDeleted(@NotNull EntityAddedOrDeletedHandleChecker handleChecker) {
            UpdatableEntityIdSortedSetCachedInstanceIterable iterable
                    = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatableEntityIdSortedSetCachedInstanceIterable.class);
            if (iterable != null) {
                iterable.removeEntity(handleChecker.getId());
                return true;
            }
            return false;
        }
    }
}
