/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.bindings.IntegerBinding;
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
                return new EntitiesOfTypeIterable(txn, store, Integer.valueOf((String) parameters[0]));
            }
        });
    }

    public EntitiesOfTypeIterable(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntityStoreImpl store, final int entityTypeId) {
        super(store);
        this.entityTypeId = entityTypeId;
        if (!txn.isCurrent()) {
            txnGetter = txn;
        }
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ALL_ENTITIES;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntitiesOfTypeIterator(this, getStore().getEntitiesIndexCursor(txn, entityTypeId));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntitiesOfTypeIterable.getType()) {
            @Override
            public void getStringHandle(@NotNull final StringBuilder builder) {
                super.getStringHandle(builder);
                builder.append('-');
                builder.append(entityTypeId);
            }

            @Override
            public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
                return added.getTypeId() == entityTypeId;
            }

            @Override
            public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
                return deleted.getTypeId() == entityTypeId;
            }
        };
    }

    @Override
    public boolean nonCachedHasFastCount() {
        return true;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getEntitiesTable(txn, entityTypeId).count(txn.getEnvironmentTransaction());
    }

    private final class EntitiesOfTypeIterator extends EntityIteratorBase {

        private boolean hasNext;
        private boolean hasNextValid;

        private EntitiesOfTypeIterator(@NotNull final EntitiesOfTypeIterable iterable,
                                       @NotNull final Cursor index) {
            super(iterable);
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
        public int getCurrentVersion() {
            return IntegerBinding.compressedEntryToInt(getCursor().getValue());
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                explain(getType());
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

}
