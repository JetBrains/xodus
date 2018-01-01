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

import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntitiesWithPropertyIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int propertyId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntitiesWithPropertyIterable(txn,
                        Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]));
            }
        });
    }

    public EntitiesWithPropertyIterable(@NotNull final PersistentStoreTransaction txn,
                                        final int entityTypeId,
                                        final int propertyId) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.propertyId = propertyId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_PROPERTY;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new PropertiesIterator(getStore().getEntityWithPropCursor(txn, entityTypeId));
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesWithPropertyIterableHandle();
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull PersistentStoreTransaction txn) {
        return new UpdatableEntityIdSortedSetCachedInstanceIterable(txn, this);
    }

    private final class EntitiesWithPropertyIterableHandle extends ConstantEntityIterableHandle {

        private EntitiesWithPropertyIterableHandle() {
            super(EntitiesWithPropertyIterable.this.getStore(), EntitiesWithPropertyIterable.getType());
        }

        @NotNull
        @Override
        public int[] getPropertyIds() {
            return new int[]{propertyId};
        }

        @Override
        public void toString(@NotNull final StringBuilder builder) {
            super.toString(builder);
            builder.append(entityTypeId);
            builder.append('-');
            builder.append(propertyId);
        }

        @Override
        public void hashCode(@NotNull final EntityIterableHandleHash hash) {
            hash.apply(entityTypeId);
            hash.applyDelimiter();
            hash.apply(propertyId);
        }

        @Override
        public int getEntityTypeId() {
            return entityTypeId;
        }

        @Override
        public boolean isMatchedPropertyChanged(final int typeId,
                                                final int propertyId,
                                                @Nullable final Comparable oldValue,
                                                @Nullable final Comparable newValue) {
            return EntitiesWithPropertyIterable.this.propertyId == propertyId && entityTypeId == typeId;
        }

        @Override
        public boolean onPropertyChanged(@NotNull PropertyChangedHandleChecker handleChecker) {
            final Comparable oldValue = handleChecker.getOldValue();
            final Comparable newValue = handleChecker.getNewValue();
            if (oldValue == null || newValue == null) {
                UpdatableEntityIdSortedSetCachedInstanceIterable iterable
                        = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatableEntityIdSortedSetCachedInstanceIterable.class);
                if (iterable != null) {
                    final long localId = handleChecker.getLocalId();
                    if (oldValue == null) {
                        iterable.addEntity(new PersistentEntityId(entityTypeId, localId));
                    } else {
                        iterable.removeEntity(new PersistentEntityId(entityTypeId, localId));
                    }
                    return true;
                }
            }
            return false;
        }
    }

    public final class PropertiesIterator extends EntityIteratorBase {

        private boolean hasNext;

        public PropertiesIterator(@NotNull final Cursor cursor) {
            super(EntitiesWithPropertyIterable.this);
            setCursor(cursor);
            hasNext = cursor.getSearchKey(IntegerBinding.intToCompressedEntry(propertyId)) != null;
        }

        @Override
        protected boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        protected EntityId nextIdImpl() {
            if (hasNext) {
                explain(getType());
                final Cursor cursor = getCursor();
                final long localId = LongBinding.compressedEntryToLong(cursor.getValue());
                final EntityId result = new PersistentEntityId(entityTypeId, localId);
                hasNext = cursor.getNextDup();
                return result;
            }
            return null;
        }
    }
}
