/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntitiesWithPropertyIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int propertyId;

    static {
        registerType(getType(), (txn, store, parameters) -> new EntitiesWithPropertyIterable(txn,
            Integer.parseInt((String) parameters[0]), Integer.parseInt((String) parameters[1])));
    }

    public EntitiesWithPropertyIterable(@NotNull final StoreTransaction txn,
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
    public EntityIterator getIteratorImpl(@NotNull final StoreTransaction txn) {
        return new FieldIndexIterator(this, entityTypeId, propertyId,
            getStoreImpl().getEntityWithPropIterable((PersistentStoreTransaction) txn, entityTypeId, propertyId));
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesWithPropertyIterableHandle();
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull StoreTransaction txn) {
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
        public boolean isMatchedPropertyChanged(@NotNull final EntityId id,
                                                final int propertyId,
                                                @Nullable final Comparable oldValue,
                                                @Nullable final Comparable newValue) {
            return EntitiesWithPropertyIterable.this.propertyId == propertyId && entityTypeId == id.getTypeId();
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
}
