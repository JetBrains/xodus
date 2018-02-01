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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.ComparableSet;
import jetbrains.exodus.bindings.ComparableValueType;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.entitystore.tables.PropertyValue;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Store;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"RawUseOfParameterizedType"})
public final class PropertiesIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int propertyId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new PropertiesIterable(txn, Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]));
            }
        });
    }

    private static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_PROPERTY_SORTED_BY_VALUE;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    public PropertiesIterable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId, final int propertyId) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.propertyId = propertyId;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        final PropertiesIterator result = getIterator(txn, true);
        if (result == null) {
            return EntityIteratorBase.EMPTY;
        }
        return result;
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @NotNull
    @Override
    public EntityIterator getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        final PropertiesIterator result = getIterator(txn, false);
        if (result == null) {
            return EntityIteratorBase.EMPTY;
        }
        return result;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new PropertiesIterableHandle();
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull final PersistentStoreTransaction txn) {
        return UpdatablePropertiesCachedInstanceIterable.newInstance(txn, getIterator(txn, true), this);
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final Store valueIndex = getStore().getPropertiesTable(txn, entityTypeId).getValueIndex(txn, propertyId, false);
        return valueIndex == null ? 0 : valueIndex.count(txn.getEnvironmentTransaction());
    }

    @Override
    public boolean isEmptyImpl(@NotNull final PersistentStoreTransaction txn) {
        return countImpl(txn) == 0;
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getPropertyValuesIndexCursor(txn, entityTypeId, propertyId);
    }

    private PropertiesIterator getIterator(@NotNull final PersistentStoreTransaction txn, final boolean ascending) {
        try (Cursor primaryIndex = getStore().getPrimaryPropertyIndexCursor(txn, entityTypeId)) {
            final Cursor valueIdx = openCursor(txn);
            if (valueIdx == null) {
                return null;
            }
            return new PropertiesIterator(valueIdx, primaryIndex, ascending);
        }
    }

    /**
     * Public access is needed in order to access directly from PersistentStoreTransaction.
     */

    private final class PropertiesIterableHandle extends ConstantEntityIterableHandle {

        public PropertiesIterableHandle() {
            super(PropertiesIterable.this.getStore(), PropertiesIterable.getType());
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
            return PropertiesIterable.this.propertyId == propertyId && entityTypeId == id.getTypeId();
        }

        @Override
        public boolean onPropertyChanged(@NotNull PropertyChangedHandleChecker handleChecker) {
            UpdatablePropertiesCachedInstanceIterable iterable
                    = PersistentStoreTransaction.getUpdatable(handleChecker, this, UpdatablePropertiesCachedInstanceIterable.class);
            if (iterable != null) {
                final Comparable oldValue = handleChecker.getOldValue();
                final Comparable newValue = handleChecker.getNewValue();
                final long localId = handleChecker.getLocalId();
                if (oldValue instanceof ComparableSet || newValue instanceof ComparableSet) {
                    //noinspection ConstantConditions
                    final ComparableSet oldSet = (ComparableSet) oldValue;
                    final ComparableSet newSet = (ComparableSet) newValue;
                    if (oldSet != null) {
                        //noinspection unchecked
                        for (final Comparable item : (Iterable<? extends Comparable>) oldSet.minus(newSet)) {
                            iterable.update(entityTypeId, localId, item, null);
                        }
                    }
                    if (newSet != null) {
                        //noinspection unchecked
                        for (final Comparable item : (Iterable<? extends Comparable>) newSet.minus(oldSet)) {
                            iterable.update(entityTypeId, localId, null, item);
                        }
                    }
                } else {
                    iterable.update(entityTypeId, localId, oldValue, newValue);
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Public access is needed in order to access directly from SortIterator.
     */
    private final class PropertiesIterator extends EntityIteratorBase implements PropertyValueIterator {

        private boolean hasNext;
        private final boolean ascending;
        private final ComparableBinding binding;
        @Nullable
        private Comparable currentValue;

        private PropertiesIterator(@NotNull final Cursor secondaryIndex,
                                   @NotNull final Cursor primaryIndex,
                                   final boolean ascending) {
            super(PropertiesIterable.this);
            setCursor(secondaryIndex);
            this.ascending = ascending;
            //noinspection AssignmentUsedAsCondition
            if (hasNext = getNext(secondaryIndex)) {
                final long entityLocalId = LongBinding.compressedEntryToLong(secondaryIndex.getValue());
                final ByteIterable value = primaryIndex.getSearchKey(
                        PropertyKey.propertyKeyToEntry(new PropertyKey(entityLocalId, propertyId)));
                if ((hasNext = value != null)) {
                    final PropertyValue propertyValue = getStore().getPropertyTypes().entryToPropertyValue(value);
                    if (propertyValue.getType().getTypeId() != ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
                        binding = propertyValue.getBinding();
                    } else {
                        final Class itemClass = ((ComparableSet) propertyValue.getData()).getItemClass();
                        if (itemClass == null) {
                            throw new NullPointerException("Can't be: null item class for a non-empty ComparableSet");
                        }
                        //noinspection unchecked
                        binding = getStore().getPropertyTypes().getPropertyType(itemClass).getBinding();
                    }
                } else {
                    binding = null;
                }
            } else {
                binding = null;
            }
        }

        @Override
        public boolean hasNextImpl() {
            currentValue = hasNext ? binding.entryToObject(getCursor().getKey()) : null;
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                explain(getType());
                final Cursor cursor = getCursor();
                final EntityId result = new PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(cursor.getValue()));
                hasNext = getNext(cursor);
                return result;
            }
            return null;
        }

        @Override
        @Nullable
        public Comparable currentValue() {
            return currentValue;
        }

        private boolean getNext(@NotNull final Cursor cursor) {
            return ascending ? cursor.getNext() : cursor.getPrev();
        }
    }
}
