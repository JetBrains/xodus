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
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates all entities of specified entity type having specified property equal to a value.
 */
@SuppressWarnings({"unchecked"})
public final class PropertyValueIterable extends PropertyRangeOrValueIterableBase {

    @NotNull
    private final Comparable value;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                try {
                    return new PropertyValueIterable(txn,
                        Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]),
                        Long.parseLong((String) parameters[2]));
                } catch (NumberFormatException e) {
                    return new PropertyValueIterable(txn,
                        Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]),
                        (Comparable) parameters[2]);
                }
            }
        });
    }

    public PropertyValueIterable(@NotNull final PersistentStoreTransaction txn,
                                 final int entityTypeId,
                                 final int propertyId,
                                 @NotNull final Comparable value) {
        super(txn, entityTypeId, propertyId);
        this.value = PropertyTypes.toLowerCase(value);
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        final EntityIterableBase it = getPropertyValueIndex();
        if (it.isCachedInstance()) {
            final UpdatablePropertiesCachedInstanceIterable cached = (UpdatablePropertiesCachedInstanceIterable) it;
            if (value.getClass() != cached.getPropertyValueClass()) {
                return EntityIteratorBase.EMPTY;
            }
            return cached.getPropertyValueIterator(value);
        }
        final Cursor valueIdx = openCursor(txn);
        if (valueIdx == null) {
            return EntityIteratorBase.EMPTY;
        }
        return new PropertyValueIterator(valueIdx);
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        final int entityTypeId = getEntityTypeId();
        final int propertyId = getPropertyId();
        return new ConstantEntityIterableHandle(getStore(), PropertyValueIterable.getType()) {

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
                builder.append('-');
                builder.append(value.toString());
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(entityTypeId);
                hash.applyDelimiter();
                hash.apply(propertyId);
                hash.applyDelimiter();
                hash.apply(value.toString());
            }

            @Override
            public int getEntityTypeId() {
                return PropertyValueIterable.this.getEntityTypeId();
            }

            @Override
            public boolean isMatchedPropertyChanged(@NotNull final EntityId id,
                                                    final int propId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                //noinspection OverlyComplexBooleanExpression
                return propertyId == propId && entityTypeId == id.getTypeId() && (isValueMatched(oldValue) || isValueMatched(newValue));
            }

            private boolean isValueMatched(Comparable value) {
                if (value == null) {
                    return false;
                }
                if (value instanceof ComparableSet) {
                    return ((ComparableSet) value).containsItem(PropertyValueIterable.this.value);
                }
                value = PropertyTypes.toLowerCase(value);
                return value.compareTo(PropertyValueIterable.this.value) == 0;
            }
        };
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final ByteIterable key = getStore().getPropertyTypes().dataToPropertyValue(value).dataToEntry();
        final Cursor valueIdx = openCursor(txn);
        return valueIdx == null ? 0 : new SingleKeyCursorCounter(valueIdx, key).getCount();
    }

    @Override
    public boolean isEmptyImpl(@NotNull PersistentStoreTransaction txn) {
        final ByteIterable key = getStore().getPropertyTypes().dataToPropertyValue(value).dataToEntry();
        final Cursor valueIdx = openCursor(txn);
        return valueIdx == null || new SingleKeyCursorIsEmptyChecker(valueIdx, key).isEmpty();
    }

    private static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_BY_PROP_VALUE;
    }

    private final class PropertyValueIterator extends EntityIteratorBase {

        private boolean hasNext;
        @NotNull
        private final ComparableBinding binding;

        private PropertyValueIterator(@NotNull final Cursor cursor) {
            super(PropertyValueIterable.this);
            setCursor(cursor);
            binding = getStore().getPropertyTypes().dataToPropertyValue(value).getBinding();
            final ByteIterable key = binding.objectToEntry(value);
            checkHasNext(getCursor().getSearchKey(key) != null);
        }

        @Override
        public boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                explain(getType());
                final Cursor cursor = getCursor();
                final EntityId result = new PersistentEntityId(getEntityTypeId(), LongBinding.compressedEntryToLong(cursor.getValue()));
                checkHasNext(cursor.getNextDup());
                return result;
            }
            return null;
        }

        private void checkHasNext(final boolean success) {
            hasNext = success && value.compareTo(binding.entryToObject(getCursor().getKey())) == 0;
        }
    }
}
