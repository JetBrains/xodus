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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.ComparableBinding;
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
public final class PropertyValueIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int propertyId;
    @NotNull
    private final Comparable value;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                try {
                    return new PropertyValueIterable(store,
                            Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]),
                            Long.parseLong((String) parameters[2]));
                } catch (NumberFormatException e) {
                    return new PropertyValueIterable(store,
                            Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]),
                            (Comparable) parameters[2]);
                }
            }
        });
    }

    public PropertyValueIterable(@NotNull final PersistentEntityStoreImpl store, final int entityTypeId,
                                 final int propertyId, @NotNull final Comparable value) {
        super(store);
        this.entityTypeId = entityTypeId;
        this.propertyId = propertyId;
        this.value = PropertyTypes.toLowerCase(value);
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        // first, look for cached properties iterable (whole index in-memory)
        final EntityIterableCacheImpl iterableCache = getStore().getEntityIterableCache();
        final PropertiesIterable propertiesIterable = new PropertiesIterable(getStore(), entityTypeId, propertyId);
        final EntityIterableBase it = iterableCache.putIfNotCached(propertiesIterable);
        if (it.isCachedInstance()) {
            final PropertiesCachedInstanceIterable wrapper = (PropertiesCachedInstanceIterable) it;
            if (value.getClass() != wrapper.getPropertyValueClass()) {
                return EntityIteratorBase.EMPTY;
            }
            return wrapper.getPropertyValueIterator(value);
        }
        final Cursor valueIdx = openCursor(txn);
        if (valueIdx == null) {
            return EntityIteratorBase.EMPTY;
        }
        return new PropertyValueIterator(valueIdx);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), PropertyValueIterable.getType()) {

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
            public boolean isMatchedPropertyChanged(final int typeId,
                                                    final int propertyId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                //noinspection OverlyComplexBooleanExpression
                return PropertyValueIterable.this.propertyId == propertyId && entityTypeId == typeId &&
                        (isValueMatched(oldValue) || isValueMatched(newValue));
            }

            private boolean isValueMatched(Comparable value) {
                if (value == null) {
                    return false;
                }
                value = PropertyTypes.toLowerCase(value);
                return value.compareTo(PropertyValueIterable.this.value) == 0;
            }
        };
    }

    private static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_BY_PROP_VALUE;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final ByteIterable key = getStore().getPropertyTypes().dataToPropertyValue(value).dataToEntry();
        final Cursor valueIdx = openCursor(txn);
        if (valueIdx == null) {
            return 0;
        }
        return new SingleKeyCursorCounter(valueIdx, key).getCount();
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getPropertyValuesIndexCursor(txn, entityTypeId, propertyId);
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
                final EntityId result = new PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(cursor.getValue()));
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
