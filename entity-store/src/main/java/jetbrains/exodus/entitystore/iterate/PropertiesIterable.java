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
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
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
                return new PropertiesIterable(store, Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]));
            }
        });
    }

    private static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_PROPERTY_SORTED_BY_VALUE;
    }

    public PropertiesIterable(@NotNull final PersistentEntityStoreImpl store, final int entityTypeId, final int propertyId) {
        super(store);
        this.entityTypeId = entityTypeId;
        this.propertyId = propertyId;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    public boolean canBeReordered() {
        // it always has to be sorted by property
        return false;
    }

    @Override
    protected CachedWrapperIterable createCachedWrapper(@NotNull final PersistentStoreTransaction txn) {
        return new PropertiesIterableWrapper(getStore(), getIterator(txn, true), this);
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
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final Cursor cursor = openCursor(txn);
        if (cursor == null) {
            return 0;
        }
        try {
            return cursor.count();
        } finally {
            cursor.close();
        }
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getPropertyValuesIndexCursor(txn, entityTypeId, propertyId);
    }

    private PropertiesIterator getIterator(@NotNull final PersistentStoreTransaction txn, final boolean ascending) {
        final Cursor primaryIndex = getStore().getPrimaryPropertyIndexCursor(txn, entityTypeId);
        try {
            final Cursor valueIdx = openCursor(txn);
            if (valueIdx == null) {
                return null;
            }
            return new PropertiesIterator(valueIdx, primaryIndex, ascending);
        } finally {
            primaryIndex.close();
        }
    }

    /**
     * Public access is needed in order to access directly from PersistentStoreTransaction.
     */

    public final class PropertiesIterableHandle extends ConstantEntityIterableHandle {
        public PropertiesIterableHandle() {
            super(PropertiesIterable.this.getStore(), PropertiesIterable.getType());
        }

        @Override
        public void getStringHandle(@NotNull final StringBuilder builder) {
            super.getStringHandle(builder);
            builder.append('-');
            builder.append(entityTypeId);
            builder.append('-');
            builder.append(propertyId);
        }

        @Override
        public boolean isMatchedPropertyChanged(final int typeId,
                                                final int propertyId,
                                                @Nullable final Comparable oldValue,
                                                @Nullable final Comparable newValue) {
            return PropertiesIterable.this.propertyId == propertyId && entityTypeId == typeId;
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
                binding = (hasNext = value != null) ? getStore().getPropertyTypes().entryToPropertyValue(value).getBinding() : null;
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
