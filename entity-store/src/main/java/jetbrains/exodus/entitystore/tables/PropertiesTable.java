/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.ComparableSet;
import jetbrains.exodus.bindings.ComparableValueType;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * A table for storing properties with secondary indexes of property values.
 */
public final class PropertiesTable extends Table {

    @NonNls
    private static final String PROP_VALUE_IDX = "#value_idx";

    @NotNull
    private final PersistentEntityStoreImpl store;
    private final Store primaryStore;
    private final IntHashMap<Store> valueIndexes;
    private final FieldIndex allPropsIndex;

    public PropertiesTable(@NotNull final PersistentStoreTransaction txn,
                           @NotNull final String name,
                           @NotNull final StoreConfig primaryConfig) {
        this.store = txn.getStore();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final Environment env = store.getEnvironment();
        primaryStore = env.openStore(name, primaryConfig, envTxn);
        allPropsIndex = FieldIndex.fieldIndex(txn, name);
        store.trackTableCreation(primaryStore, txn);
        valueIndexes = new IntHashMap<>();
    }

    @Nullable
    public ByteIterable get(@NotNull final PersistentStoreTransaction txn, @NotNull final ByteIterable key) {
        return primaryStore.get(txn.getEnvironmentTransaction(), key);
    }

    /**
     * Setter for property value. Doesn't affect entity version and doesn't
     * invalidate any of the cached entity iterables.
     *
     * @param localId    entity local id.
     * @param value      property value.
     * @param oldValue   property old value
     * @param propertyId property id
     */
    public void put(@NotNull final PersistentStoreTransaction txn,
                    final long localId,
                    @NotNull final ByteIterable value,
                    @Nullable final ByteIterable oldValue,
                    final int propertyId,
                    @NotNull final ComparableValueType type) {
        final Store valueIdx = getOrCreateValueIndex(txn, propertyId);
        final ByteIterable key = PropertyKey.propertyKeyToEntry(new PropertyKey(localId, propertyId));
        final Transaction envTxn = txn.getEnvironmentTransaction();
        boolean success = primaryStore.put(envTxn, key, value);
        final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
        if (oldValue == null) {
            if (!allPropsIndex.put(envTxn, propertyId, localId)) {
                success = false;
            }
        } else {
            if (!deleteFromStore(envTxn, valueIdx, secondaryValue, createSecondaryKeys(store.getPropertyTypes(), oldValue, type))) {
                success = false;
            }
        }
        for (final ByteIterable secondaryKey : createSecondaryKeys(store.getPropertyTypes(), value, type)) {
            if (!valueIdx.put(envTxn, secondaryKey, secondaryValue)) {
                success = false;
            }
        }
        checkStatus(success, "Failed to put");
    }

    public void delete(@NotNull final PersistentStoreTransaction txn,
                       final long localId,
                       @NotNull final ByteIterable value,
                       final int propertyId,
                       @NotNull final ComparableValueType type) {
        checkStatus(deleteNoFail(txn, localId, value, propertyId, type), "Failed to delete");
    }

    public boolean deleteNoFail(@NotNull final PersistentStoreTransaction txn,
                                final long localId,
                                @NotNull final ByteIterable value,
                                int propertyId,
                                @NotNull final ComparableValueType type) {
        final ByteIterable key = PropertyKey.propertyKeyToEntry(new PropertyKey(localId, propertyId));
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
        return primaryStore.delete(envTxn, key) &&
                deleteFromStore(envTxn, getOrCreateValueIndex(txn, propertyId),
                        secondaryValue, createSecondaryKeys(store.getPropertyTypes(), value, type)) &&
                allPropsIndex.remove(envTxn, propertyId, localId);
    }

    public Store getPrimaryIndex() {
        return primaryStore;
    }

    public FieldIndex getAllPropsIndex() {
        return allPropsIndex;
    }

    @Nullable
    public Store getValueIndex(@NotNull final PersistentStoreTransaction txn, final int propertyId, final boolean creationRequired) {
        Store valueIndex;
        synchronized (valueIndexes) {
            valueIndex = valueIndexes.get(propertyId);
            if (valueIndex == null) {
                final Transaction envTxn = txn.getEnvironmentTransaction();
                valueIndex = envTxn.getEnvironment().openStore(
                        valueIndexName(propertyId), StoreConfig.WITH_DUPLICATES, envTxn, creationRequired);
                if (valueIndex != null && !valueIndex.getConfig().temporaryEmpty) {
                    store.trackTableCreation(valueIndex, txn);
                    valueIndexes.put(propertyId, valueIndex);
                }
            }
        }
        return valueIndex;
    }

    @NotNull
    public Store getOrCreateValueIndex(@NotNull final PersistentStoreTransaction txn, final int propertyId) {
        final Store result = getValueIndex(txn, propertyId, true);
        if (result == null) {
            throw new EntityStoreException("Failed to create value index " + valueIndexName(propertyId));
        }
        return result;
    }

    @NotNull
    public Collection<Map.Entry<Integer, Store>> getValueIndices() {
        synchronized (valueIndexes) {
            return new ArrayList<>(valueIndexes.entrySet());
        }
    }

    public static ByteIterable[] createSecondaryKeys(@NotNull final PropertyTypes propertyTypes,
                                                     @NotNull final ByteIterable value,
                                                     @NotNull final ComparableValueType type) {
        final int valueTypeId = type.getTypeId();
        if (valueTypeId == ComparableValueType.STRING_VALUE_TYPE) {
            final PropertyValue propValue = propertyTypes.entryToPropertyValue(value);
            return new ByteIterable[]{new PropertyValue(type, ((String) propValue.getData()).toLowerCase()).dataToEntry()};
        }
        if (valueTypeId == ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
            final PropertyValue propValue = propertyTypes.entryToPropertyValue(value);
            final ComparableSet data = (ComparableSet) propValue.getData();
            final Class itemClass = data.getItemClass();
            final ComparableBinding itemBinding = propertyTypes.getPropertyType(itemClass).getBinding();
            final ByteIterable[] result = new ByteIterable[data.size()];
            //noinspection unchecked
            data.forEach((item, index) -> result[index] = itemBinding.objectToEntry(PropertyTypes.toLowerCase(item)));
            return result;
        }
        return new ByteIterable[]{value.subIterable(1, value.getLength() - 1)}; // skip property type
    }

    private String valueIndexName(final int propertyId) {
        return primaryStore.getName() + PROP_VALUE_IDX + propertyId;
    }

    private static boolean deleteFromStore(@NotNull final Transaction txn,
                                           @NotNull final Store store,
                                           @NotNull final ByteIterable value,
                                           @NotNull final ByteIterable... keys) {
        boolean result = true;
        try (Cursor cursor = store.openCursor(txn)) {
            for (final ByteIterable key : keys) {
                if (!cursor.getSearchBoth(key, value)) {
                    // repeat for debugging
                    cursor.getSearchBoth(key, value);
                    result = false;
                } else {
                    if (!cursor.deleteCurrent()) {
                        // repeat for debugging
                        cursor.deleteCurrent();
                        result = false;
                    }
                }
            }
            return result;
        }
    }

    @Override
    public boolean canBeCached() {
        return !primaryStore.getConfig().temporaryEmpty && !allPropsIndex.getStore().getConfig().temporaryEmpty;
    }
}
