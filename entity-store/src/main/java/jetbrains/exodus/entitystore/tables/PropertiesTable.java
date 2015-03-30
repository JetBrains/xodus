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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.iterate.FixedLengthByteIterable;
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
    @NonNls
    private static final String ALL_PROPS_IDX = "#all_idx";

    @NotNull
    private final PersistentEntityStoreImpl store;
    private final String name;
    private final Store primaryStore;
    private final IntHashMap<Store> valueIndexes;
    private final Store allPropsIndex;

    public PropertiesTable(@NotNull final PersistentStoreTransaction txn,
                           @NotNull final String name,
                           @NotNull final StoreConfig primaryConfig) {
        this.store = txn.getStore();
        this.name = name;
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final Environment env = store.getEnvironment();
        primaryStore = env.openStore(name, primaryConfig, envTxn);
        allPropsIndex = env.openStore(name + ALL_PROPS_IDX, StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, envTxn);
        store.trackTableCreation(primaryStore, txn);
        store.trackTableCreation(allPropsIndex, txn);
        valueIndexes = new IntHashMap<Store>();
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
                    @Nullable final PropertyType type) {
        final Store valueIdx = getOrCreateValueIndex(txn, propertyId);
        final ByteIterable key = PropertyKey.propertyKeyToEntry(new PropertyKey(localId, propertyId));
        final Transaction envTxn = txn.getEnvironmentTransaction();
        primaryStore.put(envTxn, key, value);
        final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
        boolean success;
        if (oldValue == null) {
            success = allPropsIndex.put(envTxn, IntegerBinding.intToCompressedEntry(propertyId), secondaryValue);
        } else {
            success = deleteFromCursorAndClose(
                    valueIdx.openCursor(envTxn), createSecondaryKey(store.getPropertyTypes(), oldValue, type), secondaryValue);
        }
        if (success) {
            valueIdx.put(envTxn, createSecondaryKey(store.getPropertyTypes(), value, type), secondaryValue);
        }
        checkStatus(success, "Failed to put");
    }

    public void delete(@NotNull final PersistentStoreTransaction txn, final long localId,
                       @NotNull final ByteIterable value, final int propertyId, @Nullable final PropertyType type) {
        checkStatus(deleteNoFail(txn, localId, value, propertyId, type), "Failed to delete");
    }

    public boolean deleteNoFail(@NotNull final PersistentStoreTransaction txn, final long localId,
                                @NotNull final ByteIterable value, int propertyId, @Nullable final PropertyType type) {
        final ByteIterable key = PropertyKey.propertyKeyToEntry(new PropertyKey(localId, propertyId));
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
        return primaryStore.delete(envTxn, key) &&
                deleteFromCursorAndClose(getOrCreateValueIndex(txn, propertyId).openCursor(envTxn),
                        createSecondaryKey(store.getPropertyTypes(), value, type), secondaryValue) &&
                deleteFromCursorAndClose(allPropsIndex.openCursor(envTxn),
                        IntegerBinding.intToCompressedEntry(propertyId), secondaryValue);
    }

    public Store getPrimaryIndex() {
        return primaryStore;
    }

    public Store getAllPropsIndex() {
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
            return new ArrayList<Map.Entry<Integer, Store>>(valueIndexes.entrySet());
        }
    }

    public static ByteIterable createSecondaryKey(@NotNull final PropertyTypes propertyTypes,
                                                  @NotNull final ByteIterable value,
                                                  @Nullable final PropertyType type) {
        if (type == null) {
            return value;
        }
        if (type.getTypeId() == PropertyType.STRING_PROPERTY_TYPE) {
            final PropertyValue propValue = propertyTypes.entryToPropertyValue(value);
            return new PropertyValue(type, ((String) propValue.getData()).toLowerCase()).dataToEntry();
        }
        // TODO: simplify bindings and replace this hack with CompoundByteIterable in direct value instead
        return new FixedLengthByteIterable(value, 1, value.getLength() - 1); // skip property type
    }

    private String valueIndexName(final int propertyId) {
        return name + PROP_VALUE_IDX + propertyId;
    }

    private static boolean deleteFromCursorAndClose(@NotNull final Cursor cursor,
                                                    @NotNull final ByteIterable key,
                                                    @NotNull final ByteIterable value) {
        try {
            final boolean found = cursor.getSearchBoth(key, value);
            if (!found) {
                cursor.getSearchBoth(key, value);
                return false;
            }
            final boolean deleted = cursor.deleteCurrent();
            if (!deleted) {
                cursor.deleteCurrent();
                return false;
            }
            return true;
        } finally {
            cursor.close();
        }
    }

    @Override
    public boolean canBeCached() {
        return !primaryStore.getConfig().temporaryEmpty && !allPropsIndex.getConfig().temporaryEmpty;
    }
}
