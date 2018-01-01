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
package jetbrains.exodus.query;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.NanoSet;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashMap;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import jetbrains.exodus.entitystore.tables.SingleColumnTable;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.query.metadata.Index;
import jetbrains.exodus.query.metadata.IndexField;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UniqueKeyIndicesEngine {

    private static final Logger logger = LoggerFactory.getLogger(UniqueKeyIndicesEngine.class);

    @NonNls
    private static final String UNIQUEKEY_INDEX = "uniquekey.index";

    private final PersistentEntityStoreImpl persistentStore;

    UniqueKeyIndicesEngine(final PersistentEntityStoreImpl persistentStore) {
        this.persistentStore = persistentStore;
    }

    public void updateUniqueKeyIndices(@NotNull final Iterable<Index> indices) {
        final Environment environment = persistentStore.getEnvironment();
        environment.suspendGC();
        try {
            persistentStore.executeInTransaction(new StoreTransactionalExecutable() {
                @Override
                public void execute(@NotNull StoreTransaction txn) {
                    final PersistentStoreTransaction t = (PersistentStoreTransaction) txn;
                    final PersistentStoreTransaction snapshot = t.getSnapshot();
                    try {
                        final Collection<String> indexNames = new HashSet<>();
                        for (final String dbName : environment.getAllStoreNames(t.getEnvironmentTransaction())) {
                            if (isUniqueKeyIndexName(dbName)) {
                                indexNames.add(dbName);
                            }
                        }
                        for (final Index index : indices) {
                            final String indexName = getUniqueKeyIndexName(index);
                            if (indexNames.contains(indexName)) {
                                indexNames.remove(indexName);
                            } else {
                                createUniqueKeyIndex(t, snapshot, index);
                            }
                        }
                        // remove obsolete indices
                        for (final String indexName : indexNames) {
                            removeObsoleteUniqueKeyIndex(t, indexName);
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("Flush index persistent transaction " + t);
                        }
                        t.flush();
                    } finally {
                        snapshot.abort(); // reading snapshot is obsolete now
                    }
                }
            });
        } finally {
            environment.resumeGC();
        }
    }

    public void insertUniqueKey(@NotNull final PersistentStoreTransaction txn,
                                @NotNull final Index index,
                                @NotNull final List<Comparable> propValues,
                                @NotNull final Entity entity) {
        final PropertyTypes propertyTypes = persistentStore.getPropertyTypes();
        final int propCount = index.getFields().size();
        if (propCount != propValues.size()) {
            throw new IllegalArgumentException("Number of fields differs from the number of property values");
        }
        final Store indexTable = getUniqueKeyIndex(txn, index);
        if (!indexTable.add(txn.getEnvironmentTransaction(), propertyTypes.dataArrayToEntry(propValues.toArray(new Comparable[propCount])),
                LongBinding.longToCompressedEntry(entity.getId().getLocalId()))) {
            throw new InsertConstraintException("Failed to insert unique key (already exists). Index: " + index);
        }
    }

    public void deleteUniqueKey(@NotNull final PersistentStoreTransaction txn,
                                @NotNull final Index index,
                                @NotNull final List<Comparable> propValues) {
        final PropertyTypes propertyTypes = persistentStore.getPropertyTypes();
        final int propCount = index.getFields().size();
        if (propCount != propValues.size()) {
            throw new IllegalArgumentException("Number of fields differs from the number of property values");
        }
        getUniqueKeyIndex(txn, index).delete(txn.getEnvironmentTransaction(), propertyTypes.dataArrayToEntry(propValues.toArray(new Comparable[propCount])));
    }

    private void removeObsoleteUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn, @NotNull final String indexName) {
        if (logger.isDebugEnabled()) {
            logger.debug("Remove obsolete index [" + indexName + ']');
        }
        persistentStore.getEnvironment().removeStore(indexName, txn.getEnvironmentTransaction());
    }

    private void createUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn,
                                      @NotNull final PersistentStoreTransaction snapshot,
                                      @NotNull final Index index) {
        if (logger.isDebugEnabled()) {
            logger.debug("Create index [" + index + ']');
        }

        final Environment environment = persistentStore.getEnvironment();
        final PersistentEntityStoreConfig config = persistentStore.getConfig();
        final PropertyTypes propertyTypes = persistentStore.getPropertyTypes();

        final List<IndexField> fields = index.getFields();
        final int propCount = fields.size();
        if (propCount == 0) {
            throw new EntityStoreException("Can't create unique key index on empty list of keys.");
        }
        SingleColumnTable indexTable = null;
        Comparable[] props = new Comparable[propCount];
        for (final String entityType : getEntityTypesToIndex(index)) {
            int i = 0;
            for (final Entity entity : snapshot.getAll(entityType)) {
                for (int j = 0; j < propCount; ++j) {
                    final IndexField field = fields.get(j);
                    if (field.isProperty()) {
                        if ((props[j] = persistentStore.getProperty(txn, (PersistentEntity) entity, field.getName())) == null) {
                            throw new EntityStoreException("Can't create unique key index with null property value: " + entityType + '.' + field.getName());
                        }
                    } else {
                        if ((props[j] = entity.getLink(field.getName())) == null) {
                            throw new EntityStoreException("Can't create unique key index with null link: " + entityType + '.' + field.getName());
                        }
                    }
                }
                if (indexTable == null) {
                    final String uniqueKeyIndexName = getUniqueKeyIndexName(index);
                    indexTable = new SingleColumnTable(txn, uniqueKeyIndexName,
                            environment.storeExists(uniqueKeyIndexName, txn.getEnvironmentTransaction()) ?
                                    StoreConfig.USE_EXISTING : StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
                }
                ArrayByteIterable propsEntry = propertyTypes.dataArrayToEntry(props);
                if (!indexTable.getDatabase().add(txn.getEnvironmentTransaction(), propsEntry, LongBinding.longToCompressedEntry(entity.getId().getLocalId()))) {
                    ByteIterable oldEntityIdEntry = indexTable.getDatabase().get(txn.getEnvironmentTransaction(), propsEntry);
                    assert oldEntityIdEntry != null;
                    long oldEntityId = LongBinding.compressedEntryToLong(oldEntityIdEntry);
                    throw new EntityStoreException("Failed to insert unique key (already exists), index: " + index + ", values = " + Arrays.toString(props) + ", new entity = " + entity + ", old entity id = " + oldEntityId + ", index owner entity type = " + index.getOwnerEntityType());
                }
                if (++i % 100 == 0) {
                    txn.flush();
                }
            }
            txn.flush();
        }
    }

    protected Set<String> getEntityTypesToIndex(@NotNull Index index) {
        return new NanoSet<>(index.getOwnerEntityType());
    }

    @NotNull
    private Store getUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn, @NotNull final Index index) {
        return persistentStore.getEnvironment().openStore(getUniqueKeyIndexName(index), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn.getEnvironmentTransaction());
    }

    private String getUniqueKeyIndexName(@NotNull final Index index) {
        final List<IndexField> fields = index.getFields();
        final int fieldCount = fields.size();
        if (fieldCount < 1) {
            throw new EntityStoreException("Can't define unique key on empty set of fields");
        }
        final LinkedHashMap<String, Boolean> names = new LinkedHashMap<>();
        for (final IndexField field : fields) {
            final String name = field.getName();
            final Boolean b = names.get(name);
            if (b != null && b == field.isProperty()) {
                throw new EntityStoreException("Can't define unique key, field is used twice: " + name);
            }
            names.put(name, field.isProperty());
        }
        return getUniqueKeyIndexName(index.getOwnerEntityType(), names);
    }

    @NotNull
    private String getUniqueKeyIndexName(final String prefix, LinkedHashMap<String, Boolean> fieldNames) {
        final List<String> params = new ArrayList<>();
        for (final Map.Entry<String, Boolean> fieldEntry : fieldNames.entrySet()) {
            final String name = fieldEntry.getKey();
            params.add(fieldEntry.getValue() ? name : name + "@link");
        }
        return getFQName(UNIQUEKEY_INDEX + prefix, params.toArray());
    }

    private boolean isUniqueKeyIndexName(final String indexName) {
        final int prefixLen = persistentStore.getName().length() + 1;
        return indexName.length() > prefixLen && indexName.substring(prefixLen).startsWith(UNIQUEKEY_INDEX);
    }

    @NotNull
    private String getFQName(@NotNull final String localName, Object... params) {
        final StringBuilder builder = new StringBuilder();
        builder.append(persistentStore.getName());
        builder.append('.');
        builder.append(localName);
        for (final Object param : params) {
            builder.append('#');
            builder.append(param);
        }
        return builder.toString();
    }
}
