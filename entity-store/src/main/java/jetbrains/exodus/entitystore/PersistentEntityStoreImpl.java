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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.ComparableValueType;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashMap;
import jetbrains.exodus.entitystore.iterate.*;
import jetbrains.exodus.entitystore.management.EntityStoreConfig;
import jetbrains.exodus.entitystore.management.EntityStoreStatistics;
import jetbrains.exodus.entitystore.metadata.Index;
import jetbrains.exodus.entitystore.metadata.IndexField;
import jetbrains.exodus.entitystore.tables.*;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.CompoundByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import jetbrains.exodus.util.UTFUtil;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "UnusedDeclaration", "ThisEscapedInObjectConstruction", "VolatileLongOrDoubleField", "ObjectAllocationInLoop", "ReuseOfLocalVariable", "rawtypes"})
public class PersistentEntityStoreImpl implements PersistentEntityStore, FlushLog.Member {

    private static final Logger logger = LoggerFactory.getLogger(PersistentEntityStoreImpl.class);

    @NonNls
    static final String BLOBS_DIR = "blobs";
    @NonNls
    static final String BLOBS_EXTENSION = ".blob";
    @NonNls
    static final String BLOB_HANDLES_SEQUENCE = "blob.handles.sequence";
    @NonNls
    private static final String SEQUENCES_STORE = "sequences";
    private static final long EMPTY_BLOB_HANDLE = Long.MAX_VALUE;
    private static final long IN_PLACE_BLOB_HANDLE = EMPTY_BLOB_HANDLE - 1;
    private static final int ENTITY_ID_CACHE_SIZE = 2047;

    @NotNull
    private static final ByteArrayInputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);
    @NotNull
    private static final String EMPTY_STRING = "";

    private final int hashCode;
    @NotNull
    private final PersistentEntityStoreConfig config;
    @NotNull
    private final String name;
    @NotNull
    private final Environment environment;
    @NotNull
    private final String location;
    @NotNull
    private final Map<Thread, Deque<PersistentStoreTransaction>> txns = new ConcurrentHashMap<>(4, 0.75f, 4);

    @NotNull
    private final StoreNamingRules namingRulez;
    @NotNull
    private BlobVault blobVault;

    @NotNull
    private final Map<String, PersistentSequence> allSequences;
    @NotNull
    private final IntHashMap<PersistentSequence> entitiesSequences;

    @NotNull
    private final PersistentSequentialDictionary entityTypes;
    @NotNull
    private final PersistentSequentialDictionary propertyIds;
    @NotNull
    private final PersistentSequentialDictionary linkIds;

    @NotNull
    private final PropertyTypes propertyTypes;
    @NotNull
    private final PersistentSequentialDictionary propertyCustomTypeIds;

    @NotNull
    private final OpenTablesCache entitiesTables;
    @NotNull
    private final OpenTablesCache propertiesTables;
    @NotNull
    private final OpenTablesCache linksTables;
    @NotNull
    private final OpenTablesCache blobsTables;
    @NotNull
    private final OpenTablesCache entitiesHistoryTables;
    @NotNull
    private final OpenTablesCache propertiesHistoryTables;
    @NotNull
    private final OpenTablesCache linksHistoryTables;
    @NotNull
    private final OpenTablesCache blobsHistoryTables;
    @NotNull
    private final Store internalSettings;
    @NotNull
    private Store sequences;

    @NotNull
    private final EntityIterableCacheImpl iterableCache;
    @NotNull
    private final ConcurrentObjectCache<String, EntityId> entityIdCache; // this cache doesn't need snapshot isolation
    private Explainer explainer;

    private final DataGetter propertyDataGetter;
    private final DataGetter linkDataGetter;
    private final DataGetter blobDataGetter;

    @NotNull
    private final PersistentEntityStoreStatistics statistics;
    @Nullable
    private final EntityStoreConfig configMBean;
    @Nullable
    private final EntityStoreStatistics statisticsMBean;
    @NotNull
    private final PersistentEntityStoreSettingsListener entityStoreSettingsListener;

    @NotNull
    private final Set<TableCreationOperation> tableCreationLog = new HashSet<>();

    public PersistentEntityStoreImpl(@NotNull final Environment environment, @NotNull final String name) throws Exception {
        this(environment, null, name);
    }

    public PersistentEntityStoreImpl(@NotNull final Environment environment,
                                     @Nullable final BlobVault blobVault,
                                     @NotNull final String name) {
        this(new PersistentEntityStoreConfig(), environment, blobVault, name);
    }

    public PersistentEntityStoreImpl(@NotNull final PersistentEntityStoreConfig config,
                                     @NotNull final Environment environment,
                                     @Nullable final BlobVault blobVault,
                                     @NotNull final String name) {
        hashCode = System.identityHashCode(this);
        this.config = config;
        this.environment = environment;
        PersistentEntityStores.adjustEnvironmentConfigForEntityStore(environment.getEnvironmentConfig());
        this.name = name;
        location = environment.getLocation();
        namingRulez = new StoreNamingRules(name);
        iterableCache = new EntityIterableCacheImpl(this);
        entityIdCache = new ConcurrentObjectCache<>(ENTITY_ID_CACHE_SIZE);
        explainer = new Explainer(config.isExplainOn());
        propertyDataGetter = new PropertyDataGetter();
        linkDataGetter = config.isDebugLinkDataGetter() ? new DebugLinkDataGetter() : new LinkDataGetter();
        blobDataGetter = new BlobDataGetter();
        allSequences = new HashMap<>();

        final PersistentStoreTransaction txn = beginTransaction();
        sequences = environment.openStore(SEQUENCES_STORE, StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction());
        final boolean fromScratch;
        try {
            this.blobVault = blobVault == null ? createDefaultFSBlobVault() : blobVault;
            this.blobVault.setStringContentCacheSize(config.getBlobStringsCacheSize());

            entitiesSequences = new IntHashMap<>();
            final TwoColumnTable entityTypesTable = new TwoColumnTable(txn,
                    namingRulez.getEntityTypesTableName(), StoreConfig.WITHOUT_DUPLICATES);
            final PersistentSequence entityTypesSequence = getSequence(txn, namingRulez.getEntityTypesSequenceName());
            entityTypes = new PersistentSequentialDictionary(entityTypesSequence, entityTypesTable) {
                @Override
                protected void created(final PersistentStoreTransaction txn, final int id) {
                    preloadTables(txn, id);
                }
            };
            propertyIds = new PersistentSequentialDictionary(getSequence(txn, namingRulez.getPropertyIdsSequenceName()),
                    new TwoColumnTable(txn, namingRulez.getPropertyIdsTableName(), StoreConfig.WITHOUT_DUPLICATES));
            linkIds = new PersistentSequentialDictionary(getSequence(txn, namingRulez.getLinkIdsSequenceName()),
                    new TwoColumnTable(txn, namingRulez.getLinkIdsTableName(), StoreConfig.WITHOUT_DUPLICATES));

            propertyTypes = new PropertyTypes();
            propertyCustomTypeIds = new PersistentSequentialDictionary(getSequence(txn, namingRulez.getPropertyCustomTypesSequence()),
                    new TwoColumnTable(txn, namingRulez.getPropertyCustomTypesTable(), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING));

            entitiesTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new SingleColumnTable(txn,
                            namingRulez.getEntitiesTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
                }
            });
            entitiesHistoryTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new SingleColumnTable(txn,
                            namingRulez.getEntitiesHistoryTableName(entityTypeId), StoreConfig.WITH_DUPLICATES);
                }
            });
            propertiesTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new PropertiesTable(txn,
                            namingRulez.getPropertiesTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
                }
            });
            propertiesHistoryTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new SingleColumnTable(txn,
                            namingRulez.getPropertiesHistoryTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
                }
            });
            linksTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new TwoColumnTable(txn,
                            namingRulez.getLinksTableName(entityTypeId), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING);
                }
            });
            linksHistoryTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, int entityTypeId) {
                    return new SingleColumnTable(txn,
                            namingRulez.getLinksHistoryTableName(entityTypeId), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING);
                }
            });
            blobsTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new BlobsTable(PersistentEntityStoreImpl.this, txn,
                            namingRulez.getBlobsTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
                }
            });
            blobsHistoryTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                @NotNull
                @Override
                public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                    return new SingleColumnTable(txn,
                            namingRulez.getBlobsHistoryTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
                }
            });

            final String internalSettingsName = namingRulez.getInternalSettingsName();
            final Store settings = environment.openStore(internalSettingsName,
                    StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction(), false);
            fromScratch = settings == null;
            if (fromScratch) {
                internalSettings = environment.openStore(internalSettingsName,
                        StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction(), true);
            } else {
                internalSettings = settings;
            }
            txn.flush();
        } catch (IOException e) {
            throw ExodusException.toEntityStoreException(e);
        } finally {
            txn.abort();
        }

        if (!config.getRefactoringSkipAll()) {
            applyRefactorings(fromScratch); // this method includes refactorings that could be clustered into separate txns
        }

        final PersistentStoreTransaction preloadTxn = beginTransaction();
        try {
            preloadTables(preloadTxn); // this is called to pre-load tables for all entity types to prevent them from being lazy loaded
        } finally {
            preloadTxn.commit();
        }

        statistics = new PersistentEntityStoreStatistics(this);
        if (config.isManagementEnabled()) {
            configMBean = new EntityStoreConfig(this);
            // if we don't gather statistics then we should not expose corresponding managed bean
            statisticsMBean = config.getGatherStatistics() ? new EntityStoreStatistics(this) : null;
        } else {
            configMBean = null;
            statisticsMBean = null;
        }
        entityStoreSettingsListener = new PersistentEntityStoreSettingsListener(this);
        config.addChangedSettingsListener(entityStoreSettingsListener);

        if (logger.isDebugEnabled()) {
            logger.debug("Created successfully.");
        }
    }

    private void applyRefactorings(final boolean fromScratch) {
        environment.suspendGC();
        try {
            final PersistentEntityStoreRefactorings refactorings = new PersistentEntityStoreRefactorings(this);
            if (config.getRefactoringDeleteRedundantBlobs()) {
                refactorings.refactorDeleteRedundantBlobs();
            }
            if (fromScratch || Settings.get(internalSettings, "Entities' stores key-prefixed") == null) {
                if (!fromScratch) {
                    refactorings.refactorEntitiesTables();
                }
                Settings.set(internalSettings, "Entities' stores key-prefixed", "yes");
            }
            if (fromScratch || Settings.get(internalSettings, "Null-indices present 2") == null || config.getRefactoringNullIndices()) {
                if (!fromScratch) {
                    Settings.delete(internalSettings, "Null-indices present"); // don't waste space
                    refactorings.refactorCreateNullPropertyIndices();
                }
                Settings.set(internalSettings, "Null-indices present 2", "yes");
            }
            if (fromScratch || Settings.get(internalSettings, "Blobs' null-indices present") == null || config.getRefactoringBlobNullIndices()) {
                if (!fromScratch) {
                    refactorings.refactorCreateNullBlobIndices();
                }
                Settings.set(internalSettings, "Blobs' null-indices present", "yes");
            }
            if (fromScratch || Settings.get(internalSettings, "Links consistency fixed") == null || config.getRefactoringHeavyLinks()) {
                if (!fromScratch) {
                    refactorings.refactorMakeLinkTablesConsistent();
                }
                Settings.set(internalSettings, "Links consistency fixed", "yes");
            }
            if (fromScratch || Settings.get(internalSettings, "Props consistency fixed") == null || config.getRefactoringHeavyProps()) {
                if (!fromScratch) {
                    refactorings.refactorMakePropTablesConsistent();
                }
                Settings.set(internalSettings, "Props consistency fixed", "yes");
            }
            if (blobVault instanceof VFSBlobVault && new File(location, BLOBS_DIR).exists()) {
                try {
                    ((VFSBlobVault) blobVault).refactorFromFS(this);
                } catch (IOException e) {
                    throw ExodusException.toEntityStoreException(e);
                }
            }
            if (blobVault instanceof FileSystemBlobVaultOld &&
                    !(blobVault instanceof FileSystemBlobVault) &&
                    config.getMaxInPlaceBlobSize() > 0) {
                refactorings.refactorInPlaceBlobs((FileSystemBlobVaultOld) blobVault, BLOB_HANDLES_SEQUENCE);
            }
        } finally {
            environment.resumeGC();
        }
    }

    private BlobVault createDefaultFSBlobVault() throws IOException {
        final PersistentSequence sequence = getSequence(getAndCheckCurrentTransaction(), BLOB_HANDLES_SEQUENCE);
        FileSystemBlobVaultOld blobVault;
        try {
            blobVault = new FileSystemBlobVault(location, BLOBS_DIR, BLOBS_EXTENSION, new PersistentSequenceBlobHandleGenerator(sequence));
        } catch (UnexpectedBlobVaultVersionException e) {
            blobVault = null;
        }
        if (blobVault == null) {
            if (config.getMaxInPlaceBlobSize() > 0) {
                blobVault = new FileSystemBlobVaultOld(location, BLOBS_DIR, BLOBS_EXTENSION, BlobHandleGenerator.IMMUTABLE);
            } else {
                blobVault = new FileSystemBlobVaultOld(location, BLOBS_DIR, BLOBS_EXTENSION, new PersistentSequenceBlobHandleGenerator(sequence));
            }
        }
        final long current = sequence.get();
        for (long blobHandle = current + 1; blobHandle < current + 1000; ++blobHandle) {
            final File file = blobVault.getBlobLocation(blobHandle);
            if (file.exists()) {
                logger.error("Redundant blob file: " + file);
            }
        }
        return blobVault;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    @NotNull
    public PersistentStoreTransaction beginTransaction() {
        final PersistentStoreTransaction txn = createTxn();
        registerTransaction(txn);
        return txn;
    }

    @NotNull
    public PersistentStoreTransaction beginReadonlyTransaction() {
        final PersistentStoreTransaction txn = new ReadonlyPersistentStoreTransaction(this);
        registerTransaction(txn);
        return txn;
    }

    protected PersistentStoreTransaction createTxn() {
        return new PersistentStoreTransaction(this);
    }

    @Override
    @Nullable
    public PersistentStoreTransaction getCurrentTransaction() {
        final Thread thread = Thread.currentThread();
        final Deque<PersistentStoreTransaction> stack = txns.get(thread);
        return stack == null ? null : stack.peek();
    }

    @NotNull
    public PersistentStoreTransaction getAndCheckCurrentTransaction() {
        final PersistentStoreTransaction transaction = getCurrentTransaction();
        if (transaction == null) {
            throw new IllegalStateException("EntityStore: current transaction is not set.");
        }
        return transaction;
    }

    void registerTransaction(@NotNull final PersistentStoreTransaction txn) {
        final Thread thread = Thread.currentThread();
        Deque<PersistentStoreTransaction> stack = txns.get(thread);
        if (stack == null) {
            stack = new ArrayDeque<>(4);
            txns.put(thread, stack);
        }
        stack.push(txn);
    }

    void unregisterTransaction(@NotNull final PersistentStoreTransaction txn) {
        final Thread thread = Thread.currentThread();
        final Deque<PersistentStoreTransaction> stack = txns.get(thread);
        if (stack == null) {
            throw new EntityStoreException("Transaction was already finished");
        }
        if (txn != stack.peek()) {
            throw new EntityStoreException("Can't finish transaction: nested transaction is not finished");
        }
        stack.pop();
        if (stack.isEmpty()) {
            txns.remove(thread);
        }
        txn.closeCaches();
    }

    @Override
    public void clear() {
        environment.clear();
    }

    @Override
    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    @Override
    public PersistentEntityStoreConfig getConfig() {
        return config;
    }

    @Override
    @NotNull
    public String getLocation() {
        return location;
    }

    @Override
    @NotNull
    public Environment getEnvironment() {
        return environment;
    }

    @NotNull
    public PersistentSequence getSequence(@NotNull final PersistentStoreTransaction txn, @NotNull final String sequenceName) {
        synchronized (allSequences) {
            PersistentSequence result = allSequences.get(sequenceName);
            if (result == null) {
                result = new PersistentSequence(txn, sequences, sequenceName);
                allSequences.put(sequenceName, result);
            }
            return result;
        }
    }

    public List<PersistentSequence> getAllSequences() {
        synchronized (allSequences) {
            return new ArrayList<>(allSequences.values());
        }
    }

    @Override
    public long getUsableSpace() {
        return new File(location).getUsableSpace();
    }

    @Override
    @NotNull
    public BlobVault getBlobVault() {
        return blobVault;
    }

    void setBlobVault(@NotNull final BlobVault blobVault) {
        this.blobVault = blobVault;
    }

    @Override
    public void registerCustomPropertyType(@NotNull final StoreTransaction txn,
                                           @NotNull final Class<? extends Comparable> clazz,
                                           @NotNull final ComparableBinding binding) {
        propertyTypes.registerCustomPropertyType(
                propertyCustomTypeIds.getOrAllocateId((PersistentStoreTransaction) txn, clazz.getName()), clazz, binding);
    }

    @Override
    public void executeInTransaction(@NotNull final StoreTransactionalExecutable executable) {
        PersistentStoreTransaction txn = beginTransaction();
        try {
            do {
                executable.execute(txn);
                // if txn has already been aborted in execute()
                if (txn != getCurrentTransaction()) {
                    txn = null;
                    break;
                }

            } while (!txn.flush());
        } finally {
            // if txn has not already been aborted in execute()
            if (txn != null) {
                txn.abort();
            }
        }
    }

    @Override
    public void executeInReadonlyTransaction(@NotNull StoreTransactionalExecutable executable) {
        final PersistentStoreTransaction txn = beginReadonlyTransaction();
        try {
            executable.execute(txn);
        } finally {
            // if txn has not already been aborted in execute()
            if (txn == getCurrentTransaction()) {
                txn.abort();
            }
        }
    }

    @Override
    public <T> T computeInTransaction(@NotNull final StoreTransactionalComputable<T> computable) {
        T result;
        PersistentStoreTransaction txn = beginTransaction();
        try {
            do {
                result = computable.compute(txn);
                // if txn has already been aborted in compute()
                if (txn != getCurrentTransaction()) {
                    txn = null;
                    break;
                }
            } while (!txn.flush());
        } finally {
            // if txn has not already been aborted in compute()
            if (txn != null) {
                txn.abort();
            }
        }
        return result;
    }

    @Override
    public <T> T computeInReadonlyTransaction(@NotNull StoreTransactionalComputable<T> computable) {
        final PersistentStoreTransaction txn = beginReadonlyTransaction();
        try {
            return computable.compute(txn);
        } finally {
            // if txn has not already been aborted in compute()
            if (txn == getCurrentTransaction()) {
                txn.abort();
            }
        }
    }

    @Override
    public Explainer getExplainer() {
        return explainer;
    }

    @Override
    @NotNull
    public EntityIterableCacheImpl getEntityIterableCache() {
        return iterableCache;
    }

    @Nullable
    public EntityId getCachedEntityId(@NotNull final String representation) {
        return entityIdCache.tryKey(representation);
    }

    public void cacheEntityId(@NotNull final String representation, @NotNull final EntityId id) {
        entityIdCache.cacheObject(representation, id);
    }

    @Nullable
    Comparable getProperty(@NotNull final PersistentStoreTransaction txn,
                           @NotNull final PersistentEntity entity,
                           @NotNull final String propertyName) {
        final int propertyId = getPropertyId(txn, propertyName, false);
        if (propertyId < 0) {
            return null;
        }
        Comparable result = txn.getCachedProperty(entity, propertyId);
        if (result == null) {
            final ByteIterable resultEntry = getRawProperty(txn, entity, propertyId);
            if (resultEntry != null) {
                final PropertyValue propValue = propertyTypes.entryToPropertyValue(resultEntry);
                result = propValue.getData();
                if (propValue.getType().getTypeId() != ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
                    txn.cacheProperty(entity.getId(), propertyId, result);
                }
            }
        }
        return result;
    }

    @Nullable
    public ByteIterable getRawProperty(@NotNull final PersistentStoreTransaction txn,
                                       @NotNull final PersistentEntity entity,
                                       final int propertyId) {
        return getRawValue(txn, entity, propertyId, propertyDataGetter);
    }

    public boolean setProperty(@NotNull final PersistentStoreTransaction txn,
                               @NotNull final PersistentEntity entity,
                               @NotNull final String propertyName,
                               @NotNull final Comparable value) {
        final int propertyId = getPropertyId(txn, propertyName, true);
        final ByteIterable oldValueEntry = getRawProperty(txn, entity, propertyId);
        final Comparable oldValue = oldValueEntry == null ? null : propertyTypes.entryToPropertyValue(oldValueEntry).getData();

        if (value.equals(oldValue)) { // value is not null by contract
            return false;
        }

        final PersistentEntityId entityId = entity.getId();
        final PropertyValue propValue = propertyTypes.dataToPropertyValue(value);
        getPropertiesTable(txn, entityId.getTypeId()).put(
                txn, entityId.getLocalId(), PropertyTypes.propertyValueToEntry(propValue), oldValueEntry, propertyId, propValue.getType());
        txn.propertyChanged(entityId, propertyId, oldValue, value);

        return true;
    }

    public boolean deleteProperty(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity, @NotNull final String propertyName) {
        final int propertyId = getPropertyId(txn, propertyName, false);
        if (propertyId < 0) {
            return false;
        }
        final ByteIterable oldValue = getRawProperty(txn, entity, propertyId);
        if (oldValue == null) {
            return false;
        }
        final EntityId id = entity.getId();
        final PropertyValue propValue = propertyTypes.entryToPropertyValue(oldValue);
        getPropertiesTable(txn, id.getTypeId()).delete(txn, id.getLocalId(),
                oldValue, propertyId, propValue.getType());
        txn.propertyChanged((PersistentEntityId) id, propertyId, propValue.getData(), null);

        return true;
    }

    @NotNull
    public List<String> getPropertyNames(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final List<String> result = new ArrayList<>();
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
        try (Cursor index = getPrimaryPropertyIndexCursor(txn, id.getTypeId())) {
            for (boolean success = index.getSearchKeyRange(PropertyKey.propertyKeyToEntry(propertyKey)) != null;
                 success; success = index.getNext()) {
                propertyKey = PropertyKey.entryToPropertyKey(index.getKey());
                if (propertyKey.getEntityLocalId() != entityLocalId) {
                    break;
                }
                final String propertyName = getPropertyName(txn, propertyKey.getPropertyId());
                if (propertyName != null) {
                    result.add(propertyName);
                }
            }
            return result;
        }
    }

    @NotNull
    public List<Pair<String, Comparable>> getProperties(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final List<Pair<String, Comparable>> result = new ArrayList<>();
        final EntityId fromId = entity.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        Cursor cursor = null;
        try {
            if (entity.isUpToDate()) {
                PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
                cursor = getPrimaryPropertyIndexCursor(txn, entityTypeId);
                for (boolean success = cursor.getSearchKeyRange(PropertyKey.propertyKeyToEntry(propertyKey)) != null;
                     success; success = cursor.getNext()) {
                    propertyKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                    if (propertyKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String propertyName = getPropertyName(txn, propertyKey.getPropertyId());
                    if (propertyName != null) {
                        result.add(new Pair<>(
                                propertyName, propertyTypes.entryToPropertyValue(cursor.getValue()).getData()));
                    }
                }
            } else {
                final int version = entity.getVersion();
                PropertyHistoryKey propertyHistoryKey = new PropertyHistoryKey(entityLocalId, version, 0);
                cursor = getPropertiesHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction());
                for (boolean success = cursor.getSearchKeyRange(PropertyHistoryKey.propertyHistoryKeyToEntry(propertyHistoryKey)) != null;
                     success; success = cursor.getNext()) {
                    propertyHistoryKey = PropertyHistoryKey.entryToPropertyHistoryKey(cursor.getKey());
                    if (propertyHistoryKey.getVersion() != version || propertyHistoryKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String propertyName = getPropertyName(txn, propertyHistoryKey.getPropertyId());
                    if (propertyName != null) {
                        result.add(new Pair<>(
                                propertyName, propertyTypes.entryToPropertyValue(cursor.getValue()).getData()));
                    }
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @NotNull
    public Cursor getPrimaryPropertyIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getPrimaryPropertyIndexCursor(txn, getPropertiesTable(txn, entityTypeId));
    }

    @NotNull
    public Cursor getPrimaryPropertyIndexCursor(@NotNull final PersistentStoreTransaction txn, @NotNull final PropertiesTable properties) {
        return properties.getPrimaryIndex().openCursor(txn.getEnvironmentTransaction());
    }

    @Nullable
    public Cursor getPropertyValuesIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId, final int propertyId) {
        final Store valueIdx = getPropertiesTable(txn, entityTypeId).getValueIndex(txn, propertyId, false);
        if (valueIdx == null) {
            return null;
        }
        return valueIdx.openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getEntityWithPropCursor(@NotNull final PersistentStoreTransaction txn, int entityTypeId) {
        return getPropertiesTable(txn, entityTypeId).getAllPropsIndex().openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getEntityWithBlobCursor(@NotNull final PersistentStoreTransaction txn, int entityTypeId) {
        return getBlobsTable(txn, entityTypeId).getAllBlobsIndex().openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getEntitiesIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getEntitiesTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getLinksHistoryIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getLinksHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getLinksFirstIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getLinksTable(txn, entityTypeId).getFirstIndexCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getLinksSecondIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getLinksTable(txn, entityTypeId).getSecondIndexCursor(txn.getEnvironmentTransaction());
    }

    /**
     * Clears all properties of specified entity.
     *
     * @param entity to clear.
     */
    @SuppressWarnings({"OverlyLongMethod"})
    public void clearProperties(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final PersistentEntityId id = (PersistentEntityId) entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        if (entity.isUpToDate()) { // up-to-date entity
            final PropertiesTable properties = getPropertiesTable(txn, entityTypeId);
            final PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
            try (Cursor cursor = getPrimaryPropertyIndexCursor(txn, properties)) {
                for (boolean success = cursor.getSearchKeyRange(PropertyKey.propertyKeyToEntry(propertyKey)) != null;
                     success; success = cursor.getNext()) {
                    ByteIterable keyEntry = cursor.getKey();
                    final PropertyKey key = PropertyKey.entryToPropertyKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final int propertyId = key.getPropertyId();
                    final ByteIterable value = cursor.getValue();
                    final PropertyValue propValue = propertyTypes.entryToPropertyValue(value);
                    txn.propertyChanged(id, propertyId, propValue.getData(), null);
                    properties.deleteNoFail(txn, entityLocalId, value, propertyId, propValue.getType());
                }
            }
        } else { // historical entity
            final int version = entity.getVersion();
            final Store properties = getPropertiesHistoryTable(txn, entityTypeId);
            final PropertyHistoryKey propertyKey = new PropertyHistoryKey(entityLocalId, version, 0);
            ByteIterable keyEntry = PropertyHistoryKey.propertyHistoryKeyToEntry(propertyKey);
            try (Cursor cursor = properties.openCursor(envTxn)) {
                for (boolean success = cursor.getSearchKeyRange(keyEntry) != null; success; success = cursor.getNext()) {
                    keyEntry = cursor.getKey();
                    final PropertyHistoryKey key = PropertyHistoryKey.entryToPropertyHistoryKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId || key.getVersion() != version) {
                        break;
                    }
                    properties.delete(envTxn, keyEntry);
                }
            }
        }
    }

    public long getBlobSize(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity, @NotNull final String blobName) throws IOException {
        final Pair<Long, ByteIterator> blobInfo = getBlobHandleAndValue(txn, entity, blobName);
        if (blobInfo == null) {
            return -1;
        }
        final long blobHandle = blobInfo.getFirst();
        if (blobHandle == EMPTY_BLOB_HANDLE) {
            return 0;
        }
        if (blobHandle == IN_PLACE_BLOB_HANDLE) {
            return CompressedUnsignedLongByteIterable.getLong(blobInfo.getSecond());
        }
        final long result = txn.getBlobSize(blobHandle);
        if (result < 0) {
            return blobVault.getSize(blobHandle, txn.getEnvironmentTransaction());
        }
        return result;
    }

    @Nullable
    public InputStream getBlob(@NotNull final PersistentStoreTransaction txn,
                               @NotNull final PersistentEntity entity,
                               @NotNull final String blobName) throws IOException {
        final Pair<Long, InputStream> blobStream = getInPlaceBlobStream(txn, entity, blobName);
        if (blobStream == null) {
            return null;
        }
        final long blobHandle = blobStream.getFirst();
        if (blobHandle == EMPTY_BLOB_HANDLE) {
            return EMPTY_INPUT_STREAM;
        }
        final InputStream result = blobStream.getSecond();
        return result != null ? result : blobVault.getContent(blobHandle, txn.getEnvironmentTransaction());
    }

    @Nullable
    public String getBlobString(@NotNull final PersistentStoreTransaction txn,
                                @NotNull final PersistentEntity entity,
                                @NotNull final String blobName) throws IOException {
        final int blobId = getPropertyId(txn, blobName, false);
        if (blobId < 0) {
            return null;
        }
        String result = txn.getCachedBlobString(entity, blobId);
        if (result != null) {
            return result;
        }
        final Pair<Long, InputStream> blobStream = getInPlaceBlobStream(txn, entity, blobName);
        if (blobStream == null) {
            return null;
        }
        final long blobHandle = blobStream.getFirst();
        if (blobHandle == EMPTY_BLOB_HANDLE) {
            result = EMPTY_STRING;
        } else {
            try {
                final InputStream stream = blobStream.getSecond();
                if (stream == null) {
                    return blobVault.getStringContent(blobHandle, txn.getEnvironmentTransaction());
                }
                result = UTFUtil.readUTF(stream);
            } catch (UTFDataFormatException e) {
                result = e.toString();
            }
        }
        if (result != null) {
            txn.cacheBlobString(entity, blobId, result);
        }
        return result;
    }

    @Nullable
    Pair<Long, ByteIterator> getBlobHandleAndValue(@NotNull final PersistentStoreTransaction txn,
                                                   @NotNull final PersistentEntity entity,
                                                   @NotNull final String blobName) {
        final int blobId = getPropertyId(txn, blobName, false);
        if (blobId < 0) {
            return null;
        }
        final ByteIterable valueEntry = getRawValue(txn, entity, blobId, blobDataGetter);
        if (valueEntry == null) {
            return null;
        }
        final ByteIterator valueIterator = valueEntry instanceof FixedLengthByteIterable ?
                ((FixedLengthByteIterable) valueEntry).getSource().iterator() : valueEntry.iterator();
        return new Pair<>(LongBinding.readCompressed(valueIterator), valueIterator);
    }

    @Nullable
    private Pair<Long, InputStream> getInPlaceBlobStream(@NotNull final PersistentStoreTransaction txn,
                                                         @NotNull final PersistentEntity entity,
                                                         @NotNull final String blobName) throws IOException {
        final Pair<Long, ByteIterator> blobInfo = getBlobHandleAndValue(txn, entity, blobName);
        if (blobInfo == null) {
            return null;
        }
        final long blobHandle = blobInfo.getFirst();
        if (blobHandle == EMPTY_BLOB_HANDLE) {
            return new Pair<>(blobHandle, null);
        }
        if (blobHandle == IN_PLACE_BLOB_HANDLE) {
            final ByteIterator valueIterator = blobInfo.getSecond();
            final int size = (int) CompressedUnsignedLongByteIterable.getLong(valueIterator);
            return new Pair<Long, InputStream>(blobHandle,
                    new ByteArraySizedInputStream(ByteIterableBase.readIterator(valueIterator, size)));
        }
        return new Pair<>(blobHandle, txn.getBlobStream(blobHandle));
    }

    public void setBlob(@NotNull final PersistentStoreTransaction txn,
                        @NotNull final PersistentEntity entity,
                        @NotNull final String blobName,
                        @NotNull final InputStream stream) throws IOException {
        final ByteArraySizedInputStream copy = stream instanceof ByteArraySizedInputStream ?
                (ByteArraySizedInputStream) stream : blobVault.cloneStream(stream, true);
        final long blobHandle = createBlobHandle(txn, entity, blobName, copy, copy.size());
        if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
            txn.addBlob(blobHandle, copy);
        }
    }

    public void setBlob(@NotNull final PersistentStoreTransaction txn,
                        @NotNull final PersistentEntity entity,
                        @NotNull final String blobName,
                        @NotNull final File file) throws IOException {
        final long length = file.length();
        if (length > Integer.MAX_VALUE) {
            throw new EntityStoreException("Too large blob, size is greater than " + Integer.MAX_VALUE);
        }
        final int size = (int) length;
        final long blobHandle = createBlobHandle(txn, entity, blobName,
                size > config.getMaxInPlaceBlobSize() ? null : blobVault.cloneStream(new FileInputStream(file), true), size);
        if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
            txn.addBlob(blobHandle, file);
        }
    }

    public void setBlobString(@NotNull final PersistentStoreTransaction txn,
                              @NotNull final PersistentEntity entity,
                              @NotNull final String blobName,
                              @NotNull final String blobString) throws IOException {
        final int length = blobString.length();
        if (length == 0) {
            createBlobHandle(txn, entity, blobName, null, 0);
        } else {
            final LightByteArrayOutputStream memCopy = new LightByteArrayOutputStream();
            UTFUtil.writeUTF(memCopy, blobString);
            final int streamSize = memCopy.size();
            final ByteArraySizedInputStream copy = new ByteArraySizedInputStream(memCopy.toByteArray(), 0, streamSize);
            final long blobHandle = createBlobHandle(txn, entity, blobName, copy, streamSize);
            if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
                txn.addBlob(blobHandle, copy);
            }
        }
    }

    private long createBlobHandle(@NotNull final PersistentStoreTransaction txn,
                                  @NotNull final PersistentEntity entity,
                                  @NotNull final String blobName,
                                  @Nullable final ByteArraySizedInputStream stream,
                                  final int size) {
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        final int blobId = getPropertyId(txn, blobName, true);
        txn.invalidateCachedBlobString(entity, blobId);
        final BlobsTable blobs = getBlobsTable(txn, id.getTypeId());
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final ByteIterable value = blobs.get(envTxn, entityLocalId, blobId);
        if (value != null) {
            deleteObsoleteBlobHandle(LongBinding.compressedEntryToLong(value), txn);
        }
        final long blobHandle;
        if (size == 0) {
            blobHandle = EMPTY_BLOB_HANDLE;
            blobs.put(envTxn, entityLocalId, blobId, LongBinding.longToCompressedEntry(blobHandle));
        } else if (size <= config.getMaxInPlaceBlobSize()) {
            if (stream == null) {
                throw new NullPointerException("In-memory blob content is expected");
            }
            blobHandle = IN_PLACE_BLOB_HANDLE;
            blobs.put(envTxn, entityLocalId, blobId,
                    new CompoundByteIterable(new ByteIterable[]{
                            LongBinding.longToCompressedEntry(blobHandle),
                            CompressedUnsignedLongByteIterable.getIterable(size),
                            new ArrayByteIterable(stream.toByteArray(), size)
                    })
            );
        } else {
            blobHandle = blobVault.nextHandle(envTxn);
            blobs.put(envTxn, entityLocalId, blobId, LongBinding.longToCompressedEntry(blobHandle));
        }
        return blobHandle;
    }

    public boolean deleteBlob(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity, @NotNull final String blobName) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final int blobId = getPropertyId(txn, blobName, false);
        if (blobId < 0) {
            return false;
        }
        txn.invalidateCachedBlobString(entity, blobId);
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        final BlobsTable blobs = getBlobsTable(txn, id.getTypeId());
        final ByteIterable value = blobs.get(envTxn, entityLocalId, blobId);
        if (value == null) {
            return false;
        }
        blobs.delete(envTxn, entityLocalId, blobId);
        deleteObsoleteBlobHandle(LongBinding.compressedEntryToLong(value), txn);

        return true;
    }

    @SuppressWarnings({"OverlyLongMethod"})
    public void clearBlobs(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final EntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        if (entity.isUpToDate()) {
            final BlobsTable blobs = getBlobsTable(txn, entityTypeId);
            final PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
            ByteIterable keyEntry = PropertyKey.propertyKeyToEntry(propertyKey);
            try (Cursor cursor = blobs.getPrimaryIndex().openCursor(envTxn)) {
                for (boolean success = cursor.getSearchKeyRange(keyEntry) != null; success; success = cursor.getNext()) {
                    keyEntry = cursor.getKey();
                    final PropertyKey key = PropertyKey.entryToPropertyKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final ByteIterable value = cursor.getValue();
                    final int blobId = key.getPropertyId();
                    blobs.delete(envTxn, entityLocalId, blobId);
                    txn.invalidateCachedBlobString(entity, blobId);
                    deleteObsoleteBlobHandle(LongBinding.compressedEntryToLong(value), txn);
                }
            }
        } else {
            final Store blobs = getBlobsHistoryTable(txn, entityTypeId);
            final int version = entity.getVersion();
            final PropertyHistoryKey propertyKey = new PropertyHistoryKey(entityLocalId, version, 0);
            ByteIterable keyEntry = PropertyHistoryKey.propertyHistoryKeyToEntry(propertyKey);
            try (Cursor cursor = blobs.openCursor(envTxn)) {
                for (boolean success = cursor.getSearchKeyRange(keyEntry) != null; success; success = cursor.getNext()) {
                    keyEntry = cursor.getKey();
                    final PropertyHistoryKey key = PropertyHistoryKey.entryToPropertyHistoryKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId || key.getVersion() != version) {
                        break;
                    }
                    final ByteIterable value = cursor.getValue();
                    blobs.delete(envTxn, keyEntry);
                    deleteObsoleteBlobHandle(LongBinding.compressedEntryToLong(value), txn);
                }
            }
        }
    }

    @NotNull
    public List<String> getBlobNames(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final List<String> result = new ArrayList<>();
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
        try (Cursor index = getBlobsTable(txn, id.getTypeId()).getPrimaryIndex().openCursor(txn.getEnvironmentTransaction())) {
            for (boolean success = index.getSearchKeyRange(PropertyKey.propertyKeyToEntry(propertyKey)) != null; success; success = index.getNext()) {
                propertyKey = PropertyKey.entryToPropertyKey(index.getKey());
                if (propertyKey.getEntityLocalId() != entityLocalId) {
                    break;
                }
                final String propertyName = getPropertyName(txn, propertyKey.getPropertyId());
                if (propertyName != null) {
                    result.add(propertyName);
                }
            }
            return result;
        }
    }

    @NotNull
    public List<Pair<String, Long>> getBlobs(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final List<Pair<String, Long>> result = new ArrayList<>();
        final EntityId fromId = entity.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        Cursor cursor = null;
        try {
            if (entity.isUpToDate()) {
                PropertyKey blobKey = new PropertyKey(entityLocalId, 0);
                cursor = getBlobsTable(txn, entityTypeId).getPrimaryIndex().openCursor(txn.getEnvironmentTransaction());
                for (boolean success = cursor.getSearchKeyRange(PropertyKey.propertyKeyToEntry(blobKey)) != null;
                     success; success = cursor.getNext()) {
                    blobKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                    if (blobKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String blobName = getPropertyName(txn, blobKey.getPropertyId());
                    if (blobName != null) {
                        result.add(new Pair<>(
                                blobName, LongBinding.compressedEntryToLong(cursor.getValue())));
                    }
                }
            } else {
                final int version = entity.getVersion();
                PropertyHistoryKey blobHistoryKey = new PropertyHistoryKey(entityLocalId, version, 0);
                cursor = getBlobsHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction());
                for (boolean success = cursor.getSearchKeyRange(PropertyHistoryKey.propertyHistoryKeyToEntry(blobHistoryKey)) != null;
                     success; success = cursor.getNext()) {
                    blobHistoryKey = PropertyHistoryKey.entryToPropertyHistoryKey(cursor.getKey());
                    if (blobHistoryKey.getVersion() != version || blobHistoryKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String blobName = getPropertyName(txn, blobHistoryKey.getPropertyId());
                    if (blobName != null) {
                        result.add(new Pair<>(
                                blobName, LongBinding.compressedEntryToLong(cursor.getValue())));
                    }
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    boolean addLink(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, @NotNull final PersistentEntity to, final int linkId) {
        final EntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
        final LinkValue linkValue = new LinkValue(to.getId(), linkId);

        if (!getLinksTable(txn, entityTypeId).put(txn.getEnvironmentTransaction(),
                PropertyKey.propertyKeyToEntry(linkKey), LinkValue.linkValueToEntry(linkValue))) {
            return false;
        }

        txn.linkAdded(from.getId(), to.getId(), linkId);

        return true;
    }

    boolean setLink(@NotNull final PersistentStoreTransaction txn,
                    @NotNull final PersistentEntity from,
                    final int linkId,
                    @Nullable final PersistentEntity to) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final PersistentEntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final ByteIterable keyEntry = PropertyKey.propertyKeyToEntry(new PropertyKey(entityLocalId, linkId));
        final TwoColumnTable links = getLinksTable(txn, entityTypeId);
        boolean oldTargetDeleted = false;
        final ByteIterable valueEntry = getRawLink(txn, from, linkId);
        if (valueEntry != null) {
            final PersistentEntity oldTarget = getEntity(LinkValue.entryToLinkValue(valueEntry).getEntityId());
            if (oldTarget.equals(to)) {
                return false;
            }
            links.delete(envTxn, keyEntry, valueEntry);
            txn.linkDeleted(fromId, oldTarget.getId(), linkId);
            oldTargetDeleted = true;
        }
        if (to == null) {
            return oldTargetDeleted;
        }
        final LinkValue linkValue = new LinkValue(to.getId(), linkId);
        links.put(envTxn, keyEntry, LinkValue.linkValueToEntry(linkValue));
        txn.linkAdded(fromId, to.getId(), linkId);
        return true;
    }

    @Nullable
    PersistentEntity getLink(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId) {
        final PersistentEntityId resultId = getLinkAsEntityId(txn, from, linkId);
        return resultId == null ? null : getEntity(resultId);
    }

    @Nullable
    public PersistentEntityId getLinkAsEntityId(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, int linkId) {
        PersistentEntityId resultId = txn.getCachedLink(from, linkId);
        if (resultId == null) {
            final ByteIterable resultEntry = getRawLink(txn, from, linkId);
            if (resultEntry != null) {
                resultId = (PersistentEntityId) LinkValue.entryToLinkValue(resultEntry).getEntityId();
                txn.cacheLink(from, linkId, resultId);
            }
        }
        return resultId;
    }

    @Nullable
    private ByteIterable getRawLink(@NotNull final PersistentStoreTransaction txn,
                                    @NotNull final PersistentEntity from,
                                    final int linkId) {
        return getRawValue(txn, from, linkId, linkDataGetter);
    }

    @Nullable
    private ByteIterable getRawValue(@NotNull final PersistentStoreTransaction txn,
                                     @NotNull final Entity entity,
                                     final int propertyId,
                                     @NotNull final DataGetter dataGetter) {
        final EntityId fromId = entity.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        if (entity.isUpToDate()) {
            return dataGetter.getUpToDateEntry(txn, entityTypeId, new PropertyKey(entityLocalId, propertyId));
        } else {
            final int version = entity.getVersion();
            final int nextVersion;
            ByteIterable keyEntry;
            try (Cursor cursor = getEntitiesHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction())) {
                keyEntry = LongBinding.longToCompressedEntry(entityLocalId);
                final ByteIterable valueEntry = IntegerBinding.intToCompressedEntry(version);
                if (cursor.getSearchBothRange(keyEntry, valueEntry) == null) { // key and value entries unchanged
                    return dataGetter.getUpToDateEntry(txn, entityTypeId, new PropertyKey(entityLocalId, propertyId));
                }
                nextVersion = IntegerBinding.compressedEntryToInt(cursor.getValue());
            }
            keyEntry = PropertyHistoryKey.propertyHistoryKeyToEntry(new PropertyHistoryKey(entityLocalId, nextVersion, propertyId));
            return dataGetter.getHistory(txn, entityTypeId).get(txn.getEnvironmentTransaction(), keyEntry);
        }
    }

    @NotNull
    EntityIterableBase getLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId) {
        final EntityId fromId = from.getId();
        if (from.isUpToDate()) { // up-to-date entity
            return new EntityFromLinksIterable(txn, this, fromId, linkId);
        } else { // historical entity
            final int entityTypeId = fromId.getTypeId();
            final long entityLocalId = fromId.getLocalId();
            final int version = from.getVersion();
            final int nextVersion;
            try (final Cursor cursor = getEntitiesHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction())) {
                final ByteIterable keyEntry = LongBinding.longToCompressedEntry(entityLocalId);
                final ByteIterable valueEntry = IntegerBinding.intToCompressedEntry(version);
                if (cursor.getSearchBothRange(keyEntry, valueEntry) == null) {
                    return new EntityFromLinksIterable(txn, this, fromId, linkId);
                }
                nextVersion = IntegerBinding.compressedEntryToInt(valueEntry);
            }
            return new EntityFromHistoryLinksIterable(this, fromId, nextVersion, linkId);
        }
    }

    @NotNull
    EntityIterable getLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final IntHashMap<String> linkNames) {
        final EntityId fromId = from.getId();
        if (from.isUpToDate()) { // up-to-date entity
            return new EntityFromLinkSetIterable(txn, this, fromId, linkNames);
        } else { // historical entity
            final int entityTypeId = fromId.getTypeId();
            final long entityLocalId = fromId.getLocalId();
            final int version = from.getVersion();
            final int nextVersion;
            try (Cursor cursor = getEntitiesHistoryTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction())) {
                final ByteIterable keyEntry = LongBinding.longToCompressedEntry(entityLocalId);
                final ByteIterable valueEntry = IntegerBinding.intToCompressedEntry(version);
                if (cursor.getSearchBothRange(keyEntry, valueEntry) == null) {
                    return new EntityFromLinkSetIterable(txn, this, fromId, linkNames);
                }
                nextVersion = IntegerBinding.compressedEntryToInt(valueEntry);
            }
            return new EntityFromHistoryLinkSetIterable(this, fromId, nextVersion, linkNames);
        }
    }

    boolean deleteLink(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId, @NotNull final PersistentEntity to) {
        return deleteLink(txn, from, linkId, to.getId());
    }

    boolean deleteLink(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, int linkId, @NotNull final PersistentEntityId toId) {
        final PersistentEntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
        final LinkValue linkValue = new LinkValue(toId, linkId);
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final TwoColumnTable links = getLinksTable(txn, entityTypeId);

        if (links.delete(envTxn, PropertyKey.propertyKeyToEntry(linkKey), LinkValue.linkValueToEntry(linkValue))) {
            txn.linkDeleted(fromId, toId, linkId);
            return true;
        }

        return false;
    }

    @NotNull
    public List<String> getLinkNames(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final List<String> result = new ArrayList<>();
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        PropertyKey linkKey = new PropertyKey(entityLocalId, 0);
        try (final Cursor index = getLinksTable(txn, id.getTypeId()).getFirstIndexCursor(txn.getEnvironmentTransaction())) {
            for (boolean success = index.getSearchKeyRange(PropertyKey.propertyKeyToEntry(linkKey)) != null; success; success = index.getNextNoDup()) {
                linkKey = PropertyKey.entryToPropertyKey(index.getKey());
                if (linkKey.getEntityLocalId() != entityLocalId) {
                    break;
                }
                final String linkName = getLinkName(txn, linkKey.getPropertyId());
                if (linkName != null) {
                    result.add(linkName);
                }
            }
            return result;
        }
    }

    @NotNull
    public List<Pair<String, EntityId>> getLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        final List<Pair<String, EntityId>> result = new ArrayList<>();
        final EntityId fromId = entity.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        Cursor cursor = null;
        try {
            if (entity.isUpToDate()) {
                PropertyKey linkKey = new PropertyKey(entityLocalId, 0);
                cursor = getLinksTable(txn, entityTypeId).getFirstIndexCursor(txn.getEnvironmentTransaction());
                for (boolean success = cursor.getSearchKeyRange(PropertyKey.propertyKeyToEntry(linkKey)) != null;
                     success; success = cursor.getNext()) {
                    linkKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                    if (linkKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String linkName = getLinkName(txn, linkKey.getPropertyId());
                    if (linkName != null) {
                        result.add(new Pair<>(
                                linkName, LinkValue.entryToLinkValue(cursor.getValue()).getEntityId()));
                    }
                }
            } else {
                final int version = entity.getVersion();
                PropertyHistoryKey linkHistoryKey = new PropertyHistoryKey(entityLocalId, version, 0);
                cursor = getLinksHistoryIndexCursor(txn, entityTypeId);
                for (boolean success = cursor.getSearchKeyRange(PropertyHistoryKey.propertyHistoryKeyToEntry(linkHistoryKey)) != null;
                     success; success = cursor.getNext()) {
                    linkHistoryKey = PropertyHistoryKey.entryToPropertyHistoryKey(cursor.getKey());
                    if (linkHistoryKey.getVersion() != version || linkHistoryKey.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final String linkName = getLinkName(txn, linkHistoryKey.getPropertyId());
                    if (linkName != null) {
                        result.add(new Pair<>(
                                linkName, LinkValue.entryToLinkValue(cursor.getValue()).getEntityId()));
                    }
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    @Deprecated
    public int getLastVersion(@NotNull final EntityId id) {
        return getLastVersion(getAndCheckCurrentTransaction(), id);
    }

    int getLastVersion(@NotNull final PersistentStoreTransaction txn, @NotNull final EntityId id) {
        final Store entities = getEntitiesTable(txn, id.getTypeId());
        final ByteIterable versionEntry = entities.get(txn.getEnvironmentTransaction(), LongBinding.longToCompressedEntry(id.getLocalId()));
        if (versionEntry == null) {
            return -1;
        }
        return IntegerBinding.compressedEntryToInt(versionEntry);
    }

    @Override
    public PersistentEntity getEntity(@NotNull final EntityId id) {
        return new PersistentEntity(this, (PersistentEntityId) id);
    }

    /**
     * Creates new version of the specified entity copying all its properties,
     * blobs and links to the history.
     *
     * @param entity specified entity.
     */
    public void newVersion(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentStoreTransaction snapshot, @NotNull final PersistentEntity entity) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final EntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long localId = id.getLocalId();

        final Store entitiesTable = getEntitiesTable(snapshot, entityTypeId);
        final ByteIterable key = LongBinding.longToCompressedEntry(localId);
        final ByteIterable versionEntry = entitiesTable.get(snapshot.getEnvironmentTransaction(), key);
        if (versionEntry == null) {
            throw new IllegalStateException("Entity wasn't saved to database yet, can't create history snapshot");
        }
        final int version = IntegerBinding.compressedEntryToInt(versionEntry);
        entitiesTable.put(envTxn, key, IntegerBinding.intToCompressedEntry(version + 1));

        final Store entitiesHistory = getEntitiesHistoryTable(txn, entityTypeId);
        entitiesHistory.put(envTxn, LongBinding.longToCompressedEntry(localId), IntegerBinding.intToCompressedEntry(version));
        //copy properties
        final Store propertiesHistory = getPropertiesHistoryTable(txn, entityTypeId);
        for (final String propName : getPropertyNames(snapshot, entity)) {
            final int propertyId = getPropertyId(txn, propName, false);
            if (propertyId < 0) continue;
            final ByteIterable entry = getRawProperty(snapshot, entity, propertyId);
            if (entry != null) {
                propertiesHistory.put(envTxn, PropertyHistoryKey.propertyHistoryKeyToEntry(
                        new PropertyHistoryKey(localId, version, getPropertyId(txn, propName, false))), entry);
            }
        }
        //copy blobs
        final Store blobsHistory = getBlobsHistoryTable(txn, entityTypeId);
        for (final String blobName : getBlobNames(snapshot, entity)) {
            final int blobId = getPropertyId(txn, blobName, false);
            if (blobId < 0) {
                continue;
            }
            final ByteIterable valueEntry = getRawValue(txn, entity, blobId, blobDataGetter);
            if (valueEntry == null) {
                continue;
            }
            final long blobHandle = LongBinding.compressedEntryToLong(valueEntry);
            txn.preserveBlob(blobHandle);
            blobsHistory.put(envTxn, PropertyHistoryKey.propertyHistoryKeyToEntry(
                    new PropertyHistoryKey(localId, version, getPropertyId(txn, blobName, false))), valueEntry);
        }
        //copy links
        final Store linksHistory = getLinksHistoryTable(txn, entityTypeId);
        for (final String linkName : getLinkNames(snapshot, entity)) {
            final int linkId = getLinkId(snapshot, linkName, false);
            if (linkId < 0) {
                continue;
            }
            final ByteIterable linkHistoryKey = PropertyHistoryKey.propertyHistoryKeyToEntry(
                    new PropertyHistoryKey(localId, version, linkId));
            final EntityIterator itr = getLinks(snapshot, entity, linkId).getIteratorImpl(snapshot);
            while (itr.hasNext()) {
                linksHistory.put(envTxn, linkHistoryKey, LinkValue.linkValueToEntry(new LinkValue(itr.nextId(), linkId)));
            }
        }
    }

    @Nullable
    public Entity getNextVersion(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final EntityId id = entity.getId();
        final int version = entity.getVersion();
        final int lastVersion = getLastVersion(txn, id);
        if (lastVersion >= 0) {
            // if specified entity is up-to-date
            if (version == lastVersion) {
                return null;
            }
            try (Cursor cursor = getEntitiesHistoryTable(txn, id.getTypeId()).openCursor(txn.getEnvironmentTransaction())) {
                boolean success = cursor.getSearchBothRange(
                        LongBinding.longToCompressedEntry(id.getLocalId()), IntegerBinding.intToCompressedEntry(version)) != null;
                if (success) {
                    if (version == IntegerBinding.compressedEntryToInt(cursor.getValue())) {
                        success = cursor.getNext();
                    }
                    if (success) {
                        final long localId = LongBinding.compressedEntryToLong(cursor.getKey());
                        if (localId == id.getLocalId()) {
                            return new PersistentEntity(
                                    this, new PersistentEntityId(id, IntegerBinding.compressedEntryToInt(cursor.getValue())));
                        }
                    }
                }
                // return up-to-date version if there is no more historic version newer than specified one
                return getEntity(new PersistentEntityId(id));
            }
        }
        throw new EntityStoreException("getNextVersion(): could not find history version = " + version + ", id = " + id);
    }

    @Nullable
    public Entity getPreviousVersion(@NotNull final PersistentStoreTransaction txn, @NotNull final Entity entity) {
        int version = entity.getVersion();
        final EntityId id = entity.getId();
        try (Cursor cursor = getEntitiesHistoryTable(txn, id.getTypeId()).openCursor(txn.getEnvironmentTransaction())) {
            final ByteIterable keyEntry = LongBinding.longToCompressedEntry(id.getLocalId());
            while (version > 0) {
                final ByteIterable valueEntry = IntegerBinding.intToCompressedEntry(--version);
                if (cursor.getSearchBoth(keyEntry, valueEntry)) {
                    return new PersistentEntity(this, new PersistentEntityId(id, version));
                }
            }
            return null;
        }
    }

    /**
     * Deletes specified entity clearing all its properties and deleting all its outgoing links.
     *
     * @param entity to delete.
     */
    boolean deleteEntity(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final Entity prev = getPreviousVersion(txn, entity);
        if (prev != null) {
            deleteEntity(txn, (PersistentEntity) prev);
        }
        clearProperties(txn, entity);
        clearBlobs(txn, entity);
        deleteLinks(txn, entity);
        final PersistentEntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        final ByteIterable key = LongBinding.longToCompressedEntry(entityLocalId);
        if (entity.isUpToDate()) { // up-to-date entity
            if (getEntitiesTable(txn, entityTypeId).delete(envTxn, key)) {
                txn.entityDeleted(id);
                return true;
            }
            return false;
        } else { // historical entity
            return getEntitiesHistoryTable(txn, entityTypeId).delete(envTxn, key);
        }
    }

    /**
     * Deletes all outgoing links of specified entity.
     *
     * @param entity the entity.
     */
    @SuppressWarnings({"OverlyLongMethod"})
    private void deleteLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final PersistentEntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        if (entity.isUpToDate()) { // up-to-date entity
            final TwoColumnTable links = getLinksTable(txn, entityTypeId);
            final PropertyKey linkKey = new PropertyKey(entityLocalId, 0);
            ByteIterable keyEntry = PropertyKey.propertyKeyToEntry(linkKey);
            try (Cursor cursor = links.getFirstIndexCursor(envTxn)) {
                for (boolean success = cursor.getSearchKeyRange(keyEntry) != null; success; success = cursor.getNext()) {
                    keyEntry = cursor.getKey();
                    final PropertyKey key = PropertyKey.entryToPropertyKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId) {
                        break;
                    }
                    final ByteIterable valueEntry = cursor.getValue();
                    if (links.delete(envTxn, keyEntry, valueEntry)) {
                        if (getLinkName(txn, key.getPropertyId()) != null) {
                            final LinkValue linkValue = LinkValue.entryToLinkValue(valueEntry);
                            txn.linkDeleted(entity.getId(), (PersistentEntityId) linkValue.getEntityId(), linkValue.getLinkId());
                        }
                    }
                }
            }
        } else { // historical entity
            final int version = entity.getVersion();
            final Store links = getLinksHistoryTable(txn, entityTypeId);
            final PropertyHistoryKey propertyKey = new PropertyHistoryKey(entityLocalId, version, 0);
            ByteIterable keyEntry = PropertyHistoryKey.propertyHistoryKeyToEntry(propertyKey);
            try (Cursor cursor = links.openCursor(envTxn)) {
                for (boolean success = cursor.getSearchKeyRange(keyEntry) != null; success; success = cursor.getNext()) {
                    keyEntry = cursor.getKey();
                    final PropertyHistoryKey key = PropertyHistoryKey.entryToPropertyHistoryKey(keyEntry);
                    if (key.getEntityLocalId() != entityLocalId || key.getVersion() != version) {
                        break;
                    }
                    links.delete(txn.getEnvironmentTransaction(), keyEntry);
                }
            }
        }
    }

    /**
     * Gets or creates id of the entity type.
     *
     * @param entityType  entity type name.
     * @param allowCreate if set to true and if there is no entity type like entityType,
     *                    create the new id for the entityType.
     * @return entity type id.
     */
    @Override
    @Deprecated
    public int getEntityTypeId(@NotNull final String entityType, final boolean allowCreate) {
        return getEntityTypeId(getAndCheckCurrentTransaction(), entityType, allowCreate);
    }

    public int getEntityTypeId(@NotNull final PersistentStoreTransaction txn, @NotNull final String entityType, final boolean allowCreate) {
        return allowCreate ? entityTypes.getOrAllocateId(txn, entityType) : entityTypes.getId(txn, entityType);
    }

    /**
     * Gets type name of the entity type id.
     *
     * @param entityTypeId id of the entity type.
     * @return entity type name.
     */
    @Override
    @NotNull
    @Deprecated
    public String getEntityType(final int entityTypeId) {
        return getEntityType(getAndCheckCurrentTransaction(), entityTypeId);
    }

    @NotNull
    public String getEntityType(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        final String result = entityTypes.getName(txn, entityTypeId);
        if (result == null) {
            throw new EntityStoreException("Invalid type id: " + entityTypeId);
        }
        return result;
    }

    /**
     * @return all entity types available.
     */
    @Deprecated
    @Override
    @NotNull
    public List<String> getEntityTypes() {
        return getEntityTypes(getAndCheckCurrentTransaction());
    }

    /**
     * @return all entity types available.
     */
    @NotNull
    public List<String> getEntityTypes(@NotNull final PersistentStoreTransaction txn) {
        final List<String> result = new ArrayList<>();
        try (Cursor entityTypesCursor = entityTypes.getTable().getSecondIndexCursor(txn.getEnvironmentTransaction())) {
            while (entityTypesCursor.getNext()) {
                final int entityTypeId = IntegerBinding.compressedEntryToInt(entityTypesCursor.getKey());
                result.add(getEntityType(txn, entityTypeId));
            }
        }
        return result;
    }

    @Override
    public void renameEntityType(@NotNull final String oldEntityTypeName, @NotNull final String newEntityTypeName) {
        entityTypes.rename(getAndCheckCurrentTransaction(), oldEntityTypeName, newEntityTypeName);
    }

    public void deleteEntityType(@NotNull final String entityTypeName) {
        final PersistentStoreTransaction txn = getAndCheckCurrentTransaction();
        final int entityTypeId = entityTypes.delete(txn, entityTypeName);

        if (entityTypeId < 0) {
            return;
        }

        entitiesTables.remove(entityTypeId);
        entitiesHistoryTables.remove(entityTypeId);
        propertiesTables.remove(entityTypeId);
        propertiesHistoryTables.remove(entityTypeId);
        linksTables.remove(entityTypeId);
        linksHistoryTables.remove(entityTypeId);
        blobsTables.remove(entityTypeId);
        blobsHistoryTables.remove(entityTypeId);

        final String entityTableName = namingRulez.getEntitiesTableName(entityTypeId);
        final String propertiesTableName = namingRulez.getPropertiesTableName(entityTypeId);
        final String linksTableName = namingRulez.getLinksTableName(entityTypeId);
        final String secondLinksTableName = TwoColumnTable.secondColumnDatabaseName(linksTableName);
        final String blobsTableName = namingRulez.getBlobsTableName(entityTypeId);

        truncateStores(txn, Arrays.<String>asList(
                        entityTableName, linksTableName, secondLinksTableName, propertiesTableName, blobsTableName),
                new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        return new Iterator<String>() { // enumerate all property value indexes
                            private int propertyId = 0;

                            @Override
                            public boolean hasNext() {
                                return propertyId < 10000; // this was taken from
                            }

                            @Override
                            public String next() {
                                return propertiesTableName + "#value_idx" + propertyId++;
                            }

                            @Override
                            public void remove() { // don't give a damn
                            }
                        };
                    }
                }
        );
    }

    private void truncateStores(@NotNull final PersistentStoreTransaction txn, @NotNull Iterable<String> unsafe, @NotNull Iterable<String> safe) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        for (final String name : unsafe) {
            environment.truncateStore(name, envTxn);
        }

        for (final String name : safe) {
            try {
                environment.truncateStore(name, envTxn);
            } catch (ExodusException e) {
                //ignore
            }
        }
    }

    @Deprecated
    public int getPropertyId(@NotNull final String propertyName, final boolean allowCreate) {
        return getPropertyId(getAndCheckCurrentTransaction(), propertyName, allowCreate);
    }

    /**
     * Gets id of a property and creates the new one if necessary.
     *
     * @param txn          transaction
     * @param propertyName name of the property.
     * @param allowCreate  if set to true and if there is no property named as propertyName,
     *                     create the new id for the propertyName.
     * @return < 0 if there is no such property and create=false, else id of the property
     */
    public int getPropertyId(@NotNull final PersistentStoreTransaction txn, @NotNull final String propertyName, final boolean allowCreate) {
        return allowCreate ? propertyIds.getOrAllocateId(txn, propertyName) : propertyIds.getId(txn, propertyName);
    }

    @Nullable
    public String getPropertyName(@NotNull final PersistentStoreTransaction txn, final int propertyId) {
        return propertyIds.getName(txn, propertyId);
    }

    @Deprecated
    public int getLinkId(@NotNull final String linkName, final boolean allowCreate) {
        return getLinkId(getAndCheckCurrentTransaction(), linkName, allowCreate);
    }

    /**
     * Gets id of a link and creates the new one if necessary.
     *
     * @param linkName    name of the link.
     * @param allowCreate if set to true and if there is no link named as linkName,
     *                    create the new id for the linkName.
     * @return < 0 if there is no such link and create=false, else id of the link
     */
    public int getLinkId(@NotNull final PersistentStoreTransaction txn, @NotNull final String linkName, final boolean allowCreate) {
        return allowCreate ? linkIds.getOrAllocateId(txn, linkName) : linkIds.getId(txn, linkName);
    }

    @Nullable
    String getLinkName(@NotNull final PersistentStoreTransaction txn, final int linkId) {
        return linkIds.getName(txn, linkId);
    }

    public List<String> getAllLinkNames(@NotNull final PersistentStoreTransaction txn) {
        final int lastLinkId = linkIds.getLastAllocatedId();
        final List<String> result = new ArrayList<>(lastLinkId + 1);
        for (int i = 0; i <= lastLinkId; ++i) {
            final String linkName = getLinkName(txn, i);
            if (linkName != null) {
                result.add(linkName);
            }
        }
        return result;
    }

    void clearHistory(@NotNull final PersistentStoreTransaction txn, @NotNull final String entityType) {
        final int entityTypeId = getEntityTypeId(txn, entityType, false);
        if (entityTypeId < 0) {
            return;
        }
        try {
            // set versions of all entities to zero
            final Store entitiesTable = getEntitiesTable(txn, entityTypeId);
            final Transaction envTxn = txn.getEnvironmentTransaction();
            try (Cursor entities = entitiesTable.openCursor(envTxn)) {
                while (entities.getNext()) {
                    entitiesTable.put(envTxn, entities.getKey(), PersistentStoreTransaction.ZERO_VERSION_ENTRY);
                }
            }
            safeTruncateStore(txn, namingRulez.getEntitiesHistoryTableName(entityTypeId));
            safeTruncateStore(txn, namingRulez.getPropertiesHistoryTableName(entityTypeId));
            safeTruncateStore(txn, namingRulez.getBlobsHistoryTableName(entityTypeId));
            safeTruncateStore(txn, namingRulez.getLinksHistoryTableName(entityTypeId));
        } catch (Throwable t) {
            throw ExodusException.toEntityStoreException(t);
        }
    }

    @Override
    public void updateUniqueKeyIndices(@NotNull final Iterable<Index> indices) {
        environment.suspendGC();
        try {
            executeInTransaction(new StoreTransactionalExecutable() {
                @Override
                public void execute(@NotNull StoreTransaction txn) {
                    final PersistentStoreTransaction t = (PersistentStoreTransaction) txn;
                    final PersistentStoreTransaction snapshot = t.getSnapshot();
                    try {
                        final Collection<String> indexNames = new HashSet<>();
                        for (final String dbName : environment.getAllStoreNames(t.getEnvironmentTransaction())) {
                            if (namingRulez.isUniqueKeyIndexName(dbName)) {
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

    private void removeObsoleteUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn, @NotNull final String indexName) {
        if (logger.isDebugEnabled()) {
            logger.debug("Remove obsolete index [" + indexName + ']');
        }
        environment.removeStore(indexName, txn.getEnvironmentTransaction());
    }

    @SuppressWarnings({"OverlyLongMethod", "ThrowCaughtLocally", "OverlyNestedMethod", "ConstantConditions"})
    private void createUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn,
                                      @NotNull final PersistentStoreTransaction snapshot,
                                      @NotNull final Index index) {
        if (logger.isDebugEnabled()) {
            logger.debug("Create index [" + index + ']');
        }

        final List<IndexField> fields = index.getFields();
        final int propCount = fields.size();
        if (propCount == 0) {
            throw new EntityStoreException("Can't create unique key index on empty list of keys.");
        }
        SingleColumnTable indexTable = null;
        Comparable[] props = new Comparable[propCount];
        for (final String entityType : index.getEntityTypesToIndex()) {
            int i = 0;
            for (final Entity entity : snapshot.getAll(entityType)) {
                for (int j = 0; j < propCount; ++j) {
                    final IndexField field = fields.get(j);
                    if (field.isProperty()) {
                        if ((props[j] = getProperty(txn, (PersistentEntity) entity, field.getName())) == null) {
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
                                    StoreConfig.USE_EXISTING :
                                    config.getUniqueIndicesUseBtree() ? StoreConfig.WITHOUT_DUPLICATES : StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
                }
                if (!indexTable.getDatabase().add(txn.getEnvironmentTransaction(), propertyTypes.dataArrayToEntry(props), LongBinding.longToCompressedEntry(entity.getId().getLocalId()))) {
                    throw new EntityStoreException("Failed to insert unique key (already exists), index: " + index + ", values = " + Arrays.toString(props));
                }
                if (++i % 100 == 0) {
                    txn.flush();
                }
            }
            txn.flush();
        }
    }

    void insertUniqueKey(@NotNull final PersistentStoreTransaction txn, @NotNull final Index index,
                         @NotNull final List<Comparable> propValues, @NotNull final Entity entity) {
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

    void deleteUniqueKey(@NotNull final PersistentStoreTransaction txn, @NotNull final Index index,
                         @NotNull final List<Comparable> propValues) {
        final int propCount = index.getFields().size();
        if (propCount != propValues.size()) {
            throw new IllegalArgumentException("Number of fields differs from the number of property values");
        }
        getUniqueKeyIndex(txn, index).delete(txn.getEnvironmentTransaction(), propertyTypes.dataArrayToEntry(propValues.toArray(new Comparable[propCount])));
    }

    @NotNull
    PersistentSequence getEntitiesSequence(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        synchronized (entitiesSequences) {
            PersistentSequence result = entitiesSequences.get(entityTypeId);
            if (result == null) {
                result = getSequence(txn, namingRulez.getEntitiesSequenceName(entityTypeId));
                entitiesSequences.put(entityTypeId, result);
            }
            return result;
        }
    }

    void preloadTables(final PersistentStoreTransaction txn) {
        // preload tables
        for (final String entityType : getEntityTypes(txn)) {
            final int id = getEntityTypeId(txn, entityType, false);
            if (id == -1) {
                throw new IllegalStateException("Entity types iterator returned non-existent id");
            }
            preloadTables(txn, id);
        }
    }

    private void preloadTables(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        getEntitiesTable(txn, entityTypeId);
        getPropertiesTable(txn, entityTypeId);
        getLinksTable(txn, entityTypeId);
        getBlobsTable(txn, entityTypeId);
        getEntitiesHistoryTable(txn, entityTypeId);
        getPropertiesHistoryTable(txn, entityTypeId);
        getLinksHistoryTable(txn, entityTypeId);
        getBlobsHistoryTable(txn, entityTypeId);
    }

    public void trackTableCreation(@NotNull final Store table, @NotNull final PersistentStoreTransaction txn) {
        if (table.isNew(txn.getEnvironmentTransaction())) {
            synchronized (tableCreationLog) {
                tableCreationLog.add(new TableCreationOperation() {
                    @Override
                    void persist(final Transaction txn) {
                        table.persistCreation(txn);
                    }
                });
            }
        }
    }

    @Override
    public void logOperations(final Transaction txn, final FlushLog flushLog) {
        for (final PersistentSequence sequence : getAllSequences()) {
            sequence.logOperations(txn, flushLog);
        }
        entityTypes.logOperations(txn, flushLog);
        propertyIds.logOperations(txn, flushLog);
        linkIds.logOperations(txn, flushLog);
        for (final TableCreationOperation op : tableCreationLog) {
            op.persist(txn);
            flushLog.add(op);
        }
    }

    @NotNull
    public TwoColumnTable getEntityTypesTable() {
        return entityTypes.getTable();
    }

    @NotNull
    public PropertyTypes getPropertyTypes() {
        return propertyTypes;
    }

    @NotNull
    public Store getEntitiesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) entitiesTables.get(txn, entityTypeId)).getDatabase();
    }

    @NotNull
    public Store getEntitiesHistoryTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) entitiesHistoryTables.get(txn, entityTypeId)).getDatabase();
    }

    @NotNull
    public PropertiesTable getPropertiesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (PropertiesTable) propertiesTables.get(txn, entityTypeId);
    }

    @NotNull
    public Store getPropertiesHistoryTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) propertiesHistoryTables.get(txn, entityTypeId)).getDatabase();
    }

    @NotNull
    public TwoColumnTable getLinksTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (TwoColumnTable) linksTables.get(txn, entityTypeId);
    }

    @NotNull
    public Store getLinksHistoryTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) linksHistoryTables.get(txn, entityTypeId)).getDatabase();
    }

    @NotNull
    public BlobsTable getBlobsTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (BlobsTable) blobsTables.get(txn, entityTypeId);
    }

    @NotNull
    public Store getBlobsHistoryTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) blobsHistoryTables.get(txn, entityTypeId)).getDatabase();
    }

    @NotNull
    public synchronized Store getUniqueKeyIndex(@NotNull final PersistentStoreTransaction txn, @NotNull final Index index) {
        return environment.openStore(getUniqueKeyIndexName(index), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn.getEnvironmentTransaction());
    }

    @Override
    @NotNull
    public EntityStoreSharedAsyncProcessor getAsyncProcessor() {
        return iterableCache.processor;
    }

    @NotNull
    @Override
    public Statistics getStatistics() {
        return statistics;
    }

    @Override
    public void close() {
        logger.info("Closing...");
        config.removeChangedSettingsListener(entityStoreSettingsListener);
        if (configMBean != null) {
            configMBean.unregister();
        }
        if (statisticsMBean != null) {
            statisticsMBean.unregister();
        }
        try {
            getAsyncProcessor().finish();
            synchronized (this) {
                blobVault.close();
                environment.close();
            }

            logger.info("Closed successfully.");
        } catch (Exception e) {
            logger.error("close() failed", e);
            throw ExodusException.toExodusException(e);
        }
    }

    @Override
    public BackupStrategy getBackupStrategy() {
        return new PersistentEntityStoreBackupStrategy(this);
    }

    @NotNull
    StoreNamingRules getNamingRules() {
        return namingRulez;
    }

    static boolean isEmptyOrInPlaceBlobHandle(final long blobHandle) {
        return EMPTY_BLOB_HANDLE == blobHandle || IN_PLACE_BLOB_HANDLE == blobHandle;
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
        return namingRulez.getUniqueKeyIndexName(index.getOwnerEntityType(), names);
    }

    private void safeTruncateStore(@NotNull final PersistentStoreTransaction txn, @NotNull final String dbName) {
        environment.truncateStore(dbName, txn.getEnvironmentTransaction());
    }

    private void deleteObsoleteBlobHandle(final long blobHandle, final PersistentStoreTransaction txn) {
        if (isEmptyOrInPlaceBlobHandle(blobHandle)) {
            return;
        }
        if (!txn.isBlobPreserved(blobHandle)) {
            txn.deleteBlob(blobHandle);
            txn.deferBlobDeletion(blobHandle);
        }
    }

    private interface DataGetter {

        Store getHistory(@NotNull PersistentStoreTransaction txn, int typeId);

        ByteIterable getUpToDateEntry(@NotNull PersistentStoreTransaction txn, int typeId, PropertyKey key);
    }

    private class PropertyDataGetter implements DataGetter {

        @Override
        public Store getHistory(@NotNull final PersistentStoreTransaction txn, int typeId) {
            return getPropertiesHistoryTable(txn, typeId);
        }

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn, int typeId, PropertyKey key) {
            return getPropertiesTable(txn, typeId).get(txn, PropertyKey.propertyKeyToEntry(key));
        }
    }

    private class LinkDataGetter implements DataGetter {

        @Override
        public Store getHistory(@NotNull final PersistentStoreTransaction txn, int typeId) {
            return getLinksHistoryTable(txn, typeId);
        }

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn, int typeId, PropertyKey key) {
            return getLinksTable(txn, typeId).get(txn.getEnvironmentTransaction(), PropertyKey.propertyKeyToEntry(key));
        }
    }

    private class DebugLinkDataGetter implements DataGetter {

        @Override
        public Store getHistory(@NotNull final PersistentStoreTransaction txn, int typeId) {
            return getLinksHistoryTable(txn, typeId);
        }

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn, int typeId, PropertyKey key) {
            final ByteIterable keyEntry = PropertyKey.propertyKeyToEntry(key);
            final ByteIterable valueEntry;
            try (Cursor cursor = getLinksTable(txn, typeId).getFirstIndexCursor(txn.getEnvironmentTransaction())) {
                valueEntry = cursor.getSearchKey(keyEntry);
                if (valueEntry == null) {
                    return null;
                }
                if (cursor.getNextDup()) { // getNextDup screws up valueEntry only if dup is found
                    throw new IllegalStateException("Only one link is allowed.");
                }
            }
            return valueEntry;
        }
    }

    private class BlobDataGetter implements DataGetter {

        @Override
        public Store getHistory(@NotNull final PersistentStoreTransaction txn, int typeId) {
            return getBlobsHistoryTable(txn, typeId);
        }

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn, int typeId, PropertyKey key) {
            return getBlobsTable(txn, typeId).get(txn.getEnvironmentTransaction(), key);
        }
    }

    private abstract class TableCreationOperation implements FlushLog.Operation {

        abstract void persist(final Transaction txn);

        @Override
        public void flushed() {
            synchronized (PersistentEntityStoreImpl.this.tableCreationLog) {
                tableCreationLog.remove(this);
            }
        }
    }
}