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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.*;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.bindings.*;
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.crypto.EncryptedBlobVault;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.entitystore.PersistentStoreTransaction.TransactionType;
import jetbrains.exodus.entitystore.iterate.EntityFromLinkSetIterable;
import jetbrains.exodus.entitystore.iterate.EntityFromLinksIterable;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.management.EntityStoreConfig;
import jetbrains.exodus.entitystore.management.EntityStoreStatistics;
import jetbrains.exodus.entitystore.tables.*;
import jetbrains.exodus.env.*;
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

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "UnusedDeclaration", "ThisEscapedInObjectConstruction", "VolatileLongOrDoubleField", "ObjectAllocationInLoop", "ReuseOfLocalVariable", "rawtypes", "NullableProblems"})
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
    @Nullable
    private BlobVault blobVault;

    @NotNull
    private final Map<String, PersistentSequence> allSequences;
    @NotNull
    private final IntHashMap<PersistentSequence> entitiesSequences;

    @NotNull
    private PersistentSequentialDictionary entityTypes;
    @NotNull
    private PersistentSequentialDictionary propertyIds;
    @NotNull
    private PersistentSequentialDictionary linkIds;

    @NotNull
    private final PropertyTypes propertyTypes;
    @NotNull
    private PersistentSequentialDictionary propertyCustomTypeIds;

    @NotNull
    private OpenTablesCache entitiesTables;
    @NotNull
    private OpenTablesCache propertiesTables;
    @NotNull
    private OpenTablesCache linksTables;
    @NotNull
    private OpenTablesCache blobsTables;
    @NotNull
    private Store internalSettings;
    @NotNull
    private Store sequences;

    @NotNull
    private final EntityIterableCache iterableCache;
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

    @NotNull
    private final TxnProvider txnProvider = new TxnProvider() {
        @NotNull
        @Override
        public PersistentStoreTransaction getTransaction() {
            return getAndCheckCurrentTransaction();
        }
    };

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
        this.blobVault = blobVault;
        PersistentEntityStores.adjustEnvironmentConfigForEntityStore(environment.getEnvironmentConfig());
        this.name = name;
        location = environment.getLocation();
        namingRulez = new StoreNamingRules(name);
        iterableCache = new EntityIterableCache(this);
        entityIdCache = new ConcurrentObjectCache<>(ENTITY_ID_CACHE_SIZE);
        explainer = new Explainer(config.isExplainOn());
        propertyDataGetter = new PropertyDataGetter();
        linkDataGetter = config.isDebugLinkDataGetter() ? new DebugLinkDataGetter() : new LinkDataGetter();
        blobDataGetter = new BlobDataGetter();
        allSequences = new HashMap<>();
        entitiesSequences = new IntHashMap<>();
        propertyTypes = new PropertyTypes();

        init();

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

    private void init() {
        final boolean fromScratch = computeInTransaction(new StoreTransactionalComputable<Boolean>() {
            @Override
            public Boolean compute(@NotNull StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                sequences = environment.openStore(SEQUENCES_STORE, StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction());
                if (blobVault == null) {
                    BlobVault vault = createDefaultFSBlobVault();
                    final StreamCipherProvider cipherProvider = environment.getCipherProvider();
                    if (cipherProvider != null) {
                        vault = new EncryptedBlobVault(vault, cipherProvider,
                            Objects.requireNonNull(environment.getCipherKey()), environment.getCipherBasicIV());
                    }
                    blobVault = vault;
                }

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
                propertiesTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                    @NotNull
                    @Override
                    public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                        return new PropertiesTable(txn,
                            namingRulez.getPropertiesTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
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
                blobsTables = new OpenTablesCache(new OpenTablesCache.TableCreator() {
                    @NotNull
                    @Override
                    public Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
                        return new BlobsTable(PersistentEntityStoreImpl.this, txn,
                            namingRulez.getBlobsTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES);
                    }
                });
                final String internalSettingsName = namingRulez.getInternalSettingsName();
                final Store settings = environment.openStore(internalSettingsName,
                    StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction(), false);
                final boolean result = settings == null;
                if (result) {
                    internalSettings = environment.openStore(internalSettingsName,
                        StoreConfig.WITHOUT_DUPLICATES, txn.getEnvironmentTransaction(), true);
                } else {
                    internalSettings = settings;
                }
                return result;
            }
        });
        if (!config.getRefactoringSkipAll() && !environment.getEnvironmentConfig().getEnvIsReadonly()) {
            applyRefactorings(fromScratch); // this method includes refactorings that could be clustered into separate txns
        }
        executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                preloadTables((PersistentStoreTransaction) txn); // pre-load tables for all entity types to avoid lazy load of the tables
            }
        });
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
            if (fromScratch || Settings.get(internalSettings, "refactorDropEmptyPrimaryLinkTables() applied") == null) {
                if (!fromScratch) {
                    refactorings.refactorDropEmptyPrimaryLinkTables();
                }
                Settings.set(internalSettings, "refactorDropEmptyPrimaryLinkTables() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "refactorMakeLinkTablesConsistent() applied") == null || config.getRefactoringHeavyLinks()) {
                if (!fromScratch) {
                    refactorings.refactorMakeLinkTablesConsistent();
                }
                Settings.set(internalSettings, "refactorMakeLinkTablesConsistent() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "refactorMakePropTablesConsistent() applied") == null || config.getRefactoringHeavyProps()) {
                if (!fromScratch) {
                    refactorings.refactorMakePropTablesConsistent();
                }
                Settings.set(internalSettings, "refactorMakePropTablesConsistent() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "Entities history deleted") == null) {
                if (!fromScratch) {
                    refactorings.refactorRemoveHistoryStores();
                }
                Settings.set(internalSettings, "Entities history deleted", "y");
            }
            if (blobVault instanceof VFSBlobVault && new File(location, BLOBS_DIR).exists()) {
                try {
                    ((VFSBlobVault) blobVault).refactorFromFS(this);
                } catch (IOException e) {
                    throw ExodusException.toEntityStoreException(e);
                }
            }
        } finally {
            environment.resumeGC();
        }
    }

    private BlobVault createDefaultFSBlobVault() {
        try {
            FileSystemBlobVaultOld blobVault;
            final PersistentSequenceBlobHandleGenerator.PersistentSequenceGetter persistentSequenceGetter =
                new PersistentSequenceBlobHandleGenerator.PersistentSequenceGetter() {
                    @Override
                    public PersistentSequence get() {
                        return getSequence(getAndCheckCurrentTransaction(), BLOB_HANDLES_SEQUENCE);
                    }
                };
            try {
                blobVault = new FileSystemBlobVault(config, location, BLOBS_DIR, BLOBS_EXTENSION,
                    new PersistentSequenceBlobHandleGenerator(persistentSequenceGetter));
            } catch (UnexpectedBlobVaultVersionException e) {
                blobVault = null;
            }
            if (blobVault == null) {
                if (config.getMaxInPlaceBlobSize() > 0) {
                    blobVault = new FileSystemBlobVaultOld(config, location, BLOBS_DIR, BLOBS_EXTENSION, BlobHandleGenerator.IMMUTABLE);
                } else {
                    blobVault = new FileSystemBlobVaultOld(config, location, BLOBS_DIR, BLOBS_EXTENSION,
                        new PersistentSequenceBlobHandleGenerator(persistentSequenceGetter));
                }
            }
            final long current = persistentSequenceGetter.get().get();
            for (long blobHandle = current + 1; blobHandle < current + 1000; ++blobHandle) {
                final File file = blobVault.getBlobLocation(blobHandle);
                if (file.exists()) {
                    logger.error("Redundant blob file: " + file);
                }
            }
            return blobVault;
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    @NotNull
    public PersistentStoreTransaction beginTransaction() {
        final PersistentStoreTransaction txn = new PersistentStoreTransaction(this);
        registerTransaction(txn);
        return txn;
    }

    @NotNull
    @Override
    public StoreTransaction beginExclusiveTransaction() {
        final PersistentStoreTransaction txn = new PersistentStoreTransaction(this, TransactionType.Exclusive);
        registerTransaction(txn);
        return txn;
    }

    @NotNull
    public PersistentStoreTransaction beginReadonlyTransaction() {
        final PersistentStoreTransaction txn = new ReadonlyPersistentStoreTransaction(this);
        registerTransaction(txn);
        return txn;
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

    public void registerTransaction(@NotNull final PersistentStoreTransaction txn) {
        final Thread thread = Thread.currentThread();
        Deque<PersistentStoreTransaction> stack = txns.get(thread);
        if (stack == null) {
            stack = new ArrayDeque<>(4);
            txns.put(thread, stack);
        }
        stack.push(txn);
    }

    public void unregisterTransaction(@NotNull final PersistentStoreTransaction txn) {
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
        allSequences.clear();
        entitiesSequences.clear();
        propertyTypes.clear();

        // fix of XD-536
        iterableCache.clear();

        init();

        blobVault.clear();
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

    @Override
    public void registerCustomPropertyType(@NotNull final StoreTransaction txn,
                                           @NotNull final Class<? extends Comparable> clazz,
                                           @NotNull final ComparableBinding binding) {
        final boolean[] wasAllocated = {false};
        final int typeId = propertyCustomTypeIds.getOrAllocateId(new TxnProvider() {
            @Override
            public @NotNull PersistentStoreTransaction getTransaction() {
                wasAllocated[0] = true;
                return (PersistentStoreTransaction) txn;
            }
        }, clazz.getName());
        if (wasAllocated[0]) {
            propertyTypes.registerCustomPropertyType(typeId, clazz, binding);
        }
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
    public void executeInExclusiveTransaction(@NotNull final StoreTransactionalExecutable executable) {
        final StoreTransaction txn = beginExclusiveTransaction();
        try {
            executable.execute(txn);
        } finally {
            txn.commit();
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
    public <T> T computeInExclusiveTransaction(@NotNull final StoreTransactionalComputable<T> computable) {
        final StoreTransaction txn = beginExclusiveTransaction();
        try {
            return computable.compute(txn);
        } finally {
            txn.commit();
        }
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

    public Explainer getExplainer() {
        return explainer;
    }

    @NotNull
    public EntityIterableCache getEntityIterableCache() {
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
    public Comparable getProperty(@NotNull final PersistentStoreTransaction txn,
                                  @NotNull final PersistentEntity entity,
                                  @NotNull final String propertyName) {
        final int propertyId = getPropertyId(txn, propertyName, false);
        if (propertyId < 0) {
            return null;
        }
        Comparable result = txn.getCachedProperty(entity, propertyId);
        if (result == null) {
            final PropertyValue propValue = getPropertyValue(txn, entity, propertyId);
            if (propValue != null) {
                result = propValue.getData();
                if (propValue.getType().getTypeId() != ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
                    txn.cacheProperty(entity.getId(), propertyId, result);
                }
            }
        }
        return result;
    }

    @Nullable
    public PropertyValue getPropertyValue(@NotNull final PersistentStoreTransaction txn,
                                          @NotNull final PersistentEntity entity,
                                          final int propertyId) {
        final ByteIterable entry = getRawProperty(txn, entity.getId(), propertyId);
        return entry != null ? propertyTypes.entryToPropertyValue(entry) : null;
    }

    @Nullable
    public ByteIterable getRawProperty(@NotNull final PersistentStoreTransaction txn,
                                       @NotNull final PersistentEntityId entityId,
                                       final int propertyId) {
        return getRawValue(txn, entityId, propertyId, propertyDataGetter);
    }

    public boolean setProperty(@NotNull final PersistentStoreTransaction txn,
                               @NotNull final PersistentEntity entity,
                               @NotNull final String propertyName,
                               @NotNull final Comparable value) {
        final PropertyValue propValue = propertyTypes.dataToPropertyValue(value);
        final ComparableValueType valueType = propValue.getType();
        if (valueType.getBinding() == ComparableSetBinding.BINDING && ((ComparableSet) value).isEmpty()) {
            return deleteProperty(txn, entity, propertyName);
        }
        final PersistentEntityId entityId = entity.getId();
        final int propertyId = getPropertyId(txn, propertyName, true);
        final ByteIterable oldValueEntry = getRawProperty(txn, entityId, propertyId);
        final Comparable oldValue = oldValueEntry == null ? null : propertyTypes.entryToPropertyValue(oldValueEntry).getData();
        if (value.equals(oldValue)) { // value is not null by contract
            return false;
        }
        getPropertiesTable(txn, entityId.getTypeId()).put(
            txn, entityId.getLocalId(), PropertyTypes.propertyValueToEntry(propValue), oldValueEntry, propertyId, valueType);
        txn.propertyChanged(entityId, propertyId, oldValue, value);
        return true;

    }

    public boolean deleteProperty(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity, @NotNull final String propertyName) {
        final int propertyId = getPropertyId(txn, propertyName, false);
        if (propertyId < 0) {
            return false;
        }
        final PersistentEntityId id = entity.getId();
        final ByteIterable oldValue = getRawProperty(txn, id, propertyId);
        if (oldValue == null) {
            return false;
        }
        final PropertyValue propValue = propertyTypes.entryToPropertyValue(oldValue);
        getPropertiesTable(txn, id.getTypeId()).delete(txn, id.getLocalId(),
            oldValue, propertyId, propValue.getType());
        txn.propertyChanged(id, propertyId, propValue.getData(), null);

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
        PropertyKey propertyKey = new PropertyKey(entityLocalId, 0);
        try (Cursor cursor = getPrimaryPropertyIndexCursor(txn, entityTypeId)) {
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
        final ByteIterable valueEntry = getRawValue(txn, entity.getId(), blobId, blobDataGetter);
        if (valueEntry == null) {
            return null;
        }
        final ByteIterator valueIterator = valueEntry.iterator();
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
        PropertyKey blobKey = new PropertyKey(entityLocalId, 0);
        try (Cursor cursor = getBlobsTable(txn, entityTypeId).getPrimaryIndex().openCursor(txn.getEnvironmentTransaction())) {
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
        }
        return result;
    }

    boolean addLink(@NotNull final PersistentStoreTransaction txn,
                    @NotNull final PersistentEntity from,
                    @NotNull PersistentEntity target,
                    final int linkId) {
        final EntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
        final LinkValue linkValue = new LinkValue(target.getId(), linkId);

        // check if the target is already deleted
        if (config.isDebugTestLinkedEntities()) {
            target = txn.getEntity(target.getId());
        }

        if (!getLinksTable(txn, entityTypeId).put(txn.getEnvironmentTransaction(),
            PropertyKey.propertyKeyToEntry(linkKey), LinkValue.linkValueToEntry(linkValue))) {
            return false;
        }

        txn.linkAdded(from.getId(), target.getId(), linkId);

        return true;
    }

    boolean setLink(@NotNull final PersistentStoreTransaction txn,
                    @NotNull final PersistentEntity from,
                    final int linkId,
                    @Nullable PersistentEntity target) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final PersistentEntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final ByteIterable keyEntry = PropertyKey.propertyKeyToEntry(new PropertyKey(entityLocalId, linkId));
        final TwoColumnTable links = getLinksTable(txn, entityTypeId);
        boolean oldTargetDeleted = false;

        // check if the target is already deleted
        if (target != null && config.isDebugTestLinkedEntities()) {
            target = txn.getEntity(target.getId());
        }

        final ByteIterable valueEntry = getRawLink(txn, fromId, linkId);
        if (valueEntry != null) {
            final PersistentEntity oldTarget = getEntity(LinkValue.entryToLinkValue(valueEntry).getEntityId());
            if (oldTarget.equals(target)) {
                return false;
            }
            links.delete(envTxn, keyEntry, valueEntry);
            txn.linkDeleted(fromId, oldTarget.getId(), linkId);
            oldTargetDeleted = true;
        }
        if (target == null) {
            return oldTargetDeleted;
        }
        final LinkValue linkValue = new LinkValue(target.getId(), linkId);
        links.put(envTxn, keyEntry, LinkValue.linkValueToEntry(linkValue));
        txn.linkAdded(fromId, target.getId(), linkId);
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
            resultId = getRawLinkAsEntityId(txn, from.getId(), linkId);
            if (resultId != null) {
                txn.cacheLink(from, linkId, resultId);
            }
        }
        return resultId;
    }

    @Nullable
    public PersistentEntityId getRawLinkAsEntityId(@NotNull final PersistentStoreTransaction txn,
                                                   @NotNull final PersistentEntityId fromId,
                                                   final int linkId) {
        final ByteIterable resultEntry = getRawLink(txn, fromId, linkId);
        return resultEntry == null ? null : (PersistentEntityId) LinkValue.entryToLinkValue(resultEntry).getEntityId();
    }

    @Nullable
    private ByteIterable getRawLink(@NotNull final PersistentStoreTransaction txn,
                                    @NotNull final PersistentEntityId fromId,
                                    final int linkId) {
        return getRawValue(txn, fromId, linkId, linkDataGetter);
    }

    @Nullable
    private ByteIterable getRawValue(@NotNull final PersistentStoreTransaction txn,
                                     @NotNull final EntityId entityId,
                                     final int propertyId,
                                     @NotNull final DataGetter dataGetter) {
        return dataGetter.getUpToDateEntry(txn, entityId.getTypeId(), new PropertyKey(entityId.getLocalId(), propertyId));
    }

    @NotNull
    EntityIterableBase getLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId) {
        return new EntityFromLinksIterable(txn, from.getId(), linkId);
    }

    @NotNull
    EntityIterable getLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final IntHashMap<String> linkNames) {
        return new EntityFromLinkSetIterable(txn, from.getId(), linkNames);
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
        PropertyKey linkKey = new PropertyKey(entityLocalId, 0);
        try (Cursor cursor = getLinksTable(txn, entityTypeId).getFirstIndexCursor(txn.getEnvironmentTransaction())) {
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
        }
        return result;
    }

    @Deprecated
    public int getLastVersion(@NotNull final EntityId id) {
        return getLastVersion(getAndCheckCurrentTransaction(), id);
    }

    public int getLastVersion(@NotNull final PersistentStoreTransaction txn, @NotNull final EntityId id) {
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
     * Deletes specified entity clearing all its properties and deleting all its outgoing links.
     *
     * @param entity to delete.
     */
    boolean deleteEntity(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        clearProperties(txn, entity);
        clearBlobs(txn, entity);
        deleteLinks(txn, entity);
        final PersistentEntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        final ByteIterable key = LongBinding.longToCompressedEntry(entityLocalId);
        if (config.isDebugSearchForIncomingLinksOnDelete()) {
            // search for incoming links
            final List<String> allLinkNames = getAllLinkNames(txn);
            for (final String entityType : txn.getEntityTypes()) {
                for (final String linkName : allLinkNames) {
                    //noinspection LoopStatementThatDoesntLoop
                    for (final Entity referrer : txn.findLinks(entityType, entity, linkName)) {
                        throw new EntityStoreException(entity +
                            " is about to be deleted, but it is referenced by " + referrer + ", link name: " + linkName);
                    }
                }
            }
        }
        if (getEntitiesTable(txn, entityTypeId).delete(txn.getEnvironmentTransaction(), key)) {
            txn.entityDeleted(id);
            return true;
        }
        return false;
    }

    /**
     * Deletes all outgoing links of specified entity.
     *
     * @param entity the entity.
     */
    private void deleteLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity entity) {
        final PersistentEntityId id = entity.getId();
        final int entityTypeId = id.getTypeId();
        final long entityLocalId = id.getLocalId();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final TwoColumnTable links = getLinksTable(txn, entityTypeId);
        try (Cursor cursor = links.getFirstIndexCursor(envTxn)) {
            for (boolean success = cursor.getSearchKeyRange(PropertyKey.propertyKeyToEntry(new PropertyKey(entityLocalId, 0))) != null;
                 success; success = cursor.getNext()) {
                final ByteIterable keyEntry = cursor.getKey();
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
    }

    @Override
    public int getEntityTypeId(@NotNull final String entityType) {
        return getEntityTypeId(entityType, false);
    }

    /**
     * Gets or creates id of the entity type.
     *
     * @param entityType  entity type name.
     * @param allowCreate if set to true and if there is no entity type like entityType,
     *                    create the new id for the entityType.
     * @return entity type id.
     */
    @Deprecated
    public int getEntityTypeId(@NotNull final String entityType, final boolean allowCreate) {
        return getEntityTypeId(txnProvider, entityType, allowCreate);
    }

    public int getEntityTypeId(@NotNull final PersistentStoreTransaction txn, @NotNull final String entityType, final boolean allowCreate) {
        return allowCreate ? entityTypes.getOrAllocateId(txn, entityType) : entityTypes.getId(txn, entityType);
    }

    public int getEntityTypeId(@NotNull final TxnProvider txnProvider, @NotNull final String entityType, final boolean allowCreate) {
        return allowCreate ? entityTypes.getOrAllocateId(txnProvider, entityType) : entityTypes.getId(txnProvider, entityType);
    }

    /**
     * Gets type name of the entity type id.
     *
     * @param entityTypeId id of the entity type.
     * @return entity type name.
     */
    @Override
    @NotNull
    public String getEntityType(final int entityTypeId) {
        return getEntityType(txnProvider, entityTypeId);
    }

    @NotNull
    public String getEntityType(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        final String result = entityTypes.getName(txn, entityTypeId);
        if (result == null) {
            throw new EntityStoreException("Invalid type id: " + entityTypeId);
        }
        return result;
    }

    @NotNull
    public String getEntityType(@NotNull final TxnProvider txnProvider, final int entityTypeId) {
        final String result = entityTypes.getName(txnProvider, entityTypeId);
        if (result == null) {
            throw new EntityStoreException("Invalid type id: " + entityTypeId);
        }
        return result;
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
        propertiesTables.remove(entityTypeId);
        linksTables.remove(entityTypeId);
        blobsTables.remove(entityTypeId);

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
        return getPropertyId(txnProvider, propertyName, allowCreate);
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

    public int getPropertyId(@NotNull final TxnProvider txnProvider, @NotNull final String propertyName, final boolean allowCreate) {
        return allowCreate ? propertyIds.getOrAllocateId(txnProvider, propertyName) : propertyIds.getId(txnProvider, propertyName);
    }

    @Nullable
    public String getPropertyName(@NotNull final PersistentStoreTransaction txn, final int propertyId) {
        return propertyIds.getName(txn, propertyId);
    }

    @Deprecated
    public int getLinkId(@NotNull final String linkName, final boolean allowCreate) {
        return getLinkId(txnProvider, linkName, allowCreate);
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

    public int getLinkId(@NotNull final TxnProvider txnProvider, @NotNull final String linkName, final boolean allowCreate) {
        return allowCreate ? linkIds.getOrAllocateId(txnProvider, linkName) : linkIds.getId(txnProvider, linkName);
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

    private void preloadTables(final PersistentStoreTransaction txn) {
        // preload tables
        for (final String entityType : getEntityTypes(txn)) {
            final int id = getEntityTypeId(txn, entityType, false);
            if (id == -1) {
                throw new IllegalStateException("Entity types iterator returned non-existent id");
            }
        }
    }

    private void preloadTables(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        getEntitiesTable(txn, entityTypeId);
        getPropertiesTable(txn, entityTypeId);
        getLinksTable(txn, entityTypeId);
        getBlobsTable(txn, entityTypeId);
    }

    public void trackTableCreation(@NotNull final Store table, @NotNull final PersistentStoreTransaction txn) {
        final StoreImpl tableImpl = (StoreImpl) table;
        if (tableImpl.isNew(txn.getEnvironmentTransaction())) {
            synchronized (tableCreationLog) {
                tableCreationLog.add(new TableCreationOperation() {
                    @Override
                    void persist(final Transaction txn) {
                        tableImpl.persistCreation(txn);
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
        propertyCustomTypeIds.logOperations(txn, flushLog);
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
    public PropertiesTable getPropertiesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (PropertiesTable) propertiesTables.get(txn, entityTypeId);
    }

    @NotNull
    public TwoColumnTable getLinksTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (TwoColumnTable) linksTables.get(txn, entityTypeId);
    }

    @NotNull
    public BlobsTable getBlobsTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (BlobsTable) blobsTables.get(txn, entityTypeId);
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
            iterableCache.clear();
            logger.info("Closed successfully.");
        } catch (Exception e) {
            logger.error("close() failed", e);
            throw ExodusException.toExodusException(e);
        }
    }

    @NotNull
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

    private void safeTruncateStore(@NotNull final PersistentStoreTransaction txn, @NotNull final String dbName) {
        environment.truncateStore(dbName, txn.getEnvironmentTransaction());
    }

    private void deleteObsoleteBlobHandle(final long blobHandle, final PersistentStoreTransaction txn) {
        if (isEmptyOrInPlaceBlobHandle(blobHandle)) {
            return;
        }
        txn.deleteBlob(blobHandle);
        txn.deferBlobDeletion(blobHandle);
    }

    private interface DataGetter {

        ByteIterable getUpToDateEntry(@NotNull PersistentStoreTransaction txn, int typeId, PropertyKey key);
    }

    private class PropertyDataGetter implements DataGetter {

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn, int typeId, PropertyKey key) {
            return getPropertiesTable(txn, typeId).get(txn, PropertyKey.propertyKeyToEntry(key));
        }
    }

    private class LinkDataGetter implements DataGetter {

        @Override
        public ByteIterable getUpToDateEntry(@NotNull final PersistentStoreTransaction txn,
                                             final int typeId,
                                             @NotNull final PropertyKey key) {
            return getTable(txn, typeId).get(txn.getEnvironmentTransaction(), PropertyKey.propertyKeyToEntry(key));
        }

        @NotNull
        private Pair<Integer, TwoColumnTable> lastUsedTable = new Pair<>(Integer.MIN_VALUE, null);

        private TwoColumnTable getTable(@NotNull final PersistentStoreTransaction txn,
                                        final int typeId) {
            final Pair<Integer, TwoColumnTable> lastUsedTable = this.lastUsedTable;
            if (lastUsedTable.getFirst() == typeId) {
                return lastUsedTable.getSecond();
            }
            final TwoColumnTable result = getLinksTable(txn, typeId);
            this.lastUsedTable = new Pair<>(typeId, result);
            return result;
        }
    }

    private class DebugLinkDataGetter implements DataGetter {

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