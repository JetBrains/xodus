/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.*;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.bindings.*;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.IntHashSet;
import jetbrains.exodus.crypto.EncryptedBlobVault;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.entitystore.PersistentStoreTransaction.TransactionType;
import jetbrains.exodus.entitystore.iterate.EntityFromLinkSetIterable;
import jetbrains.exodus.entitystore.iterate.EntityFromLinksIterable;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.management.EntityStoreConfig;
import jetbrains.exodus.entitystore.management.EntityStoreStatistics;
import jetbrains.exodus.entitystore.replication.PersistentEntityStoreReplicator;
import jetbrains.exodus.entitystore.tables.*;
import jetbrains.exodus.env.*;
import jetbrains.exodus.env.replication.EnvironmentReplicationDelta;
import jetbrains.exodus.io.DataReaderWriterProvider;
import jetbrains.exodus.io.WatchingFileDataReader;
import jetbrains.exodus.io.WatchingFileDataReaderWriterProvider;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.management.Statistics;
import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import jetbrains.exodus.util.UTFUtil;
import kotlin.Unit;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

@SuppressWarnings({"UnusedDeclaration", "ThisEscapedInObjectConstruction", "VolatileLongOrDoubleField",
        "ObjectAllocationInLoop", "ReuseOfLocalVariable", "rawtypes"})
public class PersistentEntityStoreImpl implements PersistentEntityStore, FlushLog.Member {

    private static final Logger logger = LoggerFactory.getLogger(PersistentEntityStoreImpl.class);

    @NonNls
    private static final String BLOBS_DIR = "blobs";
    @NonNls
    static final String BLOBS_EXTENSION = ".blob";
    @NonNls
    static final String BLOB_HANDLES_SEQUENCE = "blob.handles.sequence";
    @NonNls
    private static final String SEQUENCES_STORE = "sequences";
    // For version 2 database format, blob handles less than BLOB_HANDLE_ADDEND are "stub" values
    // used to encode specific cases by short entries.
    // E.g., EMPTY_BLOB_HANDLE is encoded by 0, and IN_PLACE_BLOB_HANDLE is encoded by 1
    static final long BLOB_HANDLE_ADDEND = 16L;
    static final long EMPTY_BLOB_HANDLE = Long.MAX_VALUE;
    static final long IN_PLACE_BLOB_HANDLE = EMPTY_BLOB_HANDLE - 1;
    static final long IN_PLACE_BLOB_REFERENCE_HANDLE = EMPTY_BLOB_HANDLE - 2;

    @NotNull
    private static final ByteArrayInputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

    private final int hashCode;
    @NotNull
    private final PersistentEntityStoreConfig config;
    @NotNull
    private final String name;
    @NotNull
    private final Environment environment;
    private boolean closeEnvironment;
    @NotNull
    private final DataReaderWriterProvider readerWriterProvider;
    @NotNull
    private final String location;
    @NotNull
    private final ThreadLocal<Deque<PersistentStoreTransaction>> txns =
            ThreadLocal.withInitial(() -> new ArrayDeque<>(4));

    @NotNull
    private final StoreNamingRules namingRulez;
    private BlobVault blobVault;

    @NotNull
    private final Map<String, PersistentSequence> allSequences;
    @NotNull
    private final IntHashMap<PersistentSequence> entitiesSequences;

    private PersistentSequentialDictionary entityTypes;
    private PersistentSequentialDictionary propertyIds;
    private PersistentSequentialDictionary linkIds;

    @NotNull
    private final PropertyTypes propertyTypes;
    private PersistentSequentialDictionary propertyCustomTypeIds;

    private OpenTablesCache entitiesTables;
    private OpenTablesCache propertiesTables;
    OpenTablesCache linksTables;
    private OpenTablesCache blobsTables;
    private OpenTablesCache blobHashesTables;
    private Store blobFileLengths;
    private Store internalSettings;
    private Store sequences;

    @NotNull
    private final EntityIterableCache iterableCache;
    private final Explainer explainer;

    private final DataGetter propertyDataGetter;
    private final DataGetter linkDataGetter;
    private final DataGetter nonDebugLinkDataGetter = new LinkDataGetter();
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
    private final TxnProvider txnProvider = this::getAndCheckCurrentTransaction;

    public PersistentEntityStoreImpl(@NotNull final Environment environment, @NotNull final String name) {
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
        closeEnvironment = false;
        this.blobVault = blobVault;
        PersistentEntityStores.adjustEnvironmentConfigForEntityStore(environment.getEnvironmentConfig());
        this.name = name;
        location = environment.getLocation();

        readerWriterProvider =
                Objects.requireNonNull(((EnvironmentImpl) environment).getLog().getConfig().getReaderWriterProvider());
        // if database is in-memory then never create blobs in BlobVault
        if (readerWriterProvider.isInMemory()) {
            config.setMaxInPlaceBlobSize(Integer.MAX_VALUE);
        }
        // if database is read-only turn caching off since cached results cannot be invalidated
        if (readerWriterProvider.isReadonly()) {
            config.setCachingDisabled(true);
        }

        namingRulez = new StoreNamingRules(name);
        iterableCache = new EntityIterableCache(this);
        explainer = new Explainer(config.isExplainOn());
        propertyDataGetter = new PropertyDataGetter();
        linkDataGetter = config.isDebugLinkDataGetter() ? new DebugLinkDataGetter() : nonDebugLinkDataGetter;
        blobDataGetter = new BlobDataGetter();
        allSequences = new HashMap<>();
        entitiesSequences = new IntHashMap<>();
        propertyTypes = new PropertyTypes();

        final PersistentEntityStoreReplicator replicator = config.getStoreReplicator();
        if (replicator != null) {
            replicate(replicator, blobVault);
        }

        init();

        if (readerWriterProvider instanceof WatchingFileDataReaderWriterProvider) {
            ((WatchingFileDataReader) ((EnvironmentImpl) environment).getLog().getConfig().getReader()).addNewDataListener((aLong, aLong2) -> {
                environment.executeInReadonlyTransaction(txn -> {
                    entityTypes.invalidate(txn);
                    propertyIds.invalidate(txn);
                    linkIds.invalidate(txn);
                    propertyCustomTypeIds.invalidate(txn);
                });
                return Unit.INSTANCE;
            });
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

        loggerDebug("Created successfully.");
    }

    private void init() {
        final boolean fromScratch = computeInTransaction(tx -> {
            final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
            final Transaction envTxn = txn.getEnvironmentTransaction();
            initBasicStores(envTxn);
            if (blobVault == null) {
                blobVault = initBlobVault();
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

            entitiesTables = new OpenTablesCache((t, entityTypeId) ->
            {
                final String entitiesTableName = namingRulez.getEntitiesTableName(entityTypeId);
                return useVersion1Format() ?
                        new SingleColumnTable(t, entitiesTableName, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING) :
                        new BitmapTable(t, entitiesTableName, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING);
            });
            propertiesTables = new OpenTablesCache((t, entityTypeId) -> new PropertiesTable(t,
                    namingRulez.getPropertiesTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES));
            linksTables = new OpenTablesCache((t, entityTypeId) -> new LinksTable(t,
                    namingRulez.getLinksTableName(entityTypeId), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING));
            blobsTables = new OpenTablesCache((t, entityTypeId) -> new BlobsTable(PersistentEntityStoreImpl.this, t,
                    useVersion1Format() ?
                            namingRulez.getBlobsObsoleteTableName(entityTypeId) : namingRulez.getBlobsTableName(entityTypeId),
                    useVersion1Format() ? StoreConfig.WITHOUT_DUPLICATES : StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING));
            blobHashesTables = new OpenTablesCache((t, entityTypeId) -> new SingleColumnTable(t,
                    namingRulez.getBlobHashesTableName(entityTypeId), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING));
            final String internalSettingsName = namingRulez.getInternalSettingsName();
            final Store settings = environment.openStore(internalSettingsName,
                    StoreConfig.WITHOUT_DUPLICATES, envTxn, false);
            final boolean result = settings == null;
            if (result) {
                internalSettings = environment.openStore(
                        internalSettingsName, StoreConfig.WITHOUT_DUPLICATES, envTxn, true);
            } else {
                internalSettings = settings;
            }
            return result;
        });
        if (!config.getRefactoringSkipAll() && !environment.isReadOnly()) {
            applyRefactorings(fromScratch); // this method includes refactorings that could be clustered into separate txns
        }
    }

    private void initBasicStores(Transaction envTxn) {
        sequences = environment.openStore(SEQUENCES_STORE, StoreConfig.WITHOUT_DUPLICATES, envTxn);
        blobFileLengths = environment.openStore(namingRulez.getBlobFileLengthsTable(),
                StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, envTxn);
    }

    private BlobVault initBlobVault() {
        if (readerWriterProvider.isInMemory()) {
            return new DummyBlobVault(config);
        }
        final FileSystemBlobVaultOld fsVault = createDefaultFSBlobVault();
        DiskBasedBlobVault result = fsVault;
        final StreamCipherProvider cipherProvider = environment.getCipherProvider();
        if (cipherProvider != null) {
            result = new EncryptedBlobVault(fsVault, cipherProvider,
                    Objects.requireNonNull(environment.getCipherKey()), environment.getCipherBasicIV());
        }
        final PersistentEntityStoreReplicator replicator = config.getStoreReplicator();
        if (replicator != null) {
            result = replicator.decorateBlobVault(result, this);
        }
        return (BlobVault) result; // TODO: improve this
    }

    private void applyRefactorings(final boolean fromScratch) {
        environment.suspendGC();
        try {
            final PersistentEntityStoreRefactorings refactorings = new PersistentEntityStoreRefactorings(this);
            if (config.getRefactoringDeleteRedundantBlobs()) {
                refactorings.refactorDeleteRedundantBlobs();
            }
            if (!useVersion1Format() && (fromScratch || Settings.get(internalSettings, "refactorBlobsForVersion2Format() applied") == null)) {
                if (!fromScratch) {
                    refactorings.refactorBlobsToVersion2Format(internalSettings);
                }
                Settings.set(internalSettings, "refactorBlobsForVersion2Format() applied", "y");
            }
            if (!useVersion1Format() && (fromScratch || Settings.get(internalSettings, "refactorEntitiesTablesToBitmap() applied") == null)) {
                if (!fromScratch) {
                    refactorings.refactorEntitiesTablesToBitmap(internalSettings);
                }
                Settings.set(internalSettings, "refactorEntitiesTablesToBitmap() applied", "y");
            }
            if (!useVersion1Format() && useIntForLocalId() && (fromScratch || Settings.get(internalSettings, "refactorAllPropsIndexToBitmap() applied") == null)) {
                if (!fromScratch) {
                    refactorings.refactorAllPropsIndexToBitmap();
                }
                Settings.set(internalSettings, "refactorAllPropsIndexToBitmap() applied", "y");
            }
            if (!useVersion1Format() && useIntForLocalId() && (fromScratch || Settings.get(internalSettings, "refactorAllLinksIndexToBitmap() applied") == null)) {
                if (!fromScratch) {
                    refactorings.refactorAllLinksIndexToBitmap();
                }
                Settings.set(internalSettings, "refactorAllLinksIndexToBitmap() applied", "y");
            }
            if (!useVersion1Format() && useIntForLocalId() && (fromScratch || Settings.get(internalSettings, "refactorAllBlobsIndexToBitmap() applied") == null)) {
                if (!fromScratch) {
                    refactorings.refactorAllBlobsIndexToBitmap();
                }
                Settings.set(internalSettings, "refactorAllBlobsIndexToBitmap() applied", "y");
            }
            if (!fromScratch && !useVersion1Format()) {
                refactorings.refactorDeduplicateInPlaceBlobsPeriodically(internalSettings);
            }
            if (fromScratch || Settings.get(internalSettings, "Null-indices refactored") == null || config.getRefactoringNullIndices()) {
                if (!fromScratch) {
                    Settings.delete(internalSettings, "Null-indices present"); // don't waste space
                    Settings.delete(internalSettings, "Null-indices present 2"); // don't waste space
                    Settings.delete(internalSettings, "Null-indices present 3"); // don't waste space
                    refactorings.refactorCreateNullPropertyIndices();
                }
                Settings.set(internalSettings, "Null-indices refactored", "yes");
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
                    refactorings.refactorMakeLinkTablesConsistent(internalSettings);
                }
                Settings.set(internalSettings, "refactorMakeLinkTablesConsistent() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "refactorMakePropTablesConsistent() applied") == null || config.getRefactoringHeavyProps()) {
                if (!fromScratch) {
                    refactorings.refactorMakePropTablesConsistent();
                }
                Settings.set(internalSettings, "refactorMakePropTablesConsistent() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "refactorFixNegativeFloatAndDoubleProps() applied") == null) {
                if (!fromScratch) {
                    refactorings.refactorFixNegativeFloatAndDoubleProps(internalSettings);
                }
                Settings.set(internalSettings, "refactorFixNegativeFloatAndDoubleProps() applied", "y");
            }
            if (fromScratch || Settings.get(internalSettings, "Link null-indices present") == null) {
                if (!fromScratch) {
                    refactorings.refactorCreateNullLinkIndices();
                }
                Settings.set(internalSettings, "Link null-indices present", "y");
            }
            if (blobVault instanceof VFSBlobVault && new File(location, BLOBS_DIR).exists()) {
                try {
                    ((VFSBlobVault) blobVault).refactorFromFS(this);
                } catch (IOException e) {
                    throw ExodusException.toEntityStoreException(e);
                }
            }
            if (blobVault instanceof DiskBasedBlobVault) {
                if (fromScratch || Settings.get(internalSettings, "refactorBlobFileLengths() applied") == null) {
                    if (!fromScratch) {
                        Settings.delete(internalSettings, "Blob file lengths cached"); // don't waste space
                        refactorings.refactorBlobFileLengths();
                    }
                    Settings.set(internalSettings, "refactorBlobFileLengths() applied", "y");
                }
            }
        } finally {
            environment.resumeGC();
        }
    }

    private FileSystemBlobVaultOld createDefaultFSBlobVault() {
        try {
            FileSystemBlobVaultOld blobVault;
            final PersistentSequenceBlobHandleGenerator.PersistentSequenceGetter persistentSequenceGetter =
                    () -> getSequence(getAndCheckCurrentTransaction(), BLOB_HANDLES_SEQUENCE);
            try {
                blobVault = new FileSystemBlobVault(environment, config, location, BLOBS_DIR, BLOBS_EXTENSION,
                        new PersistentSequenceBlobHandleGenerator(persistentSequenceGetter));
            } catch (UnexpectedBlobVaultVersionException e) {
                blobVault = null;
            }
            if (blobVault == null) {
                if (config.getMaxInPlaceBlobSize() > 0) {
                    blobVault = new FileSystemBlobVaultOld(environment, config, location, BLOBS_DIR, BLOBS_EXTENSION, BlobHandleGenerator.IMMUTABLE);
                } else {
                    blobVault = new FileSystemBlobVaultOld(environment, config, location, BLOBS_DIR, BLOBS_EXTENSION,
                            new PersistentSequenceBlobHandleGenerator(persistentSequenceGetter));
                }
            }
            final long current = persistentSequenceGetter.get().get();
            for (long blobHandle = current + 1; blobHandle < current + 1000; ++blobHandle) {
                final BlobVaultItem item = blobVault.getBlob(blobHandle);
                if (item.exists()) {
                    logger.error("Redundant blob item: " + item);
                }
            }
            blobVault.setSizeFunctions(new CachedBlobLengths(environment, blobFileLengths));
            return blobVault;
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
    }

    private void replicate(@NotNull PersistentEntityStoreReplicator replicator, @Nullable BlobVault blobVault) {
        if (blobVault != null) {
            throw new UnsupportedOperationException("Can only replicate default blob value");
        }

        final long highBlobHandle = computeInReadonlyTransaction(tx -> {
            final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
            final Transaction envTxn = txn.getEnvironmentTransaction();
            final Store sequencesStore = environment.openStore(
                    SEQUENCES_STORE, StoreConfig.WITHOUT_DUPLICATES, envTxn, false
            );
            final long result;
            if (sequencesStore == null) {
                result = -1;
            } else {
                final ByteIterable value = sequencesStore.get(envTxn, PersistentSequence.sequenceNameToEntry(BLOB_HANDLES_SEQUENCE));
                result = value == null ? -1 : LongBinding.compressedEntryToLong(value);
            }
            return result;
        });

        final EnvironmentReplicationDelta delta = replicator.beginReplication(environment);

        try {
            replicator.replicateEnvironment(delta, environment);

            if (highBlobHandle >= 0) {
                replicateBlobs(replicator, delta, highBlobHandle);
            }
        } finally {
            replicator.endReplication(delta);
        }
    }

    private void replicateBlobs(@NotNull PersistentEntityStoreReplicator replicator, @NotNull EnvironmentReplicationDelta delta, final long highBlobHandle) {
        final List<Pair<Long, Long>> blobsToReplicate = computeInReadonlyTransaction(tx -> {
            final String internalSettingsName = namingRulez.getInternalSettingsName();
            final Transaction envTxn = ((PersistentStoreTransaction) tx).getEnvironmentTransaction();
            final Store settings = environment.openStore(internalSettingsName,
                    StoreConfig.WITHOUT_DUPLICATES, envTxn, false);
            final Store blobFileLengths = environment.openStore(namingRulez.getBlobFileLengthsTable(),
                    StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, envTxn, false);

            if (settings == null || blobFileLengths == null) {
                return Collections.emptyList();
            }
            if (Settings.get(settings, "refactorBlobFileLengths() applied") == null) {
                throw new IllegalStateException("Cannot replicate blobs without serialized blob list");
            }

            final List<Pair<Long, Long>> result = new ArrayList<>();
            try (Cursor cursor = blobFileLengths.openCursor(getAndCheckCurrentTransaction().getEnvironmentTransaction())) {
                final ByteIterable foundBlob = cursor.getSearchKeyRange(LongBinding.longToCompressedEntry(highBlobHandle));
                if (foundBlob != null) {
                    do {
                        final long blobHandle = LongBinding.compressedEntryToLong(cursor.getKey());
                        if (blobHandle > highBlobHandle) {
                            final long fileLength = LongBinding.compressedEntryToLong(cursor.getValue());
                            result.add(new Pair<>(blobHandle, fileLength));
                        }
                    } while (cursor.getNext());
                }
            }
            return result;
        });
        if (!blobsToReplicate.isEmpty()) {
            final BlobVault vault = computeInReadonlyTransaction(txn -> {
                initBasicStores(((PersistentStoreTransaction) txn).getEnvironmentTransaction());
                return initBlobVault();
            });

            replicator.replicateBlobVault(delta, vault, blobsToReplicate);

            this.blobVault = vault;
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
    public PersistentStoreTransaction beginExclusiveTransaction() {
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
        final Deque<PersistentStoreTransaction> stack = txns.get();
        return stack.peek();
    }

    @NotNull
    public PersistentStoreTransaction getAndCheckCurrentTransaction() {
        final PersistentStoreTransaction transaction = getCurrentTransaction();
        if (transaction == null) {
            throw new IllegalStateException("EntityStore: current transaction is not set.");
        }
        return transaction;
    }

    public PersistentStoreTransaction beginTransactionAt(final long highAddress) {
        return new PersistentStoreTransactionSnapshot(this, highAddress);
    }

    public void registerTransaction(@NotNull final PersistentStoreTransaction txn) {
        final Deque<PersistentStoreTransaction> stack = txns.get();
        stack.push(txn);
    }

    public void unregisterTransaction(@NotNull final PersistentStoreTransaction txn) {
        final Thread thread = Thread.currentThread();
        final Deque<PersistentStoreTransaction> stack = txns.get();
        if (stack == null) {
            throw new EntityStoreException("Transaction was already finished");
        }
        if (txn != stack.peek()) {
            throw new EntityStoreException("Can't finish transaction: nested transaction is not finished");
        }
        stack.pop();
        txn.closeCaches();
    }

    public void ensureBlobsConsistency(final PersistentStoreTransaction txn) {
        try (Cursor entityTypesCursor = entityTypes.getTable().getSecondIndexCursor(txn.getEnvironmentTransaction())) {
            while (entityTypesCursor.getNext()) {
                final int entityTypeId = IntegerBinding.compressedEntryToInt(entityTypesCursor.getKey());

                var blobsTable = getBlobsTable(txn, entityTypeId);
                var store = blobsTable.getPrimaryIndex();
                var envTransaction = txn.getEnvironmentTransaction();

                try (var blobCursor = store.openCursor(envTransaction)) {
                    while (blobCursor.getNext()) {
                        var blobIterable = blobCursor.getValue();
                        var blobHandle = entryToBlobHandle(blobIterable);

                        if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
                            int counter = 0;
                            final int sleepInterval = 50;
                            final int counterMaxValue = config.getBlobMaxReadWaitingInterval() * 1000 / sleepInterval;

                            while (true) {
                                var blobLocation = blobVault.getBlobLocation(blobHandle);
                                if (blobLocation.exists() && blobLocation.length() ==
                                        getBlobFileLength(blobHandle, envTransaction).longValue()) {
                                    break;
                                }

                                if (counter > counterMaxValue) {
                                    if (!blobLocation.exists()) {
                                        throw new ExodusException("Blob file " + blobLocation.getAbsolutePath() +
                                                " does not exist in expected location.");
                                    }

                                    var expectedBlobLength = getBlobFileLength(blobHandle, envTransaction).longValue();
                                    if (blobLocation.length() != expectedBlobLength) {
                                        throw new ExodusException("Blob file " + blobLocation.getAbsolutePath() +
                                                " expected length is " + expectedBlobLength + " but actual is " +
                                                blobLocation.length());
                                    }

                                    break;
                                }

                                counter++;
                                //noinspection BusyWait
                                Thread.sleep(sleepInterval);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new ExodusException("Store : " + getName() + " - Execution of thread was interrupted", e);
                }
            }
        }
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

    @NotNull
    public PersistentSequence getSequence(@NotNull final PersistentStoreTransaction txn,
                                          @NotNull final String sequenceName,
                                          final long initialValue) {
        synchronized (allSequences) {
            PersistentSequence result = allSequences.get(sequenceName);
            if (result == null) {
                result = new PersistentSequence(txn, sequences, sequenceName, initialValue);
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
        final int typeId = propertyCustomTypeIds.getOrAllocateId(() -> {
            wasAllocated[0] = true;
            return (PersistentStoreTransaction) txn;
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
            txn.commit();
        } finally {
            // don't forget to release transaction permit
            // if txn has not already been aborted in execute()
            if (txn == getCurrentTransaction()) {
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
            final T result = computable.compute(txn);
            txn.commit();
            return result;
        } finally {
            if (txn == getCurrentTransaction()) {
                txn.abort();
            }
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
        if ((value instanceof ComparableSet && ((ComparableSet) value).isEmpty()) ||
                (value instanceof Boolean && value.equals(Boolean.FALSE))) {
            return deleteProperty(txn, entity, propertyName);
        }
        final PropertyValue propValue = propertyTypes.dataToPropertyValue(value);
        final ComparableValueType valueType = propValue.getType();
        final PersistentEntityId entityId = entity.getId();
        final int propertyId = getPropertyId(txn, propertyName, true);
        final ByteIterable oldValueEntry = getRawProperty(txn, entityId, propertyId);

        final Comparable oldValue = oldValueEntry == null ? null :
                propertyTypes.entryToPropertyValue(oldValueEntry).getData();
        if (value.equals(oldValue)) { // value is not null by contract
            return false;
        }
        getPropertiesTable(txn, entityId.getTypeId()).put(
                txn, entityId.getLocalId(), PropertyTypes.propertyValueToEntry(propValue), oldValueEntry,
                propertyId, valueType);
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
    public Iterable<Pair<Integer, Long>> getEntityWithPropIterable(@NotNull final PersistentStoreTransaction txn,
                                                                   int entityTypeId, int propertyId) {
        return getPropertiesTable(txn, entityTypeId)
                .getAllPropsIndex()
                .iterable(txn.getEnvironmentTransaction(), propertyId);
    }

    @NotNull
    public Iterable<Pair<Integer, Long>> getEntityWithBlobIterable(@NotNull final PersistentStoreTransaction txn,
                                                                   final int entityTypeId,
                                                                   final int blobId) {
        return getBlobsTable(txn, entityTypeId).getAllBlobsIndex().iterable(txn.getEnvironmentTransaction(), blobId);
    }

    @NotNull
    @Deprecated
    public Cursor getEntitiesIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getEntitiesTable(txn, entityTypeId).openCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public BitmapIterator getEntitiesBitmapIterator(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getEntitiesBitmapTable(txn, entityTypeId).iterator(txn.getEnvironmentTransaction());
    }

    @NotNull
    public BitmapIterator getEntitiesBitmapReverseIterator(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getEntitiesBitmapTable(txn, entityTypeId).reverseIterator(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getLinksFirstIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getLinksTable(txn, entityTypeId).getFirstIndexCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Cursor getLinksSecondIndexCursor(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return getLinksTable(txn, entityTypeId).getSecondIndexCursor(txn.getEnvironmentTransaction());
    }

    @NotNull
    public Iterable<Pair<Integer, Long>> getEntityWithLinkIterable(@NotNull final PersistentStoreTransaction txn,
                                                                   final int entityTypeId,
                                                                   final int linkId) {
        return getLinksTable(txn, entityTypeId).getAllLinksIndex().iterable(txn.getEnvironmentTransaction(), linkId);
    }

    /**
     * Clears all properties of specified entity.
     *
     * @param entity to clear.
     */
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
                properties.delete(txn, entityLocalId, value, propertyId, propValue.getType());
            }
        }
    }

    public long getBlobSize(@NotNull final PersistentStoreTransaction txn,
                            @NotNull final PersistentEntity entity,
                            @NotNull final String blobName) throws IOException {
        final Pair<Long, ByteIterator> blobInfo = getBlobHandleAndValue(txn, entity, blobName);
        if (blobInfo == null) {
            return -1;
        }
        final long blobHandle = blobInfo.getFirst();
        if (blobHandle == EMPTY_BLOB_HANDLE) {
            return 0;
        }
        if (isInPlaceBlobHandle(blobHandle)) {
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
        InputStream result = blobStream.getSecond();
        if (result != null) {
            return result;
        }

        var envTxn = txn.getEnvironmentTransaction();
        var blobLength = getBlobFileLength(blobHandle, envTxn);

        result = blobVault.getContent(blobHandle, envTxn, blobLength);

        if (result == null) {
            int counter = 0;

            final int sleepInterval = 50;
            final int counterMaxValue = config.getBlobMaxReadWaitingInterval() * 1000 / sleepInterval;

            while (true) {
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    throw new ExodusException("Store : " + getName() +
                            " . Reading of blob content was interrupted.", e);
                }
                counter++;

                result = blobVault.getContent(blobHandle, envTxn,
                        blobLength);

                if (result != null) {
                    break;
                }

                if (counter >= counterMaxValue) {
                    throw new ExodusException("Store : " + getName() + " data is broken. " +
                            "Can not read blob with handle " + blobHandle);
                }
            }
        }

        if (result == null && !readerWriterProvider.isReadonly()) {
            loggerWarn("Blob not found: " + blobVault.getBlobLocation(blobHandle), new FileNotFoundException());
        }
        return result;
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
            result = "";
        } else {
            try {
                final InputStream stream = blobStream.getSecond();
                if (stream == null) {
                    var envTxn = txn.getEnvironmentTransaction();
                    var blobLength = getBlobFileLength(blobHandle, envTxn);

                    var blobString = blobVault.getStringContent(blobHandle, envTxn,
                            blobLength);

                    if (blobString != null) {
                        return blobString;
                    }

                    int counter = 0;
                    final int sleepInterval = 50;
                    final int counterMaxValue = config.getBlobMaxReadWaitingInterval() * 1000 / sleepInterval;

                    while (true) {
                        try {
                            Thread.sleep(sleepInterval);
                        } catch (InterruptedException e) {
                            throw new ExodusException("Store : " + getName() +
                                    " . Reading of blob content was interrupted.", e);
                        }
                        counter++;

                        blobString = blobVault.getStringContent(blobHandle, envTxn,
                                blobLength);

                        if (blobString != null) {
                            return blobString;
                        }

                        if (counter >= counterMaxValue) {
                            throw new ExodusException("Store : " + getName() + " data is broken. " +
                                    "Can not read blob with handle " + blobHandle);
                        }
                    }
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
        long blobHandle = LongBinding.readCompressed(valueIterator);
        if (!useVersion1Format()) {
            if (blobHandle == 0L)
                blobHandle = EMPTY_BLOB_HANDLE;
            else if (blobHandle == 1L)
                blobHandle = IN_PLACE_BLOB_HANDLE;
            else if (blobHandle == 2L) {
                blobHandle = IN_PLACE_BLOB_REFERENCE_HANDLE;
            } else
                blobHandle -= BLOB_HANDLE_ADDEND;
        }
        return new Pair<>(blobHandle, valueIterator);
    }

    private boolean useIntForLocalId() {
        return config.getUseIntForLocalId();
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
        if (isInPlaceBlobHandle(blobHandle)) {
            final ByteIterator valueIterator = blobInfo.getSecond();
            final int size = (int) CompressedUnsignedLongByteIterable.getLong(valueIterator);
            if (blobHandle == IN_PLACE_BLOB_HANDLE) {
                return new Pair<>(blobHandle,
                        new ByteArraySizedInputStream(ByteIterableBase.readIterator(valueIterator, size)));
            }
            final ByteIterable hashEntry = new ArrayByteIterable(valueIterator);
            final ByteIterable duplicateEntry = getBlobHashesTable(txn,
                    entity.getId().getTypeId()).getDatabase().get(txn.getEnvironmentTransaction(), hashEntry);
            if (duplicateEntry == null) {
                throw new EntityStoreException("No duplicate entry is available to in-place blob reference");
            }
            return new Pair<>(blobHandle,
                    new ByteArraySizedInputStream(ByteIterableBase.readIteratorSafe(duplicateEntry.iterator(), size)));
        }
        return new Pair<>(blobHandle, txn.getBlobStream(blobHandle));
    }

    public void setBlob(@NotNull final PersistentStoreTransaction txn,
                        @NotNull final PersistentEntity entity,
                        @NotNull final String blobName,
                        @NotNull final InputStream stream) throws IOException {
        InputStream bufferedStream = new BufferedInputStream(stream);

        int maxEmbeddedBlobSize = config.getMaxInPlaceBlobSize();
        bufferedStream.mark(maxEmbeddedBlobSize + 1);

        final ByteArrayOutputStream memCopy = new LightByteArrayOutputStream();
        IOUtil.copyStreams(bufferedStream, maxEmbeddedBlobSize + 1, memCopy, IOUtil.getBUFFER_ALLOCATOR());

        final int size = memCopy.size();
        if (size <= maxEmbeddedBlobSize) {
            bufferedStream.close();

            bufferedStream = new ByteArraySizedInputStream(memCopy.toByteArray(), 0, size);
        } else {
            bufferedStream.reset();
        }

        final long blobHandle = createBlobHandle(txn, entity, blobName, bufferedStream, size);


        if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
            txn.addBlob(blobHandle, bufferedStream);
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
                size > config.getMaxInPlaceBlobSize() ? null : blobVault.cloneFile(file), size);

        if (!isEmptyOrInPlaceBlobHandle(blobHandle)) {
            setBlobFileLength(txn, blobHandle, length);
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
                setBlobFileLength(txn, blobHandle, copy.size());
                txn.addBlob(blobHandle, copy);
            }
        }
    }

    private long createBlobHandle(@NotNull final PersistentStoreTransaction txn,
                                  @NotNull final PersistentEntity entity,
                                  @NotNull final String blobName,
                                  @Nullable final InputStream stream,
                                  final int size) throws IOException {
        final EntityId id = entity.getId();
        final long entityLocalId = id.getLocalId();
        final int blobId = getPropertyId(txn, blobName, true);
        txn.invalidateCachedBlobString(entity, blobId);
        final int typeId = id.getTypeId();
        final BlobsTable blobs = getBlobsTable(txn, typeId);
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final ByteIterable value = blobs.get(envTxn, entityLocalId, blobId);
        if (value != null) {
            deleteObsoleteBlobHandle(entryToBlobHandle(value), txn);
        }
        final long blobHandle;
        if (size == 0) {
            blobHandle = EMPTY_BLOB_HANDLE;
            blobs.put(envTxn, entityLocalId, blobId, blobHandleToEntry(blobHandle));
        } else if (size <= config.getMaxInPlaceBlobSize()) {
            if (stream == null) {
                throw new NullPointerException("In-memory blob content is expected");
            }

            var embeddedStream = (ByteArraySizedInputStream) stream;
            if (!useVersion1Format()) {
                // check for duplicate
                final ByteIterable hashEntry = findDuplicate(txn, typeId, embeddedStream);
                if (hashEntry != null) {
                    blobs.put(envTxn, entityLocalId, blobId,
                            new CompoundByteIterable(new ByteIterable[]{
                                    blobHandleToEntry(IN_PLACE_BLOB_REFERENCE_HANDLE),
                                    CompressedUnsignedLongByteIterable.getIterable(size),
                                    hashEntry
                            })
                    );
                    return IN_PLACE_BLOB_REFERENCE_HANDLE;
                }
            }
            blobHandle = IN_PLACE_BLOB_HANDLE;
            blobs.put(envTxn, entityLocalId, blobId,
                    new CompoundByteIterable(new ByteIterable[]{
                            blobHandleToEntry(blobHandle),
                            CompressedUnsignedLongByteIterable.getIterable(size),
                            new ArrayByteIterable(embeddedStream.toByteArray(), size)
                    })
            );
        } else {
            blobHandle = blobVault.nextHandle(envTxn);
            blobs.put(envTxn, entityLocalId, blobId, blobHandleToEntry(blobHandle));
        }
        return blobHandle;
    }

    @Nullable
    ByteIterable findDuplicate(@NotNull final PersistentStoreTransaction txn,
                               final int typeId,
                               @NotNull final ByteArraySizedInputStream stream) {
        final ArrayByteIterable duplicateKey = IntegerBinding.intToEntry(stream.hashCode());
        final ByteIterable duplicateEntry =
                getBlobHashesTable(txn, typeId).getDatabase().get(txn.getEnvironmentTransaction(), duplicateKey);
        return (duplicateEntry != null && stream.equals(
                new ByteArraySizedInputStream(ByteIterableBase.readIterable(duplicateEntry)))) ? duplicateKey : null;
    }

    public boolean useVersion1Format() {
        return environment.getEnvironmentConfig().getUseVersion1Format();
    }

    ByteIterable blobHandleToEntry(long blobHandle) {
        if (!useVersion1Format()) {
            if (blobHandle == EMPTY_BLOB_HANDLE)
                blobHandle = 0L;
            else if (blobHandle == IN_PLACE_BLOB_HANDLE)
                blobHandle = 1L;
            else if (blobHandle == IN_PLACE_BLOB_REFERENCE_HANDLE)
                blobHandle = 2L;
            else
                blobHandle += BLOB_HANDLE_ADDEND;
        }
        return LongBinding.longToCompressedEntry(blobHandle);
    }

    private long entryToBlobHandle(final ByteIterable entry) {
        final long result = LongBinding.compressedEntryToLong(entry);
        if (useVersion1Format()) {
            return result;
        }
        if (result == 0L) return EMPTY_BLOB_HANDLE;
        if (result == 1L) return IN_PLACE_BLOB_HANDLE;
        if (result == 2L) return IN_PLACE_BLOB_REFERENCE_HANDLE;
        return result - BLOB_HANDLE_ADDEND;
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
        deleteObsoleteBlobHandle(entryToBlobHandle(value), txn);
        return true;
    }

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
                deleteObsoleteBlobHandle(entryToBlobHandle(value), txn);
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
                            blobName, entryToBlobHandle(cursor.getValue())));
                }
            }
        }
        return result;
    }

    boolean addLink(@NotNull final PersistentStoreTransaction txn,
                    @NotNull final PersistentEntity from,
                    @NotNull PersistentEntity target,
                    final int linkId) {
        final PersistentEntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
        final LinkValue linkValue = new LinkValue(target.getId(), linkId);

        // check if the target is already deleted
        if (config.isDebugTestLinkedEntities()) {
            target = getEntityAssertPhantomLink(txn, target.getId());
        }

        final ByteIterable existingValue = nonDebugLinkDataGetter.getUpToDateEntry(txn, entityTypeId, linkKey); // there can be duplicates
        if (!getLinksTable(txn, entityTypeId).put(txn.getEnvironmentTransaction(),
                entityLocalId, LinkValue.linkValueToEntry(linkValue), existingValue == null, linkId)) {
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
        final LinksTable links = getLinksTable(txn, entityTypeId);
        boolean oldTargetDeleted = false;

        // check if the target is already deleted
        if (target != null && config.isDebugTestLinkedEntities()) {
            target = getEntityAssertPhantomLink(txn, target.getId());
        }

        final ByteIterable oldValue = getRawLink(txn, fromId, linkId);
        if (oldValue != null) {
            final PersistentEntity oldTarget = getEntity(LinkValue.entryToLinkValue(oldValue).getEntityId());
            if (oldTarget.equals(target)) {
                return false;
            }
            links.delete(envTxn, entityLocalId, oldValue, target == null, linkId);
            txn.linkDeleted(fromId, oldTarget.getId(), linkId);
            oldTargetDeleted = true;
        }
        if (target == null) {
            return oldTargetDeleted;
        }
        final LinkValue linkValue = new LinkValue(target.getId(), linkId);
        links.put(envTxn, entityLocalId, LinkValue.linkValueToEntry(linkValue), oldValue == null, linkId);
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

    boolean deleteLinkInternal(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId, @NotNull final PersistentEntity to) {
        final boolean result = deleteLinkInternal(txn, from, linkId, to.getId());
        if (result) {
            final PersistentEntityId fromId = from.getId();
            final long entityLocalId = fromId.getLocalId();
            final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
            final int entityTypeId = fromId.getTypeId();
            final ByteIterable remainingValue = nonDebugLinkDataGetter.getUpToDateEntry(txn, entityTypeId, linkKey);
            if (remainingValue == null) {
                getLinksTable(txn, entityTypeId).deleteAllIndex(txn.getEnvironmentTransaction(), linkId, entityLocalId);
            }
        }
        return result;
    }

    @SuppressWarnings("ConstantConditions")
    void deleteAllLinks(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, final int linkId, @NotNull final EntityIterableBase existing) {
        final EntityIterator itr = existing.iterator();
        boolean deleted = false;
        while (itr.hasNext()) {
            deleted |= deleteLinkInternal(txn, from, linkId, (PersistentEntityId) itr.nextId());
        }
        if (deleted) {
            final PersistentEntityId fromId = from.getId();
            getLinksTable(txn, fromId.getTypeId()).deleteAllIndex(txn.getEnvironmentTransaction(), linkId, fromId.getLocalId());
        }
    }

    private boolean deleteLinkInternal(@NotNull final PersistentStoreTransaction txn, @NotNull final PersistentEntity from, int linkId, @NotNull final PersistentEntityId toId) {
        final PersistentEntityId fromId = from.getId();
        final int entityTypeId = fromId.getTypeId();
        final long entityLocalId = fromId.getLocalId();
        final PropertyKey linkKey = new PropertyKey(entityLocalId, linkId);
        final LinkValue linkValue = new LinkValue(toId, linkId);
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final LinksTable links = getLinksTable(txn, entityTypeId);

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
        if (useVersion1Format()) {
            final Store entities = getEntitiesTable(txn, id.getTypeId());
            final ByteIterable versionEntry = entities.get(txn.getEnvironmentTransaction(), LongBinding.longToCompressedEntry(id.getLocalId()));
            if (versionEntry == null) {
                return -1;
            }
            return IntegerBinding.compressedEntryToInt(versionEntry);
        }
        final Bitmap bitmap = getEntitiesBitmapTable(txn, id.getTypeId());
        final boolean exists = bitmap.get(txn.getEnvironmentTransaction(), id.getLocalId());
        return exists ? 0 : -1;
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
                    for (final Entity referrer : txn.findLinks(entityType, entity, linkName)) {
                        throw new EntityStoreException(entity +
                                " is about to be deleted, but it is referenced by " + referrer + ", link name: " + linkName);
                    }
                }
            }
        }
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final boolean result = useVersion1Format() ?
                getEntitiesTable(txn, entityTypeId).delete(envTxn, key) :
                getEntitiesBitmapTable(txn, entityTypeId).clear(envTxn, entityLocalId);
        if (result) {
            txn.entityDeleted(id);
        }
        return result;
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
        final LinksTable links = getLinksTable(txn, entityTypeId);
        final IntHashSet deletedLinks = new IntHashSet();
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
                    int linkId = key.getPropertyId();
                    if (getLinkName(txn, linkId) != null) {
                        deletedLinks.add(linkId);
                        final LinkValue linkValue = LinkValue.entryToLinkValue(valueEntry);
                        txn.linkDeleted(entity.getId(), (PersistentEntityId) linkValue.getEntityId(), linkValue.getLinkId());
                    }
                }
            }
        }
        for (Integer linkId : deletedLinks) {
            links.deleteAllIndex(envTxn, linkId, entityLocalId);
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
        Objects.requireNonNull(entityType, "Entity type cannot be null");
        return allowCreate ? entityTypes.getOrAllocateId(txn, entityType) : entityTypes.getId(txn, entityType);
    }

    public int getEntityTypeId(@NotNull final TxnProvider txnProvider, @NotNull final String entityType, final boolean allowCreate) {
        Objects.requireNonNull(entityType, "Entity type cannot be null");
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
            throw new EntityRemovedInDatabaseException("Invalid type id: " + entityTypeId);
        }
        return result;
    }

    @NotNull
    public String getEntityType(@NotNull final TxnProvider txnProvider, final int entityTypeId) {
        final String result = entityTypes.getName(txnProvider, entityTypeId);
        if (result == null) {
            throw new EntityRemovedInDatabaseException("Invalid type id: " + entityTypeId);
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
        blobHashesTables.remove(entityTypeId);

        final String entityTableName = namingRulez.getEntitiesTableName(entityTypeId);
        final String propertiesTableName = namingRulez.getPropertiesTableName(entityTypeId);
        final String linksTableName = namingRulez.getLinksTableName(entityTypeId);
        final String secondLinksTableName = TwoColumnTable.secondColumnDatabaseName(linksTableName);
        final String blobsObsoleteTableName = namingRulez.getBlobsObsoleteTableName(entityTypeId);
        final String blobsTableName = namingRulez.getBlobsTableName(entityTypeId);

        truncateStores(txn, Arrays.asList(
                        entityTableName, linksTableName, secondLinksTableName, propertiesTableName, blobsObsoleteTableName, blobsTableName),
                () -> new Iterator<String>() { // enumerate all property value indexes
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
                }
        );
    }

    private void truncateStores(@NotNull final PersistentStoreTransaction txn, @NotNull Iterable<String> unsafe, @NotNull Iterable<String> safe) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        for (final String name : unsafe) {
            if (environment.storeExists(name, envTxn)) {
                environment.truncateStore(name, envTxn);
            }
        }

        for (final String name : safe) {
            try {
                if (environment.storeExists(name, envTxn)) {
                    environment.truncateStore(name, envTxn);
                }
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
     * @return {@code < 0} if there is no such property and create=false, else id of the property
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
     * @return {@code < 0} if there is no such link and create=false, else id of the link
     */
    public int getLinkId(@NotNull final PersistentStoreTransaction txn, @NotNull final String linkName, final boolean allowCreate) {
        return allowCreate ? linkIds.getOrAllocateId(txn, linkName) : linkIds.getId(txn, linkName);
    }

    public int getLinkId(@NotNull final TxnProvider txnProvider, @NotNull final String linkName, final boolean allowCreate) {
        return allowCreate ? linkIds.getOrAllocateId(txnProvider, linkName) : linkIds.getId(txnProvider, linkName);
    }

    public void deleteLinkName(@NotNull final PersistentStoreTransaction txn, @NotNull final String linkName) {
        linkIds.delete(txn, linkName);
    }

    @Nullable
    public String getLinkName(@NotNull final PersistentStoreTransaction txn, final int linkId) {
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

    private void preloadTables(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        if (useVersion1Format()) {
            getEntitiesTable(txn, entityTypeId);
        } else {
            getEntitiesBitmapTable(txn, entityTypeId);
        }
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
    @Deprecated
    public Store getEntitiesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((SingleColumnTable) entitiesTables.get(txn, entityTypeId)).getDatabase();
    }

    public long getEntitiesCount(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        if (useVersion1Format()) {
            return getEntitiesTable(txn, entityTypeId).count(txn.getEnvironmentTransaction());
        }
        return getEntitiesBitmapTable(txn, entityTypeId).count(txn.getEnvironmentTransaction());
    }

    @NotNull
    public BitmapImpl getEntitiesBitmapTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return ((BitmapTable) entitiesTables.get(txn, entityTypeId)).getBitmap();
    }

    @NotNull
    public PropertiesTable getPropertiesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (PropertiesTable) propertiesTables.get(txn, entityTypeId);
    }

    @NotNull
    public LinksTable getLinksTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (LinksTable) linksTables.get(txn, entityTypeId);
    }

    @NotNull
    public BlobsTable getBlobsTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (BlobsTable) blobsTables.get(txn, entityTypeId);
    }

    @NotNull
    SingleColumnTable getBlobHashesTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        return (SingleColumnTable) blobHashesTables.get(txn, entityTypeId);
    }

    public Long getBlobFileLength(long blobHandle, Transaction txn) {
        final ByteIterable resultEntry = blobFileLengths.get(txn, LongBinding.longToCompressedEntry(blobHandle));
        return resultEntry == null ? null : LongBinding.compressedEntryToLong(resultEntry);
    }

    void setBlobFileLength(@NotNull final PersistentStoreTransaction txn, final long blobHandle, final long length) {
        if (length < 0) {
            throw new IllegalArgumentException("length < 0");
        }
        blobFileLengths.put(txn.getEnvironmentTransaction(),
                LongBinding.longToCompressedEntry(blobHandle), LongBinding.longToCompressedEntry(length));
    }

    void deleteBlobFileLength(@NotNull final PersistentStoreTransaction txn, final long blobHandle) {
        blobFileLengths.delete(txn.getEnvironmentTransaction(), LongBinding.longToCompressedEntry(blobHandle));
    }

    public long blobFileCount(@NotNull final PersistentStoreTransaction txn) {
        return blobFileLengths.count(txn.getEnvironmentTransaction());
    }

    public Iterable<Pair<Long, Long>> getBlobFileLengths(@NotNull final PersistentStoreTransaction txn) {
        return getBlobFileLengths(txn, 0L);
    }

    public Iterable<Pair<Long, Long>> getBlobFileLengths(@NotNull final PersistentStoreTransaction txn,
                                                         final long fromHandle) {
        return new BlobFileLengthsIterable(txn, fromHandle);
    }

    @Override
    @NotNull
    public EntityStoreSharedAsyncProcessor getAsyncProcessor() {
        return iterableCache.getProcessor();
    }

    @NotNull
    @Override
    public Statistics getStatistics() {
        return statistics;
    }

    public void setCloseEnvironment(boolean closeEnvironment) {
        this.closeEnvironment = closeEnvironment;
    }

    @Override
    public void close() {
        loggerDebug("Closing...");

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
                // by default, do not close underlying environment since it can be used also by another EntityStore or in a different way
                if (closeEnvironment) {
                    environment.close();
                }
            }
            iterableCache.clear();
            loggerDebug("Closed successfully.");
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

    static boolean isInPlaceBlobHandle(final long blobHandle) {
        return IN_PLACE_BLOB_HANDLE == blobHandle || IN_PLACE_BLOB_REFERENCE_HANDLE == blobHandle;
    }

    static boolean isEmptyOrInPlaceBlobHandle(final long blobHandle) {
        return EMPTY_BLOB_HANDLE == blobHandle || isInPlaceBlobHandle(blobHandle);
    }

    public static void loggerWarn(@NotNull final String message) {
        if (logger.isWarnEnabled()) {
            logger.warn(message);
        }
    }

    public static void loggerWarn(@NotNull final String message, @NotNull final Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.warn(message, t);
        }
    }

    public static void loggerDebug(@NotNull final String message) {
        if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    public static void loggerDebug(@NotNull final String message, @NotNull final Throwable t) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, t);
        }
    }

    private void safeTruncateStore(@NotNull final PersistentStoreTransaction txn, @NotNull final String dbName) {
        environment.truncateStore(dbName, txn.getEnvironmentTransaction());
    }

    private void deleteObsoleteBlobHandle(final long blobHandle, final PersistentStoreTransaction txn) {
        if (isEmptyOrInPlaceBlobHandle(blobHandle)) {
            return;
        }
        deleteBlobFileLength(txn, blobHandle);
        txn.deleteBlob(blobHandle);
        txn.deferBlobDeletion(blobHandle);
    }

    private PersistentEntity getEntityAssertPhantomLink(@NotNull final PersistentStoreTransaction txn, @NotNull final EntityId id) {
        final int version = getLastVersion(txn, id);
        if (version < 0) {
            throw new PhantomLinkException(getEntityType(txn, id.getTypeId()) + '[' + id + ']');
        }
        return new PersistentEntity(this, (PersistentEntityId) id);
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
        private Pair<Integer, LinksTable> lastUsedTable = new Pair<>(Integer.MIN_VALUE, null);

        private TwoColumnTable getTable(@NotNull final PersistentStoreTransaction txn,
                                        final int typeId) {
            final Pair<Integer, LinksTable> lastUsedTable = this.lastUsedTable;
            if (lastUsedTable.getFirst() == typeId) {
                return lastUsedTable.getSecond();
            }
            final LinksTable result = getLinksTable(txn, typeId);
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

    private class BlobFileLengthsIterable implements Iterable<Pair<Long, Long>> {
        @NotNull
        private final PersistentStoreTransaction txn;
        private final long fromHandle;

        BlobFileLengthsIterable(@NotNull PersistentStoreTransaction txn, final long fromHandle) {
            this.txn = txn;
            this.fromHandle = fromHandle;
        }

        @NotNull
        @Override
        public Iterator iterator() {
            return new Iterator();
        }

        private class Iterator implements java.util.Iterator<Pair<Long, Long>> {

            final Cursor cursor;
            Pair<Long, Long> next;
            boolean hasMore;

            public Iterator() {
                cursor = blobFileLengths.openCursor(txn.getEnvironmentTransaction());
                if (fromHandle == 0L) {
                    hasMore = true;
                    next = null;
                } else {
                    hasMore = cursor.getSearchKeyRange(LongBinding.longToCompressedEntry(fromHandle)) != null;
                    if (hasMore) {
                        next = newNext();
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                if (hasMore) {
                    if (next == null) {
                        if (cursor.getNext()) {
                            next = newNext();
                        } else {
                            cursor.close();
                            hasMore = false;
                        }
                    }
                }
                return hasMore;
            }

            @Override
            public Pair<Long, Long> next() {
                if (!hasNext()) return null;
                final Pair<Long, Long> result = next;
                next = null;
                return result;
            }

            @NotNull
            private Pair<Long, Long> newNext() {
                return new Pair<>(
                        LongBinding.compressedEntryToLong(cursor.getKey()),
                        LongBinding.compressedEntryToLong(cursor.getValue()));
            }
        }
    }
}
