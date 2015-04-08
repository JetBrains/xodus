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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.entitystore.tables.*;
import jetbrains.exodus.env.*;
import jetbrains.exodus.util.IOUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

final class PersistentEntityStoreRefactorings {

    private static final Log log = LogFactory.getLog(PersistentEntityStoreRefactorings.class);

    @NonNls
    private static final String TEMP_BLOBS_DIR = PersistentEntityStoreImpl.BLOBS_DIR + "-refactoring";

    @NotNull
    private final PersistentEntityStoreImpl store;

    PersistentEntityStoreRefactorings(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
    }

    void refactorDeleteRedundantBlobs() {
        final BlobVault blobVault = store.getBlobVault();
        if (blobVault instanceof FileSystemBlobVaultOld) {
            logInfo("Deleting redundant blobs...");
            final FileSystemBlobVaultOld fsBlobVault = (FileSystemBlobVaultOld) blobVault;
            final Long nextBlobHandle = store.computeInReadonlyTransaction(new StoreTransactionalComputable<Long>() {
                @Override
                public Long compute(@NotNull final StoreTransaction tx) {
                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                    return fsBlobVault.nextHandle(txn.getEnvironmentTransaction());
                }
            });
            for (int i = 0; i < 10000; ++i) {
                final File blob = fsBlobVault.getBlobLocation(nextBlobHandle + i);
                if (blob.exists()) {
                    if (blob.delete()) {
                        logInfo("Deleted " + blob);
                    } else {
                        log.error("Failed to delete " + blob);
                    }
                }
            }
        }
    }

    void refactorEntitiesTables() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                    final String sourceName = store.getNamingRules().getEntitiesTableName(entityTypeId);
                    final String targetName = sourceName + "_temp";
                    if (log.isInfoEnabled()) {
                        log.info("Refactoring " + sourceName + " to key-prefixed store.");
                    }
                    transactionalCopyAndRemoveEntitiesStore(sourceName, targetName);
                    transactionalCopyAndRemoveEntitiesStore(targetName, sourceName);
                }
            }
        });
    }

    void refactorCreateNullPropertyIndices() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (log.isInfoEnabled()) {
                        log.info("Refactoring creating null-value property indices for [" + entityType + ']');
                    }
                    safeExecuteRefactoringForEntityType(entityType,
                            new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction tx) {
                                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                    final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                                    final PropertiesTable props = store.getPropertiesTable(txn, entityTypeId);
                                    final Store allPropsIndex = props.getAllPropsIndex();
                                    final Cursor cursor = store.getPrimaryPropertyIndexCursor(txn, entityTypeId);
                                    final Transaction envTxn = txn.getEnvironmentTransaction();
                                    while (cursor.getNext()) {
                                        PropertyKey propertyKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                                        allPropsIndex.put(envTxn,
                                                IntegerBinding.intToCompressedEntry(propertyKey.getPropertyId()),
                                                LongBinding.longToCompressedEntry(propertyKey.getEntityLocalId()));
                                    }
                                    cursor.close();
                                }
                            }
                    );
                }
            }
        });
    }

    void refactorCreateNullBlobIndices() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (log.isInfoEnabled()) {
                        log.info("Refactoring creating null-value blob indices for [" + entityType + ']');
                    }
                    safeExecuteRefactoringForEntityType(entityType,
                            new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction tx) {
                                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                    final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                                    final BlobsTable blobs = store.getBlobsTable(txn, entityTypeId);
                                    final Store allBlobsIndex = blobs.getAllBlobsIndex();
                                    final Transaction envTxn = txn.getEnvironmentTransaction();
                                    final Cursor cursor = blobs.getPrimaryIndex().openCursor(envTxn);
                                    while (cursor.getNext()) {
                                        PropertyKey propertyKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                                        allBlobsIndex.put(envTxn,
                                                IntegerBinding.intToCompressedEntry(propertyKey.getPropertyId()),
                                                LongBinding.longToCompressedEntry(propertyKey.getEntityLocalId()));
                                    }
                                    cursor.close();
                                }
                            }
                    );
                }
            }
        });
    }

    void refactorMakeLinkTablesConsistent() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (log.isInfoEnabled()) {
                        log.info("Refactoring making links' tables consistent for [" + entityType + ']');
                    }
                    try {
                        final Collection<Pair<ByteIterable, ByteIterable>> badLinks = new ArrayList<>();
                        final Collection<Pair<ByteIterable, ByteIterable>> deleteLinks = new ArrayList<>();
                        final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                        final TwoColumnTable linksTable = store.getLinksTable(txn, entityTypeId);
                        final Transaction envTxn = txn.getEnvironmentTransaction();
                        final Cursor cursor = linksTable.getFirstIndexCursor(envTxn);
                        final Store entitiesTable = store.getEntitiesTable(txn, entityTypeId);
                        while (cursor.getNext()) {
                            final long localId = LongBinding.compressedEntryToLong(cursor.getKey());
                            if (entitiesTable.get(envTxn, LongBinding.longToCompressedEntry(localId)) == null) {
                                do {
                                    deleteLinks.add(new Pair<>(cursor.getKey(), cursor.getValue()));
                                } while (cursor.getNextDup());
                                continue;
                            }
                            final LinkValue linkValue = LinkValue.entryToLinkValue(cursor.getValue());
                            // if target doesn't exist
                            if (store.getLastVersion(txn, linkValue.getEntityId()) < 0) {
                                deleteLinks.add(new Pair<>(cursor.getKey(), cursor.getValue()));
                                continue;
                            }
                            if (linksTable.get2(envTxn, cursor.getValue()) == null) {
                                badLinks.add(new Pair<>(cursor.getKey(), cursor.getValue()));
                            }
                        }
                        cursor.close();
                        if (!badLinks.isEmpty()) {
                            store.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final Transaction txn) {
                                    for (final Pair<ByteIterable, ByteIterable> badLink : badLinks) {
                                        linksTable.put(txn, badLink.getFirst(), badLink.getSecond());
                                    }
                                }
                            });
                            if (log.isInfoEnabled()) {
                                log.info(badLinks.size() + " missing links found and fixed for [" + entityType + ']');
                            }
                        }
                        badLinks.clear();
                        final Cursor cursor2 = linksTable.getSecondIndexCursor(envTxn);
                        while (cursor2.getNext()) {
                            if (linksTable.get(envTxn, cursor2.getValue()) == null) {
                                badLinks.add(new Pair<>(cursor2.getKey(), cursor2.getValue()));
                            }
                        }
                        cursor2.close();
                        final int badLinksSize = badLinks.size();
                        final int deleteLinksSize = deleteLinks.size();
                        if (badLinksSize > 0 || deleteLinksSize > 0) {
                            store.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final Transaction txn) {
                                    for (final Pair<ByteIterable, ByteIterable> badLink : badLinks) {
                                        deletePair(linksTable.getSecondIndexCursor(txn), badLink.getFirst(), badLink.getSecond());
                                    }
                                    for (final Pair<ByteIterable, ByteIterable> deleteLink : deleteLinks) {
                                        deletePair(linksTable.getFirstIndexCursor(txn), deleteLink.getFirst(), deleteLink.getSecond());
                                        deletePair(linksTable.getSecondIndexCursor(txn), deleteLink.getSecond(), deleteLink.getFirst());
                                    }

                                }
                            });
                            if (log.isInfoEnabled()) {
                                if (badLinksSize > 0) {
                                    log.info(badLinksSize + " redundant links found and fixed for [" + entityType + ']');
                                }
                                if (deleteLinksSize > 0) {
                                    log.info(deleteLinksSize + " phantom links found and fixed for [" + entityType + ']');
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Failed to execute refactoring for entity type: " + entityType, t);
                        throwJVMError(t);
                    }
                }
            }
        });
    }

    void refactorMakePropTablesConsistent() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (log.isInfoEnabled()) {
                        log.info("Refactoring making props' tables consistent for [" + entityType + ']');
                    }
                    try {
                        final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                        final PropertiesTable propTable = store.getPropertiesTable(txn, entityTypeId);
                        final Transaction envTxn = txn.getEnvironmentTransaction();
                        final IntHashMap<LongHashMap<PropertyValue>> props = new IntHashMap<>();
                        final Cursor cursor = store.getPrimaryPropertyIndexCursor(txn, propTable);
                        while (cursor.getNext()) {
                            final PropertyKey propKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                            final PropertyValue propValue = store.getPropertyTypes().entryToPropertyValue(cursor.getValue());
                            final int propId = propKey.getPropertyId();
                            LongHashMap<PropertyValue> entitiesToValues = props.get(propId);
                            if (entitiesToValues == null) {
                                entitiesToValues = new LongHashMap<>();
                                props.put(propId, entitiesToValues);
                            }
                            entitiesToValues.put(propKey.getEntityLocalId(), propValue);
                        }
                        cursor.close();
                        final List<Pair<Integer, Pair<ByteIterable, ByteIterable>>> missingPairs = new ArrayList<>();
                        final IntHashMap<Set<Long>> allPropsMap = new IntHashMap<>();
                        for (final int propId : props.keySet()) {
                            final Store valueIndex = propTable.getValueIndex(txn, propId, false);
                            final Cursor valueCursor = valueIndex == null ? null : valueIndex.openCursor(envTxn);
                            final LongHashMap<PropertyValue> entitiesToValues = props.get(propId);
                            final Set<Long> localIdSet = entitiesToValues.keySet();
                            final TreeSet<Long> sortedLocalIdSet = new TreeSet<>(localIdSet);
                            allPropsMap.put(propId, sortedLocalIdSet);
                            final Long[] localIds = sortedLocalIdSet.toArray(new Long[entitiesToValues.size()]);
                            for (final long localId : localIds) {
                                final PropertyValue propValue = entitiesToValues.get(localId);
                                final ByteIterable secondaryKey = PropertiesTable.createSecondaryKey(store.getPropertyTypes(), PropertyTypes.propertyValueToEntry(propValue), propValue.getType());
                                final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
                                if (valueCursor == null || !valueCursor.getSearchBoth(secondaryKey, secondaryValue)) {
                                    missingPairs.add(new Pair<>(propId, new Pair<>(secondaryKey, secondaryValue)));
                                }
                            }
                            if (valueCursor != null) {
                                valueCursor.close();
                            }
                        }
                        if (!missingPairs.isEmpty()) {
                            store.executeInTransaction(new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction tx) {
                                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                    for (final Pair<Integer, Pair<ByteIterable, ByteIterable>> pair : missingPairs) {
                                        final Store valueIndex = propTable.getValueIndex(txn, pair.getFirst(), true);
                                        final Pair<ByteIterable, ByteIterable> missing = pair.getSecond();
                                        if (valueIndex == null) {
                                            throw new NullPointerException("Can't be");
                                        }
                                        valueIndex.put(txn.getEnvironmentTransaction(), missing.getFirst(), missing.getSecond());
                                    }
                                }
                            });
                            if (log.isInfoEnabled()) {
                                log.info(missingPairs.size() + " missing secondary keys found and fixed for [" + entityType + ']');
                            }
                        }
                        final List<Pair<Integer, Pair<ByteIterable, ByteIterable>>> phantomPairs = new ArrayList<>();
                        for (final Map.Entry<Integer, Store> entry : propTable.getValueIndices()) {
                            final int propId = entry.getKey();
                            final LongHashMap<PropertyValue> entitiesToValues = props.get(propId);
                            final Cursor c = entry.getValue().openCursor(envTxn);
                            while (c.getNext()) {
                                final ByteIterable keyEntry = c.getKey();
                                final ByteIterable valueEntry = c.getValue();
                                final PropertyValue propValue = entitiesToValues.get(LongBinding.compressedEntryToLong(valueEntry));
                                if (propValue == null) {
                                    phantomPairs.add(new Pair<>(propId, new Pair<>(keyEntry, valueEntry)));
                                } else {
                                    final ComparableBinding objectBinding = propValue.getBinding();
                                    final Comparable value;
                                    try {
                                        value = objectBinding.entryToObject(keyEntry);
                                    } catch (Throwable t) {
                                        throwJVMError(t);
                                        phantomPairs.add(new Pair<>(propId, new Pair<>(keyEntry, valueEntry)));
                                        continue;
                                    }
                                    final Comparable data = propValue.getData();
                                    //noinspection unchecked
                                    if (!data.getClass().equals(value.getClass()) || PropertyTypes.toLowerCase(data).compareTo(value) != 0) {
                                        phantomPairs.add(new Pair<>(propId, new Pair<>(keyEntry, valueEntry)));
                                    }
                                }
                            }
                            c.close();
                        }
                        if (!phantomPairs.isEmpty()) {
                            store.executeInTransaction(new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction tx) {
                                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                    final Transaction envTxn = txn.getEnvironmentTransaction();
                                    for (final Pair<Integer, Pair<ByteIterable, ByteIterable>> pair : phantomPairs) {
                                        final Store valueIndex = propTable.getValueIndex(txn, pair.getFirst(), true);
                                        final Pair<ByteIterable, ByteIterable> phantom = pair.getSecond();
                                        if (valueIndex == null) {
                                            throw new NullPointerException("Can't be");
                                        }
                                        deletePair(valueIndex.openCursor(envTxn), phantom.getFirst(), phantom.getSecond());
                                    }
                                }
                            });
                            if (log.isInfoEnabled()) {
                                log.info(phantomPairs.size() + " phantom secondary keys found and fixed for [" + entityType + ']');
                            }
                        }
                        final List<Pair<Integer, Long>> phantomIds = new ArrayList<>();
                        final Cursor c = propTable.getAllPropsIndex().openCursor(envTxn);
                        while (c.getNext()) {
                            final int propId = IntegerBinding.compressedEntryToInt(c.getKey());
                            final long localId = LongBinding.compressedEntryToLong(c.getValue());
                            final Set<Long> localIds = allPropsMap.get(propId);
                            if (localIds == null || !localIds.remove(localId)) {
                                phantomIds.add(new Pair<>(propId, localId));
                            } else {
                                if (localIds.isEmpty()) {
                                    allPropsMap.remove(propId);
                                }
                            }
                        }
                        c.close();
                        if (!allPropsMap.isEmpty()) {
                            final int[] added = {0};
                            store.executeInTransaction(new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction txn) {
                                    int count = 0;
                                    final Store allPropsIndex = propTable.getAllPropsIndex();
                                    final Transaction envTxn = ((PersistentStoreTransaction) txn).getEnvironmentTransaction();
                                    for (Map.Entry<Integer, Set<Long>> entry : allPropsMap.entrySet()) {
                                        final ArrayByteIterable keyEntry = IntegerBinding.intToCompressedEntry(entry.getKey());
                                        for (long localId : entry.getValue()) {
                                            allPropsIndex.put(envTxn, keyEntry, LongBinding.longToCompressedEntry(localId));
                                            ++count;
                                        }
                                    }
                                    added[0] = count;
                                }
                            });
                            if (log.isInfoEnabled()) {
                                log.info(added[0] + " missing id pairs found and fixed for [" + entityType + ']');
                            }
                        }
                        if (!phantomIds.isEmpty()) {
                            store.executeInTransaction(new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction txn) {
                                    final Store allPropsIndex = propTable.getAllPropsIndex();
                                    final Transaction envTxn = ((PersistentStoreTransaction) txn).getEnvironmentTransaction();
                                    final Cursor c = allPropsIndex.openCursor(envTxn);
                                    for (final Pair<Integer, Long> phantom : phantomIds) {
                                        if (!c.getSearchBoth(IntegerBinding.intToCompressedEntry(phantom.getFirst()), LongBinding.longToCompressedEntry(phantom.getSecond()))) {
                                            throw new EntityStoreException("Can't be");
                                        }
                                        c.deleteCurrent();
                                    }
                                    c.close();
                                }
                            });
                            if (log.isInfoEnabled()) {
                                log.info(phantomIds.size() + " phantom id pairs found and fixed for [" + entityType + ']');
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Failed to execute refactoring for entity type: " + entityType, t);
                        throwJVMError(t);
                    }
                }
            }
        });
    }

    void refactorInPlaceBlobs(@NotNull final FileSystemBlobVaultOld oldVault,
                              @NotNull final String blobHandlesSequenceName) {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                // reset blob handles sequence
                final PersistentSequence sequence = store.getSequence(txn, blobHandlesSequenceName);
                store.executeInTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        sequence.set(-1L);
                    }
                });
                // create new vault
                final FileSystemBlobVault newVault;
                try {
                    newVault = new FileSystemBlobVault(store.getLocation(), TEMP_BLOBS_DIR,
                            PersistentEntityStoreImpl.BLOBS_EXTENSION, new PersistentSequenceBlobHandleGenerator(sequence));
                    store.setBlobVault(newVault);
                } catch (IOException e) {
                    throw ExodusException.toEntityStoreException(e);
                }
                for (final String entityType : store.getEntityTypes(txn)) {
                    try {
                        final List<Pair<PropertyKey, Long>> blobHandles = new ArrayList<>();
                        final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                        final BlobsTable blobs = store.getBlobsTable(txn, entityTypeId);
                        final Transaction envTxn = txn.getEnvironmentTransaction();
                        try (Cursor cursor = blobs.getPrimaryIndex().openCursor(envTxn)) {
                            while (cursor.getNext()) {
                                final long blobHandle = LongBinding.compressedEntryToLong(cursor.getValue());
                                if (!PersistentEntityStoreImpl.isEmptyOrInPlaceBlobHandle(blobHandle)) {
                                    final PropertyKey key = PropertyKey.entryToPropertyKey(cursor.getKey());
                                    blobHandles.add(new Pair<>(key, blobHandle));
                                }
                            }
                        }
                        if (!blobHandles.isEmpty()) {
                            safeExecuteRefactoringForEntityType(entityType,
                                    new StoreTransactionalExecutable() {
                                        @Override
                                        public void execute(@NotNull final StoreTransaction tx) {
                                            final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                            final int blobsCount = blobHandles.size();
                                            int i = 0;
                                            for (final Pair<PropertyKey, Long> entry : blobHandles) {
                                                if ((i++ % 1000) == 0) {
                                                    if (log.isInfoEnabled()) {
                                                        log.info("Refactoring moving tiny blobs into database for [" + entityType +
                                                                "]. Blobs processed: " + i + " of " + blobsCount);
                                                    }
                                                }
                                                final PropertyKey key = entry.getFirst();
                                                final PersistentEntity entity = new PersistentEntity(
                                                        store, new PersistentEntityId(entityTypeId, key.getEntityLocalId()));
                                                final String blobName = store.getPropertyName(txn, key.getPropertyId());
                                                if (blobName == null) {
                                                    throw new NullPointerException("Blob name is expected");
                                                }
                                                try {
                                                    final long blobHandle = entry.getSecond();
                                                    txn.preserveBlob(blobHandle);
                                                    store.setBlob(txn, entity, blobName, oldVault.getBlobLocation(blobHandle));
                                                } catch (IOException e) {
                                                    log.error("Failed to set blob", e);
                                                }
                                            }
                                        }
                                    }
                            );
                            if (log.isInfoEnabled()) {
                                log.info("Refactoring moving tiny blobs into database for [" + entityType + "] completed.");
                            }
                        }
                    } catch (ReadonlyTransactionException ignore) {
                        // that fixes XD-377
                    }
                }
                oldVault.close();
                newVault.close();
                logInfo("Moving finished, deleting old vault...");
                final File oldVaultLocation = oldVault.getVaultLocation();
                IOUtil.deleteRecursively(oldVaultLocation);
                if (!oldVaultLocation.delete()) {
                    throw new EntityStoreException("Failed to delete old blob vault");
                }
                if (!newVault.getVaultLocation().renameTo(oldVaultLocation)) {
                    throw new EntityStoreException("Failed to rename temporary blob vault");
                }
                try {
                    store.setBlobVault(new FileSystemBlobVault(store.getLocation(),
                            PersistentEntityStoreImpl.BLOBS_DIR,
                            PersistentEntityStoreImpl.BLOBS_EXTENSION,
                            new PersistentSequenceBlobHandleGenerator(sequence)));
                } catch (IOException e) {
                    throw ExodusException.toEntityStoreException(e);
                }
                logInfo("Refactoring moving tiny blobs into database finished successfully.");
            }
        });
    }

    private void safeExecuteRefactoringForEntityType(@NotNull final String entityType,
                                                     @NotNull final StoreTransactionalExecutable executable) {
        try {
            store.executeInTransaction(executable);
        } catch (Throwable t) {
            log.error("Failed to execute refactoring for entity type: " + entityType, t);
            throwJVMError(t);
        }
    }

    private void transactionalCopyAndRemoveEntitiesStore(@NotNull final String sourceName, @NotNull final String targetName) {
        final Environment env = store.getEnvironment();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final Store store = env.openStore(sourceName, StoreConfig.USE_EXISTING, txn);
                final Store storeCopy = env.openStore(targetName, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
                final Cursor cursor = store.openCursor(txn);
                ArrayByteIterable lastKey = null;
                while (cursor.getNext()) {
                    final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
                    if (lastKey != null && lastKey.compareTo(key) >= 0) {
                        throw new IllegalStateException("Invalid key order");
                    }
                    storeCopy.putRight(txn, key, new ArrayByteIterable(cursor.getValue()));
                    lastKey = key;
                }
                cursor.close();
                env.removeStore(sourceName, txn);
            }
        });
    }

    private static void deletePair(@NotNull final Cursor c, @NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        if (c.getSearchBoth(key, value)) {
            c.deleteCurrent();
        }
        c.close();
    }

    private static void throwJVMError(@NotNull final Throwable t) {
        if (t instanceof VirtualMachineError) {
            throw new EntityStoreException(t);
        }
    }

    private static void logInfo(@NotNull final String message) {
        if (log.isInfoEnabled()) {
            log.info(message);
        }
    }
}