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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.*;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet;
import jetbrains.exodus.entitystore.tables.*;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

final class PersistentEntityStoreRefactorings {

    private static final Logger logger = LoggerFactory.getLogger(PersistentEntityStoreRefactorings.class);

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
                        logger.error("Failed to delete " + blob);
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
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring " + sourceName + " to key-prefixed store.");
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
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring creating null-value property indices for [" + entityType + ']');
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
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring creating null-value blob indices for [" + entityType + ']');
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

    void refactorCreateNullLinkIndices() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring creating null-value link indices for [" + entityType + ']');
                    }
                    safeExclusiveExecuteRefactoringForEntityType(entityType,
                            new StoreTransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final StoreTransaction tx) {
                                    final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                                    final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                                    LinksTable links = store.getLinksTable(txn, entityTypeId);
                                    final Store allLinksIndex = links.getAllLinksIndex();
                                    final Transaction envTxn = txn.getEnvironmentTransaction();
                                    if (allLinksIndex.count(envTxn) > 0) {
                                        if (logger.isWarnEnabled()) {
                                            logger.warn("Refactoring creating null-value link indices looped for [" + entityType + ']');
                                        }
                                        envTxn.getEnvironment().truncateStore(allLinksIndex.getName(), envTxn);
                                        store.linksTables.remove(entityTypeId);
                                        links = store.getLinksTable(txn, entityTypeId);
                                    }
                                    final Transaction readonlySnapshot = envTxn.getReadonlySnapshot();
                                    try {
                                        final Cursor cursor = links.getSecondIndexCursor(readonlySnapshot);
                                        final long total = links.getSecondaryCount(readonlySnapshot);
                                        long done = 0;
                                        int prevLinkId = -1;
                                        int linkId = -1;
                                        PersistentLongSet.MutableSet idSet = new PersistentLong23TreeSet().beginWrite();
                                        final String format = "done %4.1f%% for " + entityType;
                                        while (cursor.getNext()) {
                                            final PropertyKey linkKey = PropertyKey.entryToPropertyKey(cursor.getValue());
                                            linkId = linkKey.getPropertyId();
                                            final long entityLocalId = linkKey.getEntityLocalId();
                                            idSet.add(entityLocalId);
                                            if (prevLinkId != linkId) {
                                                if (prevLinkId == -1) {
                                                    prevLinkId = linkId;
                                                } else {
                                                    if (linkId < prevLinkId) {
                                                        throw new IllegalStateException("Unsorted index");
                                                    }
                                                    done = dumpSetAndFlush(format, allLinksIndex, txn, total, done, prevLinkId, idSet);
                                                    prevLinkId = linkId;
                                                    linkId = -1;
                                                }
                                            }
                                        }
                                        if (linkId != -1) {
                                            dumpSetAndFlush(format, allLinksIndex, txn, total, done, linkId, idSet);
                                        }
                                        cursor.close();
                                    } finally {
                                        readonlySnapshot.abort();
                                    }
                                }

                                private long dumpSetAndFlush(String format, Store allLinksIndex, PersistentStoreTransaction txn, double total, long done, int prevLinkId, PersistentLongSet.MutableSet idSet) {
                                    final LongIterator itr = idSet.longIterator();
                                    while (itr.hasNext()) {
                                        allLinksIndex.putRight(txn.getEnvironmentTransaction(), IntegerBinding.intToCompressedEntry(prevLinkId), LongBinding.longToCompressedEntry(itr.next()));
                                        done++;
                                        if (done % 10000 == 0 && logger.isInfoEnabled()) {
                                            logger.info(String.format(format, ((double) done * 100) / total));
                                        }
                                        if (done % 100000 == 0 && !txn.flush()) {
                                            throw new IllegalStateException("cannot flush");
                                        }
                                    }
                                    idSet.clear();
                                    return done;
                                }
                            }
                    );
                }
            }
        });
    }

    void refactorDropEmptyPrimaryLinkTables() {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;

                for (final String entityType : store.getEntityTypes(txn)) {
                    runReadonlyTransactionSafeForEntityType(entityType, new Runnable() {
                        @Override
                        public void run() {
                            final Transaction envTxn = txn.getEnvironmentTransaction();
                            final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                            final LinksTable linksTable = store.getLinksTable(txn, entityTypeId);

                            final long primaryCount = linksTable.getPrimaryCount(envTxn);
                            if (primaryCount != 0L && linksTable.getSecondaryCount(envTxn) == 0L) {
                                store.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                    @Override
                                    public void execute(@NotNull Transaction txn) {
                                        linksTable.truncateFirst(txn);
                                    }
                                });

                                if (logger.isInfoEnabled()) {
                                    logger.info("Drop links' tables when primary index is empty for [" + entityType + ']');
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    void refactorMakeLinkTablesConsistent(final Store internalSettings) {
        store.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction tx) {
                final PersistentStoreTransaction txn = (PersistentStoreTransaction) tx;
                for (final String entityType : store.getEntityTypes(txn)) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring making links' tables consistent for [" + entityType + ']');
                    }
                    runReadonlyTransactionSafeForEntityType(entityType, new Runnable() {
                        @Override
                        public void run() {
                            final Collection<Pair<ByteIterable, ByteIterable>> redundantLinks = new ArrayList<>();
                            final Collection<Pair<ByteIterable, ByteIterable>> deleteLinks = new ArrayList<>();
                            final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                            final LinksTable linksTable = store.getLinksTable(txn, entityTypeId);
                            final Transaction envTxn = txn.getEnvironmentTransaction();
                            final LongSet all = new PackedLongHashSet();
                            final LongSet linkFilter = new PackedLongHashSet();
                            try (Cursor cursor = store.getEntitiesIndexCursor(txn, entityTypeId)) {
                                while (cursor.getNext()) {
                                    all.add(LongBinding.compressedEntryToLong(cursor.getKey()));
                                }
                            }
                            final IntHashSet redundantLinkTypes = new IntHashSet();
                            final IntHashSet deletedLinkTypes = new IntHashSet();
                            final IntHashSet deletedLinkIds = new IntHashSet();
                            try (Cursor cursor = linksTable.getFirstIndexCursor(envTxn)) {
                                while (cursor.getNext()) {
                                    final ByteIterable first = cursor.getKey();
                                    final ByteIterable second = cursor.getValue();
                                    LinkValue linkValue = null;
                                    final long localId = LongBinding.compressedEntryToLong(first);
                                    if (!all.contains(localId)) {
                                        try {
                                            linkValue = LinkValue.entryToLinkValue(second);
                                            deletedLinkTypes.add(linkValue.getEntityId().getTypeId());
                                            deletedLinkIds.add(linkValue.getLinkId());
                                        } catch (ArrayIndexOutOfBoundsException ignore) {
                                        }
                                        do {
                                            deleteLinks.add(new Pair<>(first, second));
                                        } while (cursor.getNextDup());
                                        continue;
                                    } else {
                                        linkFilter.add((first.hashCode() << 31L) + second.hashCode());
                                    }
                                    if (linkValue == null) {
                                        try {
                                            linkValue = LinkValue.entryToLinkValue(second);
                                        } catch (ArrayIndexOutOfBoundsException ignore) {
                                            deleteLinks.add(new Pair<>(first, second));
                                        }
                                    }
                                    if (linkValue != null) {
                                        final EntityId targetEntityId = linkValue.getEntityId();
                                        // if target doesn't exist
                                        if (store.getLastVersion(txn, targetEntityId) < 0) {
                                            deletedLinkTypes.add(targetEntityId.getTypeId());
                                            deletedLinkIds.add(linkValue.getLinkId());
                                            deleteLinks.add(new Pair<>(first, second));
                                            continue;
                                        } else {
                                            linkFilter.add((first.hashCode() << 31L) + second.hashCode());
                                        }
                                        if (!linksTable.contains2(envTxn, first, second)) {
                                            redundantLinkTypes.add(targetEntityId.getTypeId());
                                            redundantLinks.add(new Pair<>(first, second));
                                        }
                                    }
                                }
                            }
                            if (!redundantLinks.isEmpty()) {
                                store.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                    @Override
                                    public void execute(@NotNull final Transaction txn) {
                                        for (final Pair<ByteIterable, ByteIterable> badLink : redundantLinks) {
                                            linksTable.put(txn, badLink.getFirst(), badLink.getSecond());
                                        }
                                    }
                                });
                                if (logger.isInfoEnabled()) {
                                    logger.info(redundantLinks.size() + " missing links found for [" + entityType + ']');
                                }
                                redundantLinks.clear();
                            }
                            try (Cursor cursor = linksTable.getSecondIndexCursor(envTxn)) {
                                while ((cursor.getNext())) {
                                    final ByteIterable second = cursor.getKey();
                                    final ByteIterable first = cursor.getValue();
                                    if (!linkFilter.contains((first.hashCode() << 31L) + second.hashCode())) {
                                        if (!linksTable.contains(envTxn, first, second)) {
                                            redundantLinks.add(new Pair<>(first, second));
                                        }
                                    }
                                }
                            }
                            final int redundantLinksSize = redundantLinks.size();
                            final int deleteLinksSize = deleteLinks.size();
                            if (redundantLinksSize > 0 || deleteLinksSize > 0) {
                                store.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                    @Override
                                    public void execute(@NotNull final Transaction txn) {
                                        for (final Pair<ByteIterable, ByteIterable> redundantLink : redundantLinks) {
                                            deletePair(linksTable.getSecondIndexCursor(txn), redundantLink.getFirst(), redundantLink.getSecond());
                                        }
                                        for (final Pair<ByteIterable, ByteIterable> deleteLink : deleteLinks) {
                                            deletePair(linksTable.getFirstIndexCursor(txn), deleteLink.getFirst(), deleteLink.getSecond());
                                            deletePair(linksTable.getSecondIndexCursor(txn), deleteLink.getSecond(), deleteLink.getFirst());
                                        }

                                    }
                                });
                                if (logger.isInfoEnabled()) {
                                    if (redundantLinksSize > 0) {
                                        final ArrayList<String> redundantLinkTypeNames = new ArrayList<>(redundantLinkTypes.size());
                                        for (final int typeId : redundantLinkTypes) {
                                            redundantLinkTypeNames.add(store.getEntityType(txn, typeId));
                                        }
                                        logger.info(redundantLinksSize + " redundant links found and fixed for [" + entityType + "] and targets: " + redundantLinkTypeNames);
                                    }
                                    if (deleteLinksSize > 0) {
                                        final ArrayList<String> deletedLinkTypeNames = new ArrayList<>(deletedLinkTypes.size());
                                        for (final int typeId : deletedLinkTypes) {
                                            try {
                                                final String entityTypeName = store.getEntityType(txn, typeId);
                                                deletedLinkTypeNames.add(entityTypeName);
                                            } catch (Throwable t) {
                                                // ignore
                                            }
                                        }
                                        final ArrayList<String> deletedLinkIdsNames = new ArrayList<>(deletedLinkIds.size());
                                        for (final int typeId : deletedLinkIds) {
                                            try {
                                                final String linkName = store.getLinkName(txn, typeId);
                                                deletedLinkIdsNames.add(linkName);
                                            } catch (Throwable t) {
                                                // ignore
                                            }
                                        }
                                        logger.info(deleteLinksSize + " phantom links found and fixed for [" + entityType + "] and targets: " + deletedLinkTypeNames);
                                        logger.info("Link types: " + deletedLinkIdsNames);
                                    }
                                }
                            }
                            Settings.delete(internalSettings, "Link null-indices present"); // reset link null indices
                        }
                    });
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
                    if (logger.isInfoEnabled()) {
                        logger.info("Refactoring making props' tables consistent for [" + entityType + ']');
                    }
                    runReadonlyTransactionSafeForEntityType(entityType, new Runnable() {
                        @Override
                        public void run() {
                            final int entityTypeId = store.getEntityTypeId(txn, entityType, false);
                            final PropertiesTable propTable = store.getPropertiesTable(txn, entityTypeId);
                            final Transaction envTxn = txn.getEnvironmentTransaction();
                            final IntHashMap<LongHashMap<PropertyValue>> props = new IntHashMap<>();
                            final Cursor cursor = store.getPrimaryPropertyIndexCursor(txn, propTable);
                            final PropertyTypes propertyTypes = store.getPropertyTypes();
                            while (cursor.getNext()) {
                                final PropertyKey propKey = PropertyKey.entryToPropertyKey(cursor.getKey());
                                final PropertyValue propValue = propertyTypes.entryToPropertyValue(cursor.getValue());
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
                                    for (final ByteIterable secondaryKey : PropertiesTable.createSecondaryKeys(
                                            propertyTypes, PropertyTypes.propertyValueToEntry(propValue), propValue.getType())) {
                                        final ByteIterable secondaryValue = LongBinding.longToCompressedEntry(localId);
                                        if (valueCursor == null || !valueCursor.getSearchBoth(secondaryKey, secondaryValue)) {
                                            missingPairs.add(new Pair<>(propId, new Pair<>(secondaryKey, secondaryValue)));
                                        }
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
                                if (logger.isInfoEnabled()) {
                                    logger.info(missingPairs.size() + " missing secondary keys found and fixed for [" + entityType + ']');
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
                                    if (propValue != null) {
                                        final Comparable data = propValue.getData();
                                        final int typeId = propValue.getType().getTypeId();
                                        final Class<? extends Comparable> dataClass;
                                        final ComparableBinding objectBinding;
                                        if (typeId == ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
                                            //noinspection unchecked
                                            dataClass = ((ComparableSet) data).getItemClass();
                                            //noinspection ConstantConditions
                                            objectBinding = propertyTypes.getPropertyType(dataClass).getBinding();
                                        } else {
                                            dataClass = data.getClass();
                                            objectBinding = propValue.getBinding();
                                        }
                                        final Comparable value;
                                        try {
                                            value = objectBinding.entryToObject(keyEntry);
                                            if (dataClass.equals(value.getClass())) {
                                                if (typeId == ComparableValueType.COMPARABLE_SET_VALUE_TYPE) {
                                                    //noinspection unchecked
                                                    if (((ComparableSet) data).containsItem(value)) {
                                                        continue;
                                                    }
                                                } else //noinspection unchecked
                                                    if (PropertyTypes.toLowerCase(data).compareTo(value) == 0) {
                                                        continue;
                                                    }
                                            }
                                        } catch (Throwable t) {
                                            logger.error("Error reading property value index ", t);
                                            throwJVMError(t);
                                        }
                                    }
                                    phantomPairs.add(new Pair<>(propId, new Pair<>(keyEntry, valueEntry)));
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
                                if (logger.isInfoEnabled()) {
                                    logger.info(phantomPairs.size() + " phantom secondary keys found and fixed for [" + entityType + ']');
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
                                if (logger.isInfoEnabled()) {
                                    logger.info(added[0] + " missing id pairs found and fixed for [" + entityType + ']');
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
                                if (logger.isInfoEnabled()) {
                                    logger.info(phantomIds.size() + " phantom id pairs found and fixed for [" + entityType + ']');
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    void refactorRemoveHistoryStores() {
        final Environment environment = store.getEnvironment();
        environment.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final String persistentStoreName = store.getName();
                for (final String storeName : environment.getAllStoreNames(txn)) {
                    if (storeName.startsWith(persistentStoreName) && storeName.endsWith("#history")) {
                        environment.executeInTransaction(new TransactionalExecutable() {
                            @Override
                            public void execute(@NotNull final Transaction txn) {
                                environment.removeStore(storeName, txn);
                            }
                        });
                    }
                }
            }
        });
    }

    private void safeExecuteRefactoringForEntityType(@NotNull final String entityType,
                                                     @NotNull final StoreTransactionalExecutable executable) {
        try {
            store.executeInTransaction(executable);
        } catch (Throwable t) {
            logger.error("Failed to execute refactoring for entity type: " + entityType, t);
            throwJVMError(t);
        }
    }

    private void safeExclusiveExecuteRefactoringForEntityType(@NotNull final String entityType,
                                                     @NotNull final StoreTransactionalExecutable executable) {
        try {
            store.executeInTransaction(executable);
        } catch (Throwable t) {
            logger.error("Failed to execute refactoring for entity type: " + entityType, t);
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

    private static void runReadonlyTransactionSafeForEntityType(@NotNull final String entityType,
                                                                @NotNull final Runnable runnable) {
        try {
            runnable.run();
        } catch (ReadonlyTransactionException ignore) {
            // that fixes XD-377, XD-492 and similar not reported issues
        } catch (Throwable t) {
            logger.error("Failed to execute refactoring for entity type: " + entityType, t);
            throwJVMError(t);
        }
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
        if (logger.isInfoEnabled()) {
            logger.info(message);
        }
    }
}
