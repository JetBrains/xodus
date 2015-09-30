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

import jetbrains.exodus.AbstractConfig;
import jetbrains.exodus.ConfigurationStrategy;
import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("UnusedDeclaration")
public final class PersistentEntityStoreConfig extends AbstractConfig {

    public static final PersistentEntityStoreConfig DEFAULT = new PersistentEntityStoreConfig(ConfigurationStrategy.IGNORE);

    public static final String REFACTORING_SKIP_ALL = "exodus.entityStore.refactoring.skipAll";

    public static final String REFACTORING_NULL_INDICES = "exodus.entityStore.refactoring.nullIndices";

    public static final String REFACTORING_BLOB_NULL_INDICES = "exodus.entityStore.refactoring.blobNullIndices";

    public static final String REFACTORING_HEAVY_LINKS = "exodus.entityStore.refactoring.heavyLinks";

    public static final String REFACTORING_HEAVY_PROPS = "exodus.entityStore.refactoring.heavyProps";

    public static final String REFACTORING_DELETE_REDUNDANT_BLOBS = "exodus.entityStore.refactoring.deleteRedundantBlobs";

    public static final String MAX_IN_PLACE_BLOB_SIZE = "exodus.entityStore.maxInPlaceBlobSize";

    public static final String BLOB_STRINGS_CACHE_SIZE = "exodus.entityStore.blobStringsCacheSize";

    public static final String CACHING_DISABLED = "exodus.entityStore.cachingDisabled";

    public static final String REORDERING_DISABLED = "exodus.entityStore.reorderingDisabled";

    public static final String EXPLAIN_ON = "exodus.entityStore.explainOn";

    public static final String UNIQUE_INDICES_USE_BTREE = "exodus.entityStore.uniqueIndices.useBtree";

    public static final String DEBUG_LINK_DATA_GETTER = "exodus.entityStore.debugLinkDataGetter";

    public static final String ENTITY_ITERABLE_CACHE_SIZE = "exodus.entityStore.entityIterableCache.size";

    public static final String ENTITY_ITERABLE_CACHE_THREAD_COUNT = "exodus.entityStore.entityIterableCache.threadCount";

    public static final String ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT = "exodus.entityStore.entityIterableCache.cachingTimeout";

    public static final String ENTITY_ITERABLE_CACHE_DEFERRED_DELAY = "exodus.entityStore.entityIterableCache.deferredDelay"; // in milliseconds

    public static final String ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE = "exodus.entityStore.entityIterableCache.maxSizeOfDirectValue";

    public static final String ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE = "exodus.entityStore.entityIterableCache.useHumanReadable";

    public static final String TRANSACTION_PROPS_CACHE_SIZE = "exodus.entityStore.transaction.propsCacheSize";

    public static final String TRANSACTION_LINKS_CACHE_SIZE = "exodus.entityStore.transaction.linksCacheSize";

    public static final String TRANSACTION_BLOB_STRINGS_CACHE_SIZE = "exodus.entityStore.transaction.blobStringsCacheSize";

    public static final String GATHER_STATISTICS = "exodus.entityStore.gatherStatistics";

    public static final String MANAGEMENT_ENABLED = "exodus.entityStore.managementEnabled";

    private static final int MAX_DEFAULT_ENTITY_ITERABLE_CACHE_SIZE = 4096;

    public PersistentEntityStoreConfig() {
        this(ConfigurationStrategy.SYSTEM_PROPERTY);
    }

    public PersistentEntityStoreConfig(@NotNull final ConfigurationStrategy strategy) {
        //noinspection unchecked
        super(new Pair[]{
                new Pair(REFACTORING_SKIP_ALL, false),
                new Pair(REFACTORING_NULL_INDICES, false),
                new Pair(REFACTORING_BLOB_NULL_INDICES, false),
                new Pair(REFACTORING_HEAVY_LINKS, false),
                new Pair(REFACTORING_HEAVY_PROPS, false),
                new Pair(REFACTORING_DELETE_REDUNDANT_BLOBS, false),
                new Pair(MAX_IN_PLACE_BLOB_SIZE, 10000),
                new Pair(BLOB_STRINGS_CACHE_SIZE, 2000),
                new Pair(CACHING_DISABLED, false),
                new Pair(REORDERING_DISABLED, false),
                new Pair(EXPLAIN_ON, false),
                new Pair(UNIQUE_INDICES_USE_BTREE, false),
                new Pair(DEBUG_LINK_DATA_GETTER, false),
                new Pair(ENTITY_ITERABLE_CACHE_SIZE, defaultEntityIterableCacheSize()),
                new Pair(ENTITY_ITERABLE_CACHE_THREAD_COUNT, Runtime.getRuntime().availableProcessors() > 3 ? 2 : 1),
                new Pair(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT, 10000L),
                new Pair(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY, 2000),
                new Pair(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE, 512),
                new Pair(ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE, false),
                new Pair(TRANSACTION_PROPS_CACHE_SIZE, 1024),
                new Pair(TRANSACTION_LINKS_CACHE_SIZE, 4096),
                new Pair(TRANSACTION_BLOB_STRINGS_CACHE_SIZE, 128),
                new Pair(GATHER_STATISTICS, true),
                new Pair(MANAGEMENT_ENABLED, true)
        }, strategy);
    }

    @Override
    public PersistentEntityStoreConfig setSetting(@NotNull String key, @NotNull Object value) {
        return (PersistentEntityStoreConfig) super.setSetting(key, value);
    }

    public boolean getRefactoringSkipAll() {
        return (Boolean) getSetting(REFACTORING_SKIP_ALL);
    }

    public PersistentEntityStoreConfig setRefactoringSkipAll(final boolean skipAll) {
        return setSetting(REFACTORING_SKIP_ALL, skipAll);
    }

    public boolean getRefactoringNullIndices() {
        return (Boolean) getSetting(REFACTORING_NULL_INDICES);
    }

    public PersistentEntityStoreConfig setRefactoringNullIndices(final boolean nullIndices) {
        return setSetting(REFACTORING_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringBlobNullIndices() {
        return (Boolean) getSetting(REFACTORING_BLOB_NULL_INDICES);
    }

    public PersistentEntityStoreConfig setRefactoringBlobNullIndices(final boolean nullIndices) {
        return setSetting(REFACTORING_BLOB_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringHeavyLinks() {
        return (Boolean) getSetting(REFACTORING_HEAVY_LINKS);
    }

    public PersistentEntityStoreConfig setRefactoringHeavyLinks(final boolean heavyLinks) {
        return setSetting(REFACTORING_HEAVY_LINKS, heavyLinks);
    }

    public boolean getRefactoringHeavyProps() {
        return (Boolean) getSetting(REFACTORING_HEAVY_PROPS);
    }

    public PersistentEntityStoreConfig setRefactoringHeavyProps(final boolean heavyProps) {
        return setSetting(REFACTORING_HEAVY_PROPS, heavyProps);
    }

    public boolean getRefactoringDeleteRedundantBlobs() {
        return (Boolean) getSetting(REFACTORING_DELETE_REDUNDANT_BLOBS);
    }

    public PersistentEntityStoreConfig setRefactoringDeleteRedundantBlobs(final boolean fixRedundantBlobs) {
        return setSetting(REFACTORING_DELETE_REDUNDANT_BLOBS, fixRedundantBlobs);
    }

    public int getMaxInPlaceBlobSize() {
        return (Integer) getSetting(MAX_IN_PLACE_BLOB_SIZE);
    }

    public PersistentEntityStoreConfig setMaxInPlaceBlobSize(final int blobSize) {
        return setSetting(MAX_IN_PLACE_BLOB_SIZE, blobSize);
    }

    public int getBlobStringsCacheSize() {
        return (Integer) getSetting(BLOB_STRINGS_CACHE_SIZE);
    }

    public PersistentEntityStoreConfig setBlobStringsCacheSize(final int blobStringsCacheSize) {
        return setSetting(BLOB_STRINGS_CACHE_SIZE, blobStringsCacheSize);
    }

    public boolean isCachingDisabled() {
        return (Boolean) getSetting(CACHING_DISABLED);
    }

    public PersistentEntityStoreConfig setCachingDisabled(final boolean disabled) {
        return setSetting(CACHING_DISABLED, disabled);
    }

    public boolean isReorderingDisabled() {
        return (Boolean) getSetting(REORDERING_DISABLED);
    }

    public PersistentEntityStoreConfig setReorderingDisabled(final boolean disabled) {
        return setSetting(REORDERING_DISABLED, disabled);
    }

    public boolean isExplainOn() {
        return (Boolean) getSetting(EXPLAIN_ON);
    }

    public PersistentEntityStoreConfig setExplainOn(final boolean explainOn) {
        return setSetting(EXPLAIN_ON, explainOn);
    }

    public boolean getUniqueIndicesUseBtree() {
        return (Boolean) getSetting(UNIQUE_INDICES_USE_BTREE);
    }

    public PersistentEntityStoreConfig setUniqueIndicesUseBtree(final boolean useBtree) {
        return setSetting(UNIQUE_INDICES_USE_BTREE, useBtree);
    }

    public boolean isDebugLinkDataGetter() {
        return (Boolean) getSetting(DEBUG_LINK_DATA_GETTER);
    }

    public PersistentEntityStoreConfig setDebugLinkDataGetter(final boolean debug) {
        return setSetting(DEBUG_LINK_DATA_GETTER, debug);
    }

    public int getEntityIterableCacheSize() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_SIZE);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheSize(final int size) {
        return setSetting(ENTITY_ITERABLE_CACHE_SIZE, size);
    }

    public int getEntityIterableCacheThreadCount() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_THREAD_COUNT);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheThreadCount(final int threadCount) {
        return setSetting(ENTITY_ITERABLE_CACHE_THREAD_COUNT, threadCount);
    }

    public long getEntityIterableCacheCachingTimeout() {
        return (Long) getSetting(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheCachingTimeout(final long cachingTimeout) {
        return setSetting(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT, cachingTimeout);
    }

    public int getEntityIterableCacheDeferredDelay() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheDeferredDelay(final int deferredDelay) {
        return setSetting(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY, deferredDelay);
    }

    public int getEntityIterableCacheMaxSizeOfDirectValue() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheMaxSizeOfDirectValue(final int maxSizeOfDirectValue) {
        return setSetting(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE, maxSizeOfDirectValue);
    }

    public boolean getEntityIterableCacheUseHumanReadable() {
        return (Boolean) getSetting(ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE);
    }

    public PersistentEntityStoreConfig setEntityIterableCacheUseHumanReadable(final boolean useHumanReadable) {
        return setSetting(ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE, useHumanReadable);
    }

    public int getTransactionPropsCacheSize() {
        return (Integer) getSetting(TRANSACTION_PROPS_CACHE_SIZE);
    }

    public PersistentEntityStoreConfig setTransactionPropsCacheSize(final int transactionPropsCacheSize) {
        return setSetting(TRANSACTION_PROPS_CACHE_SIZE, transactionPropsCacheSize);
    }

    public int getTransactionLinksCacheSize() {
        return (Integer) getSetting(TRANSACTION_LINKS_CACHE_SIZE);
    }

    public PersistentEntityStoreConfig setTransactionLinksCacheSize(final int transactionLinksCacheSize) {
        return setSetting(TRANSACTION_LINKS_CACHE_SIZE, transactionLinksCacheSize);
    }

    public int getTransactionBlobStringsCacheSize() {
        return (Integer) getSetting(TRANSACTION_BLOB_STRINGS_CACHE_SIZE);
    }

    public PersistentEntityStoreConfig setTransactionBlobStringsCacheSize(final int transactionBlobStringsCacheSize) {
        return setSetting(TRANSACTION_BLOB_STRINGS_CACHE_SIZE, transactionBlobStringsCacheSize);
    }

    public boolean getGatherStatistics() {
        return (Boolean) getSetting(GATHER_STATISTICS);
    }

    public PersistentEntityStoreConfig setGatherStatistics(final boolean gatherStatistics) {
        return setSetting(GATHER_STATISTICS, gatherStatistics);
    }

    public boolean isManagementEnabled() {
        return (Boolean) getSetting(MANAGEMENT_ENABLED);
    }

    public PersistentEntityStoreConfig setManagementEnabled(final boolean managementEnabled) {
        return setSetting(MANAGEMENT_ENABLED, managementEnabled);
    }

    private static int defaultEntityIterableCacheSize() {
        return Math.max((int) (Runtime.getRuntime().maxMemory() >> 20), MAX_DEFAULT_ENTITY_ITERABLE_CACHE_SIZE);
    }
}
