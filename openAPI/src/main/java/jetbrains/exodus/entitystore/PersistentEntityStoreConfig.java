/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
import jetbrains.exodus.ConfigSettingChangeListener;
import jetbrains.exodus.ConfigurationStrategy;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.Environment;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Map;

/**
 * Specifies settings of {@linkplain PersistentEntityStore}. Default settings are specified by {@linkplain #DEFAULT} which
 * is immutable. Any newly created {@code PersistentEntityStoreConfig} has the same settings as {@linkplain #DEFAULT}.
 *
 * <p>As a rule, the {@code PersistentEntityStoreConfig} instance is created along with the
 * {@linkplain PersistentEntityStore} one.  E.g., for given {@linkplain Environment environment} creation of
 * {@linkplain PersistentEntityStore} with some tuned caching settings can look as follows:
 * <pre>
 *     final PersistentEntityStoreConfig config = new PersistentEntityStoreConfig().setBlobStringsCacheSize(4000).setCachingDisabled(true);
 *     final PersistentEntityStore store = PersistentEntityStores.newInstance(config, environment, "storeName");
 * </pre>
 *
 * Some setting are mutable at runtime and some are immutable. Immutable at runtime settings can be changed, but they
 * won't take effect on the {@linkplain PersistentEntityStore} instance. Those settings are applicable only during
 * {@linkplain PersistentEntityStore} instance creation.
 *
 * <p>Most of the {@code PersistentEntityStoreConfig} settings allow to change behaviour of different caching processes.
 * The rest of the settings are mostly not intended for public use, but for debugging and troubleshooting purposes.
 *
 * <p>You can define custom processing of changed settings values by
 * {@linkplain #addChangedSettingsListener(ConfigSettingChangeListener)}. Override
 * {@linkplain ConfigSettingChangeListener#beforeSettingChanged(String, Object, Map)} to pre-process mutations of
 * settings and {@linkplain ConfigSettingChangeListener#afterSettingChanged(String, Object, Map)} to post-process them.
 *
 * @see PersistentEntityStore
 * @see PersistentEntityStore#getConfig()
 */
@SuppressWarnings({"UnusedDeclaration", "WeakerAccess"})
public final class PersistentEntityStoreConfig extends AbstractConfig {

    public static final PersistentEntityStoreConfig DEFAULT = new PersistentEntityStoreConfig(ConfigurationStrategy.IGNORE);

    /**
     * If is set to {@code true} then new {@linkplain PersistentEntityStore} will skip all refactorings on its creation.
     * Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_SKIP_ALL = "exodus.entityStore.refactoring.skipAll";

    /**
     * If is set to {@code true} then new {@linkplain PersistentEntityStore} will force all refactorings on its creation.
     * Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_FORCE_ALL = "exodus.entityStore.refactoring.forceAll";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_NULL_INDICES = "exodus.entityStore.refactoring.nullIndices";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_BLOB_NULL_INDICES = "exodus.entityStore.refactoring.blobNullIndices";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_HEAVY_LINKS = "exodus.entityStore.refactoring.heavyLinks";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_HEAVY_PROPS = "exodus.entityStore.refactoring.heavyProps";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String REFACTORING_DELETE_REDUNDANT_BLOBS = "exodus.entityStore.refactoring.deleteRedundantBlobs";

    /**
     * Defines the maximum size in bytes of an "in-place" blob. In-place blob saves its content in
     * {@linkplain Environment} (in .xd files), not in {@linkplain BlobVault}. "In-place" blobs are normally small
     * blobs, saving them in {@code Environment} allows to reduce the nu,ber of files in {@code BlobVault}.
     * Default value is {@code 10000}.
     * <p>Mutable at runtime: yes
     */
    public static final String MAX_IN_PLACE_BLOB_SIZE = "exodus.entityStore.maxInPlaceBlobSize";

    /**
     * If is set to {@code true} then {@linkplain BlobVault} uses shared (static) blob strings cache.
     * Otherwise, {@linkplain BlobVault} creates its own cache. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @since 1.0.5
     */
    public static final String BLOB_STRINGS_CACHE_SHARED = "exodus.entityStore.blobStringsCacheShared";

    /**
     * Defines the maximum size in bytes of a blob string that can be cached in blob strings cache.
     * Default value is {@code 1000000}.
     * <p>Mutable at runtime: yes
     *
     * @since 1.0.5
     */
    public static final String BLOB_STRINGS_CACHE_MAX_VALUE_SIZE = "exodus.entityStore.blobStringsCacheMaxValueSize";

    /**
     * As of 1.0.5, is deprecated and has no effect. Though system property with the name
     * {@code "exodus.entityStore.blobStringsCacheSize"} is used to configure size of blob strings cache.
     * <p>Mutable at runtime: no
     */
    @Deprecated
    public static final String BLOB_STRINGS_CACHE_SIZE = "exodus.entityStore.blobStringsCacheSize";

    /**
     * If is set to {@code true} then EntityIterableCache is not operable.
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String CACHING_DISABLED = "exodus.entityStore.cachingDisabled";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String REORDERING_DISABLED = "exodus.entityStore.reorderingDisabled";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String EXPLAIN_ON = "exodus.entityStore.explainOn";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String DEBUG_LINK_DATA_GETTER = "exodus.entityStore.debug.linkDataGetter";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String DEBUG_SEARCH_FOR_INCOMING_LINKS_ON_DELETE = "exodus.entityStore.debug.searchForIncomingLinksOnDelete";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String DEBUG_TEST_LINKED_ENTITIES = "exodus.entityStore.debug.testLinkedEntities";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String DEBUG_ALLOW_IN_MEMORY_SORT = "exodus.entityStore.debug.allowInMemorySort";

    /**
     * Defines the size of EntityIterableCache. EntityIterableCache is operable only if {@linkplain #CACHING_DISABLED}
     * is {@code false}. Default value depends on the JVM memory settings.
     * <p>Mutable at runtime: no
     *
     * @see #CACHING_DISABLED
     */
    public static final String ENTITY_ITERABLE_CACHE_SIZE = "exodus.entityStore.entityIterableCache.size";

    /**
     * Defines the number of thread which EntityIterableCache uses for its background caching activity.
     * EntityIterableCache is operable only if {@linkplain #CACHING_DISABLED} is {@code false}.
     * Default value is {@code 2}, if CPU count is greater than {@code 3}, otherwise it is {@code 1}.
     * <p>Mutable at runtime: no
     *
     * @see #CACHING_DISABLED
     * @see PersistentEntityStore#getAsyncProcessor()
     */
    public static final String ENTITY_ITERABLE_CACHE_THREAD_COUNT = "exodus.entityStore.entityIterableCache.threadCount";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code 10000L}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT = "exodus.entityStore.entityIterableCache.cachingTimeout";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code 2000}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENTITY_ITERABLE_CACHE_DEFERRED_DELAY = "exodus.entityStore.entityIterableCache.deferredDelay";

    /**
     * Defines the maximum size of "direct" value in EntityIterableCache. EntityIterableCache caches results of
     * different queries. Direct query results are strongly referenced, otherwise they are references through
     * {@linkplain SoftReference}. Basically, the more direct values are the better caching performance is.
     * Default value is {@code 512}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE = "exodus.entityStore.entityIterableCache.maxSizeOfDirectValue";

    /**
     * Not for public use, for debugging and troubleshooting purposes. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE = "exodus.entityStore.entityIterableCache.useHumanReadable";

    /**
     * Defines the size of "property values" cache held by each {@linkplain StoreTransaction} instance. This cache
     * reduces load created by de-serialization of property values. Default value is {@code 1024}.
     * <p>Mutable at runtime: yes
     */
    public static final String TRANSACTION_PROPS_CACHE_SIZE = "exodus.entityStore.transaction.propsCacheSize";

    /**
     * Defines the size of "links" cache held by each {@linkplain StoreTransaction} instance. This cache reduces load
     * created by de-serialization of links between {@linkplain Entity entities}. Default value is {@code 1024}.
     * <p>Mutable at runtime: yes
     */
    public static final String TRANSACTION_LINKS_CACHE_SIZE = "exodus.entityStore.transaction.linksCacheSize";

    /**
     * Defines the size of "blob strings" cache held by each {@linkplain StoreTransaction} instance. This cache
     * reduces load created by de-serialization of blob string. This cache is different from the one held by
     * {@linkplain BlobVault}. Default value is {@code 256}.
     * <p>Mutable at runtime: yes
     */
    public static final String TRANSACTION_BLOB_STRINGS_CACHE_SIZE = "exodus.entityStore.transaction.blobStringsCacheSize";

    /**
     * If is set to {@code true} then {@linkplain PersistentEntityStore} gathers statistics and exposes it via
     * JMX managed bean provided {@linkplain #MANAGEMENT_ENABLED} is also {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see #MANAGEMENT_ENABLED
     * @see PersistentEntityStore#getStatistics()
     */
    public static final String GATHER_STATISTICS = "exodus.entityStore.gatherStatistics";

    /**
     * If is set to {@code true} then the {@linkplain PersistentEntityStore} exposes two JMX managed beans. One for
     * {@linkplain PersistentEntityStore#getStatistics() statistics} and second for controlling the
     * {@code PersistentEntityStoreConfig} settings. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see PersistentEntityStore#getStatistics()
     * @see PersistentEntityStore#getConfig()
     */
    public static final String MANAGEMENT_ENABLED = "exodus.entityStore.managementEnabled";

    private static final int MAX_DEFAULT_ENTITY_ITERABLE_CACHE_SIZE = 4096;

    public PersistentEntityStoreConfig() {
        this(ConfigurationStrategy.SYSTEM_PROPERTY);
    }

    public PersistentEntityStoreConfig(@NotNull final ConfigurationStrategy strategy) {
        //noinspection unchecked
        super(new Pair[]{
            new Pair(REFACTORING_SKIP_ALL, false),
            new Pair(REFACTORING_FORCE_ALL, false),
            new Pair(REFACTORING_NULL_INDICES, false),
            new Pair(REFACTORING_BLOB_NULL_INDICES, false),
            new Pair(REFACTORING_HEAVY_LINKS, false),
            new Pair(REFACTORING_HEAVY_PROPS, false),
            new Pair(REFACTORING_DELETE_REDUNDANT_BLOBS, false),
            new Pair(MAX_IN_PLACE_BLOB_SIZE, 10000),
            new Pair(BLOB_STRINGS_CACHE_SHARED, true),
            new Pair(BLOB_STRINGS_CACHE_MAX_VALUE_SIZE, 1000000L),
            new Pair(CACHING_DISABLED, false),
            new Pair(REORDERING_DISABLED, false),
            new Pair(EXPLAIN_ON, false),
            new Pair(DEBUG_LINK_DATA_GETTER, false),
            new Pair(DEBUG_SEARCH_FOR_INCOMING_LINKS_ON_DELETE, false),
            new Pair(DEBUG_TEST_LINKED_ENTITIES, false),
            new Pair(DEBUG_ALLOW_IN_MEMORY_SORT, false),
            new Pair(ENTITY_ITERABLE_CACHE_SIZE, defaultEntityIterableCacheSize()),
            new Pair(ENTITY_ITERABLE_CACHE_THREAD_COUNT, Runtime.getRuntime().availableProcessors() > 3 ? 2 : 1),
            new Pair(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT, 10000L),
            new Pair(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY, 2000),
            new Pair(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE, 512),
            new Pair(ENTITY_ITERABLE_CACHE_USE_HUMAN_READABLE, false),
            new Pair(TRANSACTION_PROPS_CACHE_SIZE, 1024),
            new Pair(TRANSACTION_LINKS_CACHE_SIZE, 1024),
            new Pair(TRANSACTION_BLOB_STRINGS_CACHE_SIZE, 256),
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

    public boolean getRefactoringForceAll() {
        return (Boolean) getSetting(REFACTORING_FORCE_ALL);
    }

    public PersistentEntityStoreConfig setRefactoringForceAll(final boolean forceAll) {
        return setSetting(REFACTORING_FORCE_ALL, forceAll);
    }

    public boolean getRefactoringNullIndices() {
        return getRefactoringForceAll() || (Boolean) getSetting(REFACTORING_NULL_INDICES);
    }

    public PersistentEntityStoreConfig setRefactoringNullIndices(final boolean nullIndices) {
        return setSetting(REFACTORING_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringBlobNullIndices() {
        return getRefactoringForceAll() || (Boolean) getSetting(REFACTORING_BLOB_NULL_INDICES);
    }

    public PersistentEntityStoreConfig setRefactoringBlobNullIndices(final boolean nullIndices) {
        return setSetting(REFACTORING_BLOB_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringHeavyLinks() {
        return getRefactoringForceAll() || (Boolean) getSetting(REFACTORING_HEAVY_LINKS);
    }

    public PersistentEntityStoreConfig setRefactoringHeavyLinks(final boolean heavyLinks) {
        return setSetting(REFACTORING_HEAVY_LINKS, heavyLinks);
    }

    public boolean getRefactoringHeavyProps() {
        return getRefactoringForceAll() || (Boolean) getSetting(REFACTORING_HEAVY_PROPS);
    }

    public PersistentEntityStoreConfig setRefactoringHeavyProps(final boolean heavyProps) {
        return setSetting(REFACTORING_HEAVY_PROPS, heavyProps);
    }

    public boolean getRefactoringDeleteRedundantBlobs() {
        return getRefactoringForceAll() || (Boolean) getSetting(REFACTORING_DELETE_REDUNDANT_BLOBS);
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

    public boolean isBlobStringsCacheShared() {
        return (Boolean) getSetting(BLOB_STRINGS_CACHE_SHARED);
    }

    public PersistentEntityStoreConfig setBlobStringsCacheShared(final boolean shared) {
        return setSetting(BLOB_STRINGS_CACHE_SHARED, shared);
    }

    public long getBlobStringsCacheMaxValueSize() {
        return (Long) getSetting(BLOB_STRINGS_CACHE_MAX_VALUE_SIZE);
    }

    public PersistentEntityStoreConfig setBlobStringsCacheMaxValueSize(final long maxValueSize) {
        return setSetting(BLOB_STRINGS_CACHE_MAX_VALUE_SIZE, maxValueSize);
    }

    @Deprecated
    public int getBlobStringsCacheSize() {
        return (Integer) getSetting(BLOB_STRINGS_CACHE_SIZE);
    }

    @Deprecated
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

    public boolean isDebugLinkDataGetter() {
        return (Boolean) getSetting(DEBUG_LINK_DATA_GETTER);
    }

    public PersistentEntityStoreConfig setDebugLinkDataGetter(final boolean debug) {
        return setSetting(DEBUG_LINK_DATA_GETTER, debug);
    }

    public boolean isDebugSearchForIncomingLinksOnDelete() {
        return (Boolean) getSetting(DEBUG_SEARCH_FOR_INCOMING_LINKS_ON_DELETE);
    }

    public PersistentEntityStoreConfig setDebugSearchForIncomingLinksOnDelete(final boolean debug) {
        return setSetting(DEBUG_SEARCH_FOR_INCOMING_LINKS_ON_DELETE, debug);
    }

    public boolean isDebugTestLinkedEntities() {
        return (Boolean) getSetting(DEBUG_TEST_LINKED_ENTITIES);
    }

    public PersistentEntityStoreConfig setDebugTestLinkedEntities(final boolean debug) {
        return setSetting(DEBUG_TEST_LINKED_ENTITIES, debug);
    }

    public boolean isDebugAllowInMemorySort() {
        return (Boolean) getSetting(DEBUG_ALLOW_IN_MEMORY_SORT);
    }

    public PersistentEntityStoreConfig setDebugAllowInMemorySort(final boolean debug) {
        return setSetting(DEBUG_ALLOW_IN_MEMORY_SORT, debug);
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
