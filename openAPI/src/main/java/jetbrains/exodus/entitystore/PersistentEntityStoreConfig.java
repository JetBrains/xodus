/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.Pair;

@SuppressWarnings("UnusedDeclaration")
public final class PersistentEntityStoreConfig extends AbstractConfig {

    public static final PersistentEntityStoreConfig DEFAULT = new PersistentEntityStoreConfig();

    public static final String REFACTORING_NULL_INDICES = "exodus.entityStore.refactoring.nullIndices";

    public static final String REFACTORING_BLOB_NULL_INDICES = "exodus.entityStore.refactoring.blobNullIndices";

    public static final String REFACTORING_HEAVY_LINKS = "exodus.entityStore.refactoring.heavyLinks";

    public static final String REFACTORING_HEAVY_PROPS = "exodus.entityStore.refactoring.heavyProps";

    public static final String REFACTORING_DELETE_REDUNDANT_BLOBS = "exodus.entityStore.refactoring.deleteRedundantBlobs";

    public static final String MAX_IN_PLACE_BLOB_SIZE = "exodus.entityStore.maxInPlaceBlobSize";

    public static final String CACHING_DISABLED = "exodus.entityStore.cachingDisabled";

    public static final String EXPLAIN_ON = "exodus.entityStore.explainOn";

    public static final String UNIQUE_INDICES_USE_BTREE = "exodus.entityStore.uniqueIndices.useBtree";

    public static final String DEBUG_LINK_DATA_GETTER = "exodus.entityStore.debugLinkDataGetter";

    public static final String ENTITY_ITERABLE_CACHE_SIZE = "exodus.entityStore.entityIterableCache.size";

    public static final String ENTITY_ITERABLE_CACHE_THREAD_COUNT = "exodus.entityStore.entityIterableCache.threadCount";

    public static final String ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT = "exodus.entityStore.entityIterableCache.cachingTimeout";

    public static final String ENTITY_ITERABLE_CACHE_DEFERRED_DELAY = "exodus.entityStore.entityIterableCache.deferredDelay"; // in milliseconds

    public static final String ENTITY_ITERABLE_CACHE_MAX_KEY_SIZE = "exodus.entityStore.entityIterableCache.maxKeySize";

    public static final String ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE = "exodus.entityStore.entityIterableCache.maxSizeOfDirectValue";

    public static final String TRANSACTION_PROPS_CACHE_SIZE = "exodus.entityStore.transaction.propsCacheSize";

    public static final String TRANSACTION_LINKS_CACHE_SIZE = "exodus.entityStore.transaction.linksCacheSize";

    public static final String TRANSACTION_BLOB_STRINGS_CACHE_SIZE = "exodus.entityStore.transaction.blobStringsCacheSize";

    public static final String SSHD_PORT = "exodus.entityStore.sshd.port";

    public static final String SSHD_PASSWORD = "exodus.entityStore.sshd.password";

    private static final int MAX_DEFAULT_ENTITY_ITERABLE_CACHE_SIZE = 4096;

    public PersistentEntityStoreConfig() {
        //noinspection unchecked
        super(new Pair[]{
                new Pair(REFACTORING_NULL_INDICES, false),
                new Pair(REFACTORING_BLOB_NULL_INDICES, false),
                new Pair(REFACTORING_HEAVY_LINKS, false),
                new Pair(REFACTORING_HEAVY_PROPS, false),
                new Pair(REFACTORING_DELETE_REDUNDANT_BLOBS, false),
                new Pair(MAX_IN_PLACE_BLOB_SIZE, 10000),
                new Pair(CACHING_DISABLED, false),
                new Pair(EXPLAIN_ON, false),
                new Pair(UNIQUE_INDICES_USE_BTREE, false),
                new Pair(DEBUG_LINK_DATA_GETTER, false),
                new Pair(ENTITY_ITERABLE_CACHE_SIZE, defaultEntityIterableCacheSize()),
                new Pair(ENTITY_ITERABLE_CACHE_THREAD_COUNT, Runtime.getRuntime().availableProcessors() > 3 ? 2 : 1),
                new Pair(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT, 10000L),
                new Pair(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY, 2000),
                new Pair(ENTITY_ITERABLE_CACHE_MAX_KEY_SIZE, 1024),
                new Pair(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE, 512),
                new Pair(TRANSACTION_PROPS_CACHE_SIZE, 1024),
                new Pair(TRANSACTION_LINKS_CACHE_SIZE, 4096),
                new Pair(TRANSACTION_BLOB_STRINGS_CACHE_SIZE, 128)
        });
    }

    public boolean getRefactoringNullIndices() {
        return (Boolean) getSetting(REFACTORING_NULL_INDICES);
    }

    public void setRefactoringNullIndices(final boolean nullIndices) {
        setSetting(REFACTORING_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringBlobNullIndices() {
        return (Boolean) getSetting(REFACTORING_BLOB_NULL_INDICES);
    }

    public void setRefactoringBlobNullIndices(final boolean nullIndices) {
        setSetting(REFACTORING_BLOB_NULL_INDICES, nullIndices);
    }

    public boolean getRefactoringHeavyLinks() {
        return (Boolean) getSetting(REFACTORING_HEAVY_LINKS);
    }

    public void setRefactoringHeavyLinks(final boolean heavyLinks) {
        setSetting(REFACTORING_HEAVY_LINKS, heavyLinks);
    }

    public boolean getRefactoringHeavyProps() {
        return (Boolean) getSetting(REFACTORING_HEAVY_PROPS);
    }

    public void setRefactoringHeavyProps(final boolean heavyProps) {
        setSetting(REFACTORING_HEAVY_PROPS, heavyProps);
    }

    public boolean getRefactoringDeleteRedundantBlobs() {
        return (Boolean) getSetting(REFACTORING_DELETE_REDUNDANT_BLOBS);
    }

    public void setRefactoringDeleteRedundantBlobs(final boolean fixRedundantBlobs) {
        setSetting(REFACTORING_DELETE_REDUNDANT_BLOBS, fixRedundantBlobs);
    }

    public int getMaxInPlaceBlobSize() {
        return (Integer) getSetting(MAX_IN_PLACE_BLOB_SIZE);
    }

    public void setMaxInPlaceBlobSize(final int blobSize) {
        setSetting(MAX_IN_PLACE_BLOB_SIZE, blobSize);
    }

    public boolean isCachingDisabled() {
        return (Boolean) getSetting(CACHING_DISABLED);
    }

    public void setCachingDisabled(final boolean disabled) {
        setSetting(CACHING_DISABLED, disabled);
    }

    public boolean isExplainOn() {
        return (Boolean) getSetting(EXPLAIN_ON);
    }

    public void setExplainOn(final boolean explainOn) {
        setSetting(EXPLAIN_ON, explainOn);
    }

    public boolean getUniqueIndicesUseBtree() {
        return (Boolean) getSetting(UNIQUE_INDICES_USE_BTREE);
    }

    public void setUniqueIndicesUseBtree(final boolean useBtree) {
        setSetting(UNIQUE_INDICES_USE_BTREE, useBtree);
    }

    public boolean isDebugLinkDataGetter() {
        return (Boolean) getSetting(DEBUG_LINK_DATA_GETTER);
    }

    public void setDebugLinkDataGetter(final boolean debug) {
        setSetting(DEBUG_LINK_DATA_GETTER, debug);
    }

    public int getEntityIterableCacheSize() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_SIZE);
    }

    public void setEntityIterableCacheSize(final int size) {
        setSetting(ENTITY_ITERABLE_CACHE_SIZE, size);
    }

    public int getEntityIterableCacheThreadCount() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_THREAD_COUNT);
    }

    public void setEntityIterableCacheThreadCount(final int threadCount) {
        setSetting(ENTITY_ITERABLE_CACHE_THREAD_COUNT, threadCount);
    }

    public long getEntityIterableCacheCachingTimeout() {
        return (Long) getSetting(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT);
    }

    public void setEntityIterableCacheCachingTimeout(final long cachingTimeout) {
        setSetting(ENTITY_ITERABLE_CACHE_CACHING_TIMEOUT, cachingTimeout);
    }

    public int getEntityIterableCacheDeferredDelay() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY);
    }

    public void setEntityIterableCacheDeferredDelay(final int deferredDelay) {
        setSetting(ENTITY_ITERABLE_CACHE_DEFERRED_DELAY, deferredDelay);
    }

    public int getEntityIterableCacheMaxKeySize() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_MAX_KEY_SIZE);
    }

    public void setEntityIterableCacheMaxKeySize(final int maxKeySize) {
        setSetting(ENTITY_ITERABLE_CACHE_MAX_KEY_SIZE, maxKeySize);
    }

    public int getEntityIterableCacheMaxSizeOfDirectValue() {
        return (Integer) getSetting(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE);
    }

    public void setEntityIterableCacheMaxSizeOfDirectValue(final int maxSizeOfDirectValue) {
        setSetting(ENTITY_ITERABLE_CACHE_MAX_SIZE_OF_DIRECT_VALUE, maxSizeOfDirectValue);
    }

    public int getTransactionPropsCacheSize() {
        return (Integer) getSetting(TRANSACTION_PROPS_CACHE_SIZE);
    }

    public void setTransactionPropsCacheSize(final int transactionPropsCacheSize) {
        setSetting(TRANSACTION_PROPS_CACHE_SIZE, transactionPropsCacheSize);
    }

    public int getTransactionLinksCacheSize() {
        return (Integer) getSetting(TRANSACTION_LINKS_CACHE_SIZE);
    }

    public void setTransactionLinksCacheSize(final int transactionLinksCacheSize) {
        setSetting(TRANSACTION_LINKS_CACHE_SIZE, transactionLinksCacheSize);
    }

    public int getTransactionBlobStringsCacheSize() {
        return (Integer) getSetting(TRANSACTION_BLOB_STRINGS_CACHE_SIZE);
    }

    public void setTransactionBlobStringsCacheSize(final int transactionBlobStringsCacheSize) {
        setSetting(TRANSACTION_BLOB_STRINGS_CACHE_SIZE, transactionBlobStringsCacheSize);
    }

    public void setSshdPort(int port) {
        setSetting(SSHD_PORT, port);
    }

    public void setSshdPassword(String password) {
        setSetting(SSHD_PASSWORD, password);
    }

    public Integer getSshdPort() {
        return (Integer)getSetting(SSHD_PORT);
    }

    public String getSshdPassword() {
        return (String) getSetting(SSHD_PASSWORD);
    }

    private static int defaultEntityIterableCacheSize() {
        return Math.max((int) (Runtime.getRuntime().maxMemory() >> 20), MAX_DEFAULT_ENTITY_ITERABLE_CACHE_SIZE);
    }
}
