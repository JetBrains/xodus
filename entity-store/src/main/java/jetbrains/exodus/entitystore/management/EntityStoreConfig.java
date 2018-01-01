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
package jetbrains.exodus.entitystore.management;

import jetbrains.exodus.entitystore.PersistentEntityStoreConfig;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.management.MBeanBase;
import org.jetbrains.annotations.NotNull;

public class EntityStoreConfig extends MBeanBase implements EntityStoreConfigMBean {

    @NotNull
    private final PersistentEntityStoreImpl store;
    @NotNull
    private final PersistentEntityStoreConfig config;

    public EntityStoreConfig(@NotNull final PersistentEntityStoreImpl store) {
        super(getObjectName(store));
        this.store = store;
        config = store.getConfig();
    }

    @Override
    public boolean getRefactoringSkipAll() {
        return config.getRefactoringSkipAll();
    }

    @Override
    public boolean getRefactoringNullIndices() {
        return config.getRefactoringNullIndices();
    }

    @Override
    public boolean getRefactoringBlobNullIndices() {
        return config.getRefactoringBlobNullIndices();
    }

    @Override
    public boolean getRefactoringHeavyLinks() {
        return config.getRefactoringHeavyLinks();
    }

    @Override
    public boolean getRefactoringHeavyProps() {
        return config.getRefactoringHeavyProps();
    }

    @Override
    public boolean getRefactoringDeleteRedundantBlobs() {
        return config.getRefactoringDeleteRedundantBlobs();
    }

    @Override
    public int getMaxInPlaceBlobSize() {
        return config.getMaxInPlaceBlobSize();
    }

    @Override
    public void setMaxInPlaceBlobSize(int blobSize) {
        config.setMaxInPlaceBlobSize(blobSize);
    }

    @Override
    public boolean isBlobStringsCacheShared() {
        return config.isBlobStringsCacheShared();
    }

    @Override
    public long getBlobStringsCacheMaxValueSize() {
        return config.getBlobStringsCacheMaxValueSize();
    }

    @Override
    public void setBlobStringsCacheMaxValueSize(long maxValueSize) {
        config.setBlobStringsCacheMaxValueSize(maxValueSize);
    }

    @Override
    public boolean isCachingDisabled() {
        return config.isCachingDisabled();
    }

    @Override
    public void setCachingDisabled(boolean disabled) {
        config.setCachingDisabled(disabled);
    }

    @Override
    public boolean isReorderingDisabled() {
        return config.isReorderingDisabled();
    }

    @Override
    public void setReorderingDisabled(boolean disabled) {
        config.setReorderingDisabled(disabled);
    }

    @Override
    public boolean isExplainOn() {
        return config.isExplainOn();
    }

    @Override
    public boolean isDebugLinkDataGetter() {
        return config.isDebugLinkDataGetter();
    }

    @Override
    public boolean isDebugSearchForIncomingLinksOnDelete() {
        return config.isDebugSearchForIncomingLinksOnDelete();
    }

    @Override
    public void setDebugSearchForIncomingLinksOnDelete(boolean debug) {
        config.setDebugSearchForIncomingLinksOnDelete(debug);
    }

    @Override
    public boolean isDebugTestLinkedEntities() {
        return config.isDebugTestLinkedEntities();
    }

    @Override
    public void setDebugTestLinkedEntities(boolean debug) {
        config.setDebugTestLinkedEntities(debug);
    }

    @Override
    public boolean isDebugAllowInMemorySort() {
        return config.isDebugAllowInMemorySort();
    }

    @Override
    public void setDebugAllowInMemorySort(boolean debug) {
        config.setDebugAllowInMemorySort(debug);
    }

    @Override
    public int getEntityIterableCacheSize() {
        return config.getEntityIterableCacheSize();
    }

    @Override
    public int getEntityIterableCacheThreadCount() {
        return config.getEntityIterableCacheThreadCount();
    }

    @Override
    public long getEntityIterableCacheCachingTimeout() {
        return config.getEntityIterableCacheCachingTimeout();
    }

    @Override
    public void setEntityIterableCacheCachingTimeout(long cachingTimeout) {
        config.setEntityIterableCacheCachingTimeout(cachingTimeout);
    }

    @Override
    public int getEntityIterableCacheDeferredDelay() {
        return config.getEntityIterableCacheDeferredDelay();
    }

    @Override
    public void setEntityIterableCacheDeferredDelay(int deferredDelay) {
        config.setEntityIterableCacheDeferredDelay(deferredDelay);
    }

    @Override
    public int getEntityIterableCacheMaxSizeOfDirectValue() {
        return config.getEntityIterableCacheMaxSizeOfDirectValue();
    }

    @Override
    public void setEntityIterableCacheMaxSizeOfDirectValue(int maxSizeOfDirectValue) {
        config.setEntityIterableCacheMaxSizeOfDirectValue(maxSizeOfDirectValue);
    }

    @Override
    public boolean getEntityIterableCacheUseHumanReadable() {
        return config.getEntityIterableCacheUseHumanReadable();
    }

    @Override
    public void setEntityIterableCacheUseHumanReadable(boolean useHumanReadable) {
        config.setEntityIterableCacheUseHumanReadable(useHumanReadable);
    }

    @Override
    public int getTransactionPropsCacheSize() {
        return config.getTransactionPropsCacheSize();
    }

    @Override
    public void setTransactionPropsCacheSize(int transactionPropsCacheSize) {
        config.setTransactionPropsCacheSize(transactionPropsCacheSize);
    }

    @Override
    public int getTransactionLinksCacheSize() {
        return config.getTransactionLinksCacheSize();
    }

    @Override
    public void setTransactionLinksCacheSize(int transactionLinksCacheSize) {
        config.setTransactionLinksCacheSize(transactionLinksCacheSize);
    }

    @Override
    public int getTransactionBlobStringsCacheSize() {
        return config.getTransactionBlobStringsCacheSize();
    }

    @Override
    public void setTransactionBlobStringsCacheSize(int transactionBlobStringsCacheSize) {
        config.setTransactionBlobStringsCacheSize(transactionBlobStringsCacheSize);
    }

    @Override
    public boolean getGatherStatistics() {
        return config.getGatherStatistics();
    }

    @Override
    public void close() {
        store.close();
        super.close();
    }

    public static String getObjectName(@NotNull final PersistentEntityStoreImpl store) {
        return OBJECT_NAME_PREFIX + ", location=" + escapeLocation(store.getLocation());
    }
}
