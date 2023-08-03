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
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache;
import jetbrains.exodus.core.dataStructures.LongObjectCacheBase;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;

public final class SharedLogCache extends LogCache {
    private final LongObjectCacheBase<CachedValue> pagesCache;

    SharedLogCache(
            long memoryUsage,
            int pageSize,
            int cacheGenerationCount
    ) {
        super(memoryUsage, pageSize);
        var pagesCount = (int) (memoryUsage / (pageSize +  /* each page consumes additionally 96 bytes in the cache */
                96));
        pagesCache = new ConcurrentLongObjectCache<>(pagesCount, cacheGenerationCount);
    }

    SharedLogCache(
            int memoryUsagePercentage,
            int pageSize,
            int cacheGenerationCount
    ) {
        super(memoryUsagePercentage, pageSize);
        if (memoryUsage == Long.MAX_VALUE) {
            pagesCache = new ConcurrentLongObjectCache<>(ObjectCacheBase.DEFAULT_SIZE, cacheGenerationCount);
        } else {
            var pagesCount = (int) (memoryUsage / (pageSize +  /* each page consumes additionally some bytes in the cache */
                    96));
            pagesCache = new ConcurrentLongObjectCache<>(pagesCount, cacheGenerationCount);
        }
    }

    @Override
    public void clear() { // do nothing on clear since the cache can contain pages of different environments
    }


    @Override
    public float hitRate() {
        return pagesCache.hitRate();
    }

    @Override
    public void cachePage(CacheDataProvider cacheDataProvider, long pageAddress, byte[] page) {
        var logIdentity = cacheDataProvider.getIdentity();
        cachePage(getLogPageFingerPrint(logIdentity, pageAddress), logIdentity, pageAddress, page);
    }

    @Override
    public byte[] getPage(CacheDataProvider cacheDataProvider, long pageAddress, long fileStart) {
        var logIdentity = cacheDataProvider.getIdentity();
        var key = getLogPageFingerPrint(logIdentity, pageAddress);
        var cachedValue = pagesCache.tryKeyLocked(key);
        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return cachedValue.page;
        }

        var page = cacheDataProvider.readPage(pageAddress, fileStart);
        cachePage(key, logIdentity, pageAddress, page);

        return page;
    }


    @Override
    public ArrayByteIterable getPageIterable(
            CacheDataProvider cacheDataProvider,
            long pageAddress,
            boolean formatWithHashCodeIsUsed
    ) {
        var logIdentity = cacheDataProvider.getIdentity();
        var key = getLogPageFingerPrint(logIdentity, pageAddress);
        var cachedValue = pagesCache.tryKeyLocked(key);

        var adjustedPageSize = pageSize - BufferedDataWriter.HASH_CODE_SIZE;
        if (!formatWithHashCodeIsUsed) {
            adjustedPageSize = pageSize;
        }

        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return new ArrayByteIterable(cachedValue.page, adjustedPageSize);
        }

        var page = cacheDataProvider.readPage(pageAddress, -1);
        cachePage(key, logIdentity, pageAddress, page);

        return new ArrayByteIterable(page, adjustedPageSize);
    }

    @Override
    public void removePage(CacheDataProvider cacheDataProvider, long pageAddress) {
        var key = getLogPageFingerPrint(cacheDataProvider.getIdentity(), pageAddress);
        pagesCache.removeLocked(key);
    }


    private void cachePage(long key, int logIdentity, long address, byte[] page) {
        pagesCache.cacheObjectLocked(key, new CachedValue(logIdentity, address, page));
    }

    private long getLogPageFingerPrint(int logIdentity, long address) {
        return (address + logIdentity << 32) + address + logIdentity;
    }

    @SuppressWarnings("ClassCanBeRecord")
    private static final class CachedValue {
        private final int logIdentity;
        private final long address;
        private final byte[] page;

        private CachedValue(int logIdentity, long address, byte[] page) {
            this.logIdentity = logIdentity;
            this.address = address;
            this.page = page;
        }
    }

}
