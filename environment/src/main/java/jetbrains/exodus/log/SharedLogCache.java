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
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache;
import jetbrains.exodus.core.dataStructures.LongObjectCache;
import jetbrains.exodus.core.dataStructures.LongObjectCacheBase;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.core.dataStructures.LongObjectCacheBase.CriticalSection;

final class SharedLogCache extends LogCache {

    @NotNull
    private final LongObjectCacheBase<CachedValue> pagesCache;

    SharedLogCache(final long memoryUsage, final int pageSize, final boolean nonBlocking) {
        super(memoryUsage, pageSize);
        final int pagesCount = (int) (memoryUsage / (pageSize +
                /* each page consumes additionally 96 bytes in the cache */ 96));
        pagesCache = nonBlocking ?
            new ConcurrentLongObjectCache<CachedValue>(pagesCount, CONCURRENT_CACHE_GENERATION_COUNT) :
            new LongObjectCache<CachedValue>(pagesCount);
    }

    SharedLogCache(final int memoryUsagePercentage, final int pageSize, final boolean nonBlocking) {
        super(memoryUsagePercentage, pageSize);
        if (memoryUsage == Long.MAX_VALUE) {
            pagesCache = nonBlocking ?
                new ConcurrentLongObjectCache<CachedValue>(ObjectCacheBase.DEFAULT_SIZE, CONCURRENT_CACHE_GENERATION_COUNT) :
                new LongObjectCache<CachedValue>();
        } else {
            final int pagesCount = (int) (memoryUsage / (pageSize +
                    /* each page consumes additionally 96 bytes in the cache */ 96));
            pagesCache = nonBlocking ?
                new ConcurrentLongObjectCache<CachedValue>(pagesCount, CONCURRENT_CACHE_GENERATION_COUNT) :
                new LongObjectCache<CachedValue>(pagesCount);
        }
    }

    @Override
    void clear() {
        // do nothing on clear since the cache can contain pages of different environments
    }

    @Override
    float hitRate() {
        return pagesCache.hitRate();
    }

    @Override
    void cachePage(@NotNull final Log log, final long pageAddress, @NotNull final byte[] page) {
        final int logIdentity = log.getIdentity();
        cachePage(getLogPageFingerPrint(logIdentity, pageAddress), logIdentity, pageAddress, page);
    }

    @NotNull
    @Override
    byte[] getPage(@NotNull Log log, long pageAddress) {
        final int logIdentity = log.getIdentity();
        final long key = getLogPageFingerPrint(logIdentity, pageAddress);
        final CachedValue cachedValue = pagesCache.tryKeyLocked(key);
        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return cachedValue.page;
        }
        byte[] page = log.getHighPage(pageAddress);
        if (page != null) {
            return page;
        }
        page = readFullPage(log, pageAddress);
        cachePage(key, logIdentity, pageAddress, page);
        return page;
    }

    @Override
    @NotNull
    protected ArrayByteIterable getPageIterable(@NotNull final Log log, final long pageAddress) {
        final int logIdentity = log.getIdentity();
        final long key = getLogPageFingerPrint(logIdentity, pageAddress);
        final CachedValue cachedValue = pagesCache.tryKeyLocked(key);
        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return new ArrayByteIterable(cachedValue.page);
        }
        byte[] page = log.getHighPage(pageAddress);
        if (page != null) {
            return new ArrayByteIterable(page, (int) Math.min(log.getHighAddress() - pageAddress, (long) pageSize));
        }
        page = readFullPage(log, pageAddress);
        cachePage(key, logIdentity, pageAddress, page);
        return new ArrayByteIterable(page);
    }

    @Override
    protected void removePage(@NotNull final Log log, final long pageAddress) {
        final long key = getLogPageFingerPrint(log.getIdentity(), pageAddress);
        try (CriticalSection ignored = pagesCache.newCriticalSection()) {
            pagesCache.remove(key);
        }
    }

    private void cachePage(final long key, final int logIdentity, final long address, @NotNull final byte[] page) {
        try (CriticalSection ignored = pagesCache.newCriticalSection()) {
            if (pagesCache.getObject(key) == null) {
                pagesCache.cacheObject(key, new CachedValue(logIdentity, address, postProcessTailPage(page)));
            }
        }
    }

    private static long getLogPageFingerPrint(final int logIdentity, final long address) {
        return ((address + logIdentity) << 32) + address + logIdentity;
    }

    private static final class CachedValue {

        private final int logIdentity;
        private final long address;
        private final byte[] page;

        CachedValue(final int logIdentity, final long address, @NotNull final byte[] page) {
            this.logIdentity = logIdentity;
            this.address = address;
            this.page = page;
        }

    }
}
