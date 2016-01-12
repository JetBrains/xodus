/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCache;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import org.jetbrains.annotations.NotNull;

final class SharedLogCache extends LogCache {

    @NotNull
    private final ObjectCacheBase<CacheKey, ArrayByteIterable> pagesCache;

    SharedLogCache(final long memoryUsage, final int pageSize, final boolean nonBlocking) {
        super(memoryUsage, pageSize);
        final int pagesCount = (int) (memoryUsage / (pageSize +
                /* each page consumes additionally nearly 104 bytes in the cache */ 104));
        pagesCache = nonBlocking ?
                new ConcurrentObjectCache<CacheKey, ArrayByteIterable>(pagesCount, CONCURRENT_CACHE_GENERATION_COUNT) :
                new ObjectCache<CacheKey, ArrayByteIterable>(pagesCount);
    }

    SharedLogCache(final int memoryUsagePercentage, final int pageSize, final boolean nonBlocking) {
        super(memoryUsagePercentage, pageSize);
        if (memoryUsage == Long.MAX_VALUE) {
            pagesCache = nonBlocking ?
                    new ConcurrentObjectCache<CacheKey, ArrayByteIterable>(ObjectCacheBase.DEFAULT_SIZE, CONCURRENT_CACHE_GENERATION_COUNT) :
                    new ObjectCache<CacheKey, ArrayByteIterable>();
        } else {
            final int pagesCount = (int) (memoryUsage / (pageSize +
                    /* each page consumes additionally nearly 104 bytes in the cache */ 104));
            pagesCache = nonBlocking ?
                    new ConcurrentObjectCache<CacheKey, ArrayByteIterable>(pagesCount, CONCURRENT_CACHE_GENERATION_COUNT) :
                    new ObjectCache<CacheKey, ArrayByteIterable>(pagesCount);
        }
    }

    @Override
    void clear() {
        // do nothing on clear since the cache can contain pages of different environments
    }

    @Override
    double hitRate() {
        return pagesCache.hitRate();
    }

    @Override
    void cachePage(@NotNull final Log log, final long pageAddress, @NotNull final ArrayByteIterable page) {
        cachePage(new CacheKey(log.getIdentity(), pageAddress >> pageSizeLogarithm), page);
    }

    @Override
    @NotNull
    protected ArrayByteIterable getPage(@NotNull final Log log, final long pageAddress) {
        final long adjustedPageAddress = pageAddress >> pageSizeLogarithm;
        final int logIdentity = log.getIdentity();
        final CacheKey cacheKey = new CacheKey(logIdentity, adjustedPageAddress);
        ArrayByteIterable page = pagesCache.tryKeyLocked(cacheKey);
        if (page != null) {
            return page;
        }
        page = log.getHighPage(pageAddress);
        if (page != null) {
            return page;
        }
        page = readFullPage(log, pageAddress);
        cachePage(cacheKey, page);
        return page;
    }

    @Override
    protected ArrayByteIterable removePage(@NotNull final Log log, final long pageAddress) {
        final long adjustedPageAddress = pageAddress >> pageSizeLogarithm;
        final int logIdentity = log.getIdentity();
        final CacheKey cacheKey = new CacheKey(logIdentity, adjustedPageAddress);
        pagesCache.lock();
        try {
            return pagesCache.remove(cacheKey);
        } finally {
            pagesCache.unlock();
        }
    }

    private void cachePage(@NotNull final CacheKey cacheKey, @NotNull final ArrayByteIterable page) {
        pagesCache.lock();
        try {
            if (pagesCache.getObject(cacheKey) == null) {
                pagesCache.cacheObject(cacheKey, page);
            }
        } finally {
            pagesCache.unlock();
        }
    }

    private static final class CacheKey {

        private final int logIdentity;
        private final long address;

        private CacheKey(final int logIdentity, final long address) {
            this.logIdentity = logIdentity;
            this.address = address;
        }

        @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
        public boolean equals(Object obj) {
            final CacheKey key = (CacheKey) obj;
            return address == key.address && logIdentity == key.logIdentity;

        }

        public int hashCode() {
            return (logIdentity ^ (int) address) + (logIdentity << 16);
        }
    }
}
