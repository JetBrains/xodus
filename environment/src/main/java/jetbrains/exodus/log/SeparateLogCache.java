/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.core.dataStructures.LongObjectCacheBase.DEFAULT_SIZE;

final class SeparateLogCache extends LogCache {

    @NotNull
    private final LongObjectCacheBase<byte[]> pagesCache;

    SeparateLogCache(final long memoryUsage,
                     final int pageSize,
                     final boolean nonBlocking, final int cacheGenerationCount) {
        super(memoryUsage, pageSize);
        final int pagesCount = (int) (memoryUsage / (pageSize +
                /* each page consumes additionally nearly 80 bytes in the cache */ 80));
        pagesCache = nonBlocking ?
                new ConcurrentLongObjectCache<byte[]>(pagesCount, cacheGenerationCount) :
                new LongObjectCache<byte[]>(pagesCount);
    }

    SeparateLogCache(final int memoryUsagePercentage,
                     final int pageSize,
                     final boolean nonBlocking,
                     final int cacheGenerationCount) {
        super(memoryUsagePercentage, pageSize);
        if (memoryUsage == Long.MAX_VALUE) {
            pagesCache = nonBlocking ?
                    new ConcurrentLongObjectCache<byte[]>(DEFAULT_SIZE, cacheGenerationCount) :
                    new LongObjectCache<byte[]>();
        } else {
            final int pagesCount = (int) (memoryUsage / (pageSize +
                    /* each page consumes additionally nearly 80 bytes in the cache */ 80));
            pagesCache = nonBlocking ?
                    new ConcurrentLongObjectCache<byte[]>(pagesCount, cacheGenerationCount) :
                    new LongObjectCache<byte[]>(pagesCount);
        }
    }

    @Override
    public void clear() {
        pagesCache.clear();
    }

    @Override
    public float hitRate() {
        return pagesCache.hitRate();
    }

    @Override
    void cachePage(@NotNull final Log log, final long pageAddress, @NotNull final byte[] page) {
        cachePage(pageAddress, page);
    }

    @Override
    @NotNull
    ArrayByteIterable getPageIterable(@NotNull final Log log, final long pageAddress) {
        byte[] page = pagesCache.tryKeyLocked(pageAddress);
        if (page != null) {
            return new ArrayByteIterable(page);
        }
        page = log.getHighPage(pageAddress);
        if (page != null) {
            return new ArrayByteIterable(page, (int) Math.min(log.getHighAddress() - pageAddress, (long) pageSize));
        }
        page = readFullPage(log, pageAddress);
        cachePage(pageAddress, page);
        return new ArrayByteIterable(page);
    }

    @NotNull
    @Override
    byte[] getPage(@NotNull final Log log, final long pageAddress) {
        byte[] page = pagesCache.tryKeyLocked(pageAddress);
        if (page != null) {
            return page;
        }
        page = log.getHighPage(pageAddress);
        if (page != null) {
            return page;
        }
        page = readFullPage(log, pageAddress);
        cachePage(pageAddress, page);
        return page;
    }

    @Nullable
    @Override
    byte[] getCachedPage(@NotNull Log log, long pageAddress) {
        byte[] page = pagesCache.getObjectLocked(pageAddress);
        if (page != null) {
            return page;
        }
        page = log.getHighPage(pageAddress);
        if (page != null) {
            return page;
        }
        return null;
    }

    @Override
    protected void removePage(@NotNull final Log log, final long pageAddress) {
        pagesCache.removeLocked(pageAddress);
    }

    private void cachePage(final long cacheKey, @NotNull final byte[] pageArray) {
        pagesCache.cacheObjectLocked(cacheKey, postProcessTailPage(pageArray));
    }
}
