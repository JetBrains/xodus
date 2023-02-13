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
package jetbrains.exodus.log

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.core.dataStructures.*
import jetbrains.exodus.core.dataStructures.LongObjectCacheBase.Companion.DEFAULT_SIZE

internal class SeparateLogCache : LogCache {

    private val pagesCache: LongObjectCacheBase<ByteArray>

    constructor(
        memoryUsage: Long,
        pageSize: Int,
        nonBlocking: Boolean,
        useSoftReferences: Boolean,
        cacheGenerationCount: Int
    ) : super(memoryUsage, pageSize) {
        val pagesCount = (memoryUsage / (pageSize +  /* each page consumes additionally some bytes in the cache */
                if (useSoftReferences) 144 else 80)).toInt()
        pagesCache = if (nonBlocking) {
            if (useSoftReferences) {
                SoftConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
            } else {
                ConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
            }
        } else {
            if (useSoftReferences) {
                SoftLongObjectCache(pagesCount)
            } else {
                LongObjectCache(pagesCount)
            }
        }
    }

    constructor(
        memoryUsagePercentage: Int,
        pageSize: Int,
        nonBlocking: Boolean,
        useSoftReferences: Boolean,
        cacheGenerationCount: Int
    ) : super(memoryUsagePercentage, pageSize) {
        pagesCache = if (memoryUsage == Long.MAX_VALUE) {
            if (nonBlocking) {
                if (useSoftReferences) {
                    SoftConcurrentLongObjectCache(DEFAULT_SIZE, cacheGenerationCount)
                } else {
                    ConcurrentLongObjectCache(DEFAULT_SIZE, cacheGenerationCount)
                }
            } else {
                if (useSoftReferences) {
                    SoftLongObjectCache(DEFAULT_SIZE)
                } else {
                    LongObjectCache()
                }
            }
        } else {
            val pagesCount =
                (memoryUsage / (pageSize +  /* each page consumes additionally nearly 80 bytes in the cache */
                        if (useSoftReferences) 144 else 80)).toInt()
            if (nonBlocking) {
                if (useSoftReferences) {
                    SoftConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
                } else {
                    ConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
                }
            } else {
                if (useSoftReferences) {
                    SoftLongObjectCache(pagesCount)
                } else {
                    LongObjectCache(pagesCount)
                }
            }
        }
    }

    override fun clear() = pagesCache.clear()

    override fun hitRate() = pagesCache.hitRate()

    override fun cachePage(cacheDataProvider: CacheDataProvider, pageAddress: Long, page: ByteArray) = cachePage(pageAddress, page)

    override fun getPageIterable(
        cacheDataProvider: CacheDataProvider,
        pageAddress: Long,
        formatWithHashCodeIsUsed: Boolean
    ): ArrayByteIterable {
        var page = pagesCache.tryKeyLocked(pageAddress)

        var adjustedPageSize = pageSize - BufferedDataWriter.LOGGABLE_DATA
        if (!formatWithHashCodeIsUsed) {
            adjustedPageSize = pageSize
        }

        if (page != null) {
            return ArrayByteIterable(page, adjustedPageSize)
        }

        page = cacheDataProvider.readPage(pageAddress, -1)
        cachePage(pageAddress, page)

        return ArrayByteIterable(page, adjustedPageSize)
    }

    override fun getPage(
        cacheDataProvider: CacheDataProvider, pageAddress: Long, fileStart: Long
    ): ByteArray {
        var page = pagesCache.tryKeyLocked(pageAddress)
        if (page != null) {
            return page
        }

        page = cacheDataProvider.readPage(pageAddress, fileStart)
        cachePage(pageAddress, page)

        return page
    }

    override fun getCachedPage(cacheDataProvider: CacheDataProvider, pageAddress: Long): ByteArray? {
        return pagesCache.getObjectLocked(pageAddress)
    }

    override fun removePage(cacheDataProvider: CacheDataProvider, pageAddress: Long) {
        pagesCache.removeLocked(pageAddress)
    }

    private fun cachePage(cacheKey: Long, pageArray: ByteArray) {
        pagesCache.cacheObjectLocked(cacheKey, pageArray)
    }
}
