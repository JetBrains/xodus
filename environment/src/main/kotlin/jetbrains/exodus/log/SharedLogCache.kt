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
import jetbrains.exodus.core.dataStructures.ObjectCacheBase.DEFAULT_SIZE

class SharedLogCache : LogCache {

    private val pagesCache: LongObjectCacheBase<CachedValue>

    constructor(
        memoryUsage: Long,
        pageSize: Int,
        cacheGenerationCount: Int
    ) : super(memoryUsage, pageSize) {
        val pagesCount = (memoryUsage / (pageSize +  /* each page consumes additionally 96 bytes in the cache */
                96)).toInt()
        pagesCache = ConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
    }

    constructor(
        memoryUsagePercentage: Int,
        pageSize: Int,
        cacheGenerationCount: Int
    ) : super(memoryUsagePercentage, pageSize) {
        pagesCache = if (memoryUsage == Long.MAX_VALUE) {
            ConcurrentLongObjectCache(DEFAULT_SIZE, cacheGenerationCount)
        } else {
            val pagesCount = (memoryUsage / (pageSize +  /* each page consumes additionally some bytes in the cache */
                    96)).toInt()
            ConcurrentLongObjectCache(pagesCount, cacheGenerationCount)
        }
    }

    override fun clear() { // do nothing on clear since the cache can contain pages of different environments
    }

    override fun hitRate() = pagesCache.hitRate()

    override fun cachePage(cacheDataProvider: CacheDataProvider, pageAddress: Long, page: ByteArray) =
        cacheDataProvider.identity.let { logIdentity ->
            cachePage(
                getLogPageFingerPrint(logIdentity, pageAddress),
                logIdentity,
                pageAddress,
                page
            )
        }

    override fun getPage(
        cacheDataProvider: CacheDataProvider, pageAddress: Long, fileStart: Long
    ): ByteArray {
        val logIdentity = cacheDataProvider.identity
        val key = getLogPageFingerPrint(logIdentity, pageAddress)
        val cachedValue = pagesCache.tryKeyLocked(key)
        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return cachedValue.page
        }

        val page = cacheDataProvider.readPage(pageAddress, fileStart)
        cachePage(key, logIdentity, pageAddress, page)

        return page
    }

    override fun getCachedPage(cacheDataProvider: CacheDataProvider, pageAddress: Long): ByteArray? {
        val logIdentity = cacheDataProvider.identity
        val key = getLogPageFingerPrint(logIdentity, pageAddress)
        val cachedValue = pagesCache.getObjectLocked(key)

        return if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            cachedValue.page
        } else null
    }

    override fun getPageIterable(
        cacheDataProvider: CacheDataProvider,
        pageAddress: Long,
        formatWithHashCodeIsUsed: Boolean
    ): ArrayByteIterable {
        val logIdentity = cacheDataProvider.identity
        val key = getLogPageFingerPrint(logIdentity, pageAddress)
        val cachedValue = pagesCache.tryKeyLocked(key)

        var adjustedPageSize = pageSize - BufferedDataWriter.HASH_CODE_SIZE
        if (!formatWithHashCodeIsUsed) {
            adjustedPageSize = pageSize
        }

        if (cachedValue != null && cachedValue.logIdentity == logIdentity && cachedValue.address == pageAddress) {
            return ArrayByteIterable(cachedValue.page, adjustedPageSize)
        }

        val page = cacheDataProvider.readPage(pageAddress, -1)
        cachePage(key, logIdentity, pageAddress, page)

        return ArrayByteIterable(page, adjustedPageSize)
    }

    override fun removePage(cacheDataProvider: CacheDataProvider, pageAddress: Long) {
        val key = getLogPageFingerPrint(cacheDataProvider.identity, pageAddress)
        pagesCache.removeLocked(key)
    }

    private fun cachePage(key: Long, logIdentity: Int, address: Long, page: ByteArray) {
        pagesCache.cacheObjectLocked(key, CachedValue(logIdentity, address, page))
    }

    private class CachedValue(val logIdentity: Int, val address: Long, val page: ByteArray)
}

private fun getLogPageFingerPrint(logIdentity: Int, address: Long): Long =
    (address + logIdentity shl 32) + address + logIdentity
