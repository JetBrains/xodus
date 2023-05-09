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
import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.util.MathUtil

abstract class LogCache {
    @JvmField
    internal val memoryUsage: Long
    private val memoryUsagePercentage: Int

    @JvmField
    internal val pageSize: Int

    /**
     * @param memoryUsage amount of memory which the cache is allowed to occupy (in bytes).
     * @param pageSize    number of bytes in a page.
     * @throws InvalidSettingException if settings are invalid.
     */
    protected constructor(memoryUsage: Long, pageSize: Int) {
        checkPageSize(pageSize)
        this.pageSize = pageSize
        checkIntegerLogarithm(pageSize) {
            "Log cache page size should be a power of 2: $pageSize"
        }
        val maxMemory = Runtime.getRuntime().maxMemory()
        if (maxMemory <= memoryUsage) {
            throw InvalidSettingException("Memory usage cannot be greater than JVM maximum memory")
        }
        this.memoryUsage = memoryUsage
        memoryUsagePercentage = 0
    }

    /**
     * @param memoryUsagePercentage amount of memory which the cache is allowed to occupy (in percents to the max memory value).
     * @param pageSize              number of bytes in a page.
     * @throws InvalidSettingException if settings are invalid.
     */
    protected constructor(memoryUsagePercentage: Int, pageSize: Int) {
        checkPageSize(pageSize)
        if (memoryUsagePercentage < MINIMUM_MEM_USAGE_PERCENT) {
            throw InvalidSettingException("Memory usage percent cannot be less than $MINIMUM_MEM_USAGE_PERCENT")
        }
        if (memoryUsagePercentage > MAXIMUM_MEM_USAGE_PERCENT) {
            throw InvalidSettingException("Memory usage percent cannot be greater than $MAXIMUM_MEM_USAGE_PERCENT")
        }
        this.pageSize = pageSize
        checkIntegerLogarithm(pageSize) {
            "Log cache page size should be a power of 2: $pageSize"
        }
        val maxMemory = Runtime.getRuntime().maxMemory()
        memoryUsage =
            if (maxMemory == Long.MAX_VALUE) Long.MAX_VALUE else maxMemory / 100L * memoryUsagePercentage.toLong()
        this.memoryUsagePercentage = memoryUsagePercentage
    }

    abstract fun clear()

    abstract fun hitRate(): Float

    abstract fun cachePage(cacheDataProvider: CacheDataProvider, pageAddress: Long, page: ByteArray)

    abstract fun getPage(
        cacheDataProvider: CacheDataProvider, pageAddress: Long, fileStart: Long
    ): ByteArray

    abstract fun getCachedPage(cacheDataProvider: CacheDataProvider, pageAddress: Long): ByteArray?

    abstract fun getPageIterable(
        cacheDataProvider: CacheDataProvider,
        pageAddress: Long,
        formatWithHashCodeIsUsed: Boolean
    ): ArrayByteIterable

    internal abstract fun removePage(cacheDataProvider: CacheDataProvider, pageAddress: Long)

    companion object {

        const val MINIMUM_PAGE_SIZE = LogUtil.LOG_BLOCK_ALIGNMENT
        protected const val MINIMUM_MEM_USAGE_PERCENT = 5
        protected const val MAXIMUM_MEM_USAGE_PERCENT = 95

        private fun checkPageSize(pageSize: Int) {
            if (pageSize.countOneBits() != 1) {
                throw InvalidSettingException("Page size should be power of two")
            }
            if (pageSize < MINIMUM_PAGE_SIZE) {
                throw InvalidSettingException("Page size cannot be less than $MINIMUM_PAGE_SIZE")
            }
            if (pageSize % MINIMUM_PAGE_SIZE != 0) {
                throw InvalidSettingException("Page size should be multiple of $MINIMUM_PAGE_SIZE")
            }
        }

        private fun checkIntegerLogarithm(i: Int, exceptionMessage: () -> String) {
            if (1 shl MathUtil.integerLogarithm(i) != i) {
                throw InvalidSettingException(exceptionMessage())
            }
        }
    }
}
