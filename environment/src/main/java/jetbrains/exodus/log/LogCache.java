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
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.util.MathUtil;

import java.util.function.Supplier;

public abstract class LogCache {
    protected static final int MINIMUM_PAGE_SIZE = LogUtil.LOG_BLOCK_ALIGNMENT;
    protected static final int DEFAULT_OPEN_FILES_COUNT = 16;
    protected static final int MINIMUM_MEM_USAGE_PERCENT = 5;
    protected static final int MAXIMUM_MEM_USAGE_PERCENT = 95;

    long memoryUsage;
    protected int memoryUsagePercentage;
    int pageSize;

    /**
     * @param memoryUsage amount of memory which the cache is allowed to occupy (in bytes).
     * @param pageSize    number of bytes in a page.
     * @throws InvalidSettingException if settings are invalid.
     */
    protected LogCache(long memoryUsage, int pageSize) {
        checkPageSize(pageSize);
        this.pageSize = pageSize;
        checkIntegerLogarithm(pageSize, () -> "Log cache page size should be a power of 2: " + pageSize);

        var maxMemory = Runtime.getRuntime().maxMemory();
        if (maxMemory <= memoryUsage) {
            throw new InvalidSettingException("Memory usage cannot be greater than JVM maximum memory");
        }
        this.memoryUsage = memoryUsage;
        memoryUsagePercentage = 0;
    }

    /**
     * @param memoryUsagePercentage amount of memory which the cache is allowed to occupy (in percents to the max memory value).
     * @param pageSize              number of bytes in a page.
     * @throws InvalidSettingException if settings are invalid.
     */
    protected LogCache(int memoryUsagePercentage, int pageSize) {
        checkPageSize(pageSize);
        if (memoryUsagePercentage < MINIMUM_MEM_USAGE_PERCENT) {
            throw new InvalidSettingException("Memory usage percent cannot be less than " + MINIMUM_MEM_USAGE_PERCENT);
        }
        if (memoryUsagePercentage > MAXIMUM_MEM_USAGE_PERCENT) {
            throw new InvalidSettingException("Memory usage percent cannot be greater than " + MAXIMUM_MEM_USAGE_PERCENT);
        }
        this.pageSize = pageSize;
        checkIntegerLogarithm(pageSize, () -> "Log cache page size should be a power of 2:" + pageSize);
        var maxMemory = Runtime.getRuntime().maxMemory();

        if (maxMemory == Long.MAX_VALUE) {
            memoryUsage = Long.MAX_VALUE;
        } else {
            memoryUsage = maxMemory / 100L * (long) memoryUsagePercentage;
        }
        this.memoryUsagePercentage = memoryUsagePercentage;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public int getPageSize() {
        return pageSize;
    }


    public abstract void clear();

    public abstract float hitRate();

    abstract void cachePage(CacheDataProvider cacheDataProvider, long pageAddress, byte[] page);

    public abstract byte[] getPage(
            CacheDataProvider cacheDataProvider, long pageAddress, long fileStart
    );

    public abstract ArrayByteIterable getPageIterable(
            CacheDataProvider cacheDataProvider,
            long pageAddress,
            boolean formatWithHashCodeIsUsed
    );

    public abstract void removePage(CacheDataProvider cacheDataProvider, long pageAddress);

    private static void checkPageSize(int pageSize) {
        if (Integer.bitCount(pageSize) != 1) {
            throw new InvalidSettingException("Page size should be power of two");
        }
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new InvalidSettingException("Page size cannot be less than " + MINIMUM_PAGE_SIZE);
        }
        if (pageSize % MINIMUM_PAGE_SIZE != 0) {
            throw new InvalidSettingException("Page size should be multiple of " + MINIMUM_PAGE_SIZE);
        }
    }

    private void checkIntegerLogarithm(int i, Supplier<String> exceptionMessage) {
        if (1 << MathUtil.integerLogarithm(i) != i) {
            throw new InvalidSettingException(exceptionMessage.get());
        }
    }
}
