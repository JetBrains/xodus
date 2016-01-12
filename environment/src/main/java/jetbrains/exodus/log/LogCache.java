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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.util.MathUtil;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"ProtectedField"})
abstract class LogCache {

    protected static final int MINIMUM_PAGE_SIZE = LogUtil.LOG_BLOCK_ALIGNMENT;
    protected static final int DEFAULT_OPEN_FILES_COUNT = 16;
    protected static final int MINIMUM_MEM_USAGE_PERCENT = 5;
    protected static final int MAXIMUM_MEM_USAGE_PERCENT = 95;
    protected static final int CONCURRENT_CACHE_GENERATION_COUNT = 2;

    protected final long memoryUsage;
    protected final int memoryUsagePercentage;
    protected final int pageSize;
    protected final int pageSizeLogarithm;

    /**
     * @param memoryUsage amount of memory which the cache is allowed to occupy (in bytes).
     * @param pageSize    number of bytes in a page.
     * @throws InvalidSettingException if settings are invalid.
     */
    protected LogCache(final long memoryUsage, final int pageSize) {
        checkPageSize(pageSize);
        this.pageSize = pageSize;
        if ((pageSizeLogarithm = integerLogarithm(pageSize)) < 0) {
            throw new InvalidSettingException("Log cache page size should be a power of 2: " + pageSize);
        }
        final long maxMemory = Runtime.getRuntime().maxMemory();
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
    protected LogCache(final int memoryUsagePercentage, final int pageSize) {
        checkPageSize(pageSize);
        if (memoryUsagePercentage < MINIMUM_MEM_USAGE_PERCENT) {
            throw new InvalidSettingException("Memory usage percent cannot be less than " + MINIMUM_MEM_USAGE_PERCENT);
        }
        if (memoryUsagePercentage > MAXIMUM_MEM_USAGE_PERCENT) {
            throw new InvalidSettingException("Memory usage percent cannot be greater than " + MAXIMUM_MEM_USAGE_PERCENT);
        }
        this.pageSize = pageSize;
        if ((pageSizeLogarithm = integerLogarithm(pageSize)) < 0) {
            throw new InvalidSettingException("Log cache page size should be a power of 2: " + pageSize);
        }
        final long maxMemory = Runtime.getRuntime().maxMemory();
        memoryUsage = maxMemory == Long.MAX_VALUE ? Long.MAX_VALUE : maxMemory / 100L * (long) memoryUsagePercentage;
        this.memoryUsagePercentage = memoryUsagePercentage;
    }

    abstract void clear();

    abstract double hitRate();

    abstract void cachePage(@NotNull final Log log, final long pageAddress, @NotNull final ArrayByteIterable page);

    @NotNull
    abstract ArrayByteIterable getPage(@NotNull final Log log, final long pageAddress);

    abstract ArrayByteIterable removePage(@NotNull final Log log, final long pageAddress);

    protected ArrayByteIterable readFullPage(Log log, long pageAddress) {
        final ArrayByteIterable page = allocPage();
        if (log.readBytes(page.getBytesUnsafe(), pageAddress) != pageSize) {
            throw new ExodusException("Can't read full page from log [" + log.getLocation() + "] with address " + pageAddress);
        }
        return page;
    }

    ArrayByteIterable allocPage() {
        return new ArrayByteIterable(new byte[pageSize]);
    }

    private static void checkPageSize(int pageSize) throws InvalidSettingException {
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new InvalidSettingException("Page size cannot be less than " + MINIMUM_PAGE_SIZE);
        }
        if (pageSize % MINIMUM_PAGE_SIZE != 0) {
            throw new InvalidSettingException("Page size should be multiple of " + MINIMUM_PAGE_SIZE);
        }
    }

    private static int integerLogarithm(int i) {
        final int result = MathUtil.integerLogarithm(i);
        return 1 << result == i ? result : -1;
    }
}
