/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class LogConfig {

    private static final int DEFAULT_FILE_SIZE = 1024; // in kilobytes

    private File dir;
    private long fileSize;
    private long lockTimeout;
    private long memoryUsage;
    private int memoryUsagePercentage;
    private DataReader reader;
    private DataWriter writer;
    private boolean isDurableWrite;
    private boolean isFsyncSuppressed;
    private boolean sharedCache;
    private boolean nonBlockingCache;
    private int cachePageSize;
    private int cacheOpenFilesCount;
    private boolean cacheUseNio;
    private long cacheFreePhysicalMemoryThreshold;
    private boolean cleanDirectoryExpected;
    private boolean clearInvalidLog;
    private long syncPeriod;
    private boolean fullFileReadonly;

    public LogConfig() {
    }

    public LogConfig setDir(@NotNull final File dir) {
        this.dir = dir;
        return this;
    }

    public long getFileSize() {
        if (fileSize == 0) {
            fileSize = DEFAULT_FILE_SIZE;
        }
        return fileSize;
    }

    public LogConfig setFileSize(final long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public long getLockTimeout() {
        return lockTimeout;
    }

    public LogConfig setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
        return this;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public LogConfig setMemoryUsage(final long memUsage) {
        memoryUsage = memUsage;
        return this;
    }

    public int getMemoryUsagePercentage() {
        if (memoryUsagePercentage == 0) {
            memoryUsagePercentage = 50;
        }
        return memoryUsagePercentage;
    }

    public LogConfig setMemoryUsagePercentage(final int memoryUsagePercentage) {
        this.memoryUsagePercentage = memoryUsagePercentage;
        return this;
    }

    public DataReader getReader() {
        if (reader == null) {
            reader = new FileDataReader(checkDirectory(dir),
                    getCacheOpenFilesCount(), getCacheUseNio(), getCacheFreePhysicalMemoryThreshold());
        }
        return reader;
    }

    public LogConfig setReader(@NotNull final DataReader reader) {
        this.reader = reader;
        return this;
    }

    public DataWriter getWriter() {
        if (writer == null) {
            writer = new FileDataWriter(checkDirectory(dir));
        }
        return writer;
    }

    public LogConfig setWriter(@NotNull final DataWriter writer) {
        this.writer = writer;
        return this;
    }

    public boolean isDurableWrite() {
        return isDurableWrite;
    }

    public LogConfig setDurableWrite(boolean durableWrite) {
        isDurableWrite = durableWrite;
        return this;
    }

    public boolean isFsyncSuppressed() {
        return isFsyncSuppressed;
    }

    public LogConfig setFsyncSuppressed(boolean fsyncSuppressed) {
        isFsyncSuppressed = fsyncSuppressed;
        return this;
    }

    public boolean isSharedCache() {
        return sharedCache;
    }

    public LogConfig setSharedCache(boolean sharedCache) {
        this.sharedCache = sharedCache;
        return this;
    }

    public boolean isNonBlockingCache() {
        return nonBlockingCache;
    }

    public LogConfig setNonBlockingCache(boolean nonBlockingCache) {
        this.nonBlockingCache = nonBlockingCache;
        return this;
    }

    public int getCachePageSize() {
        if (cachePageSize == 0) {
            cachePageSize = LogCache.MINIMUM_PAGE_SIZE;
        }
        return cachePageSize;
    }

    public LogConfig setCachePageSize(int cachePageSize) {
        this.cachePageSize = cachePageSize;
        return this;
    }

    public int getCacheOpenFilesCount() {
        if (cacheOpenFilesCount == 0) {
            cacheOpenFilesCount = LogCache.DEFAULT_OPEN_FILES_COUNT;
        }
        return cacheOpenFilesCount;
    }

    public LogConfig setCacheOpenFilesCount(int cacheOpenFilesCount) {
        this.cacheOpenFilesCount = cacheOpenFilesCount;
        return this;
    }

    public boolean getCacheUseNio() {
        return cacheUseNio;
    }

    public LogConfig setCacheUseNio(boolean cacheUseNio) {
        this.cacheUseNio = cacheUseNio;
        return this;
    }

    public long getCacheFreePhysicalMemoryThreshold() {
        if (cacheFreePhysicalMemoryThreshold == 0L) {
            cacheFreePhysicalMemoryThreshold = EnvironmentConfig.DEFAULT.getLogCacheFreePhysicalMemoryThreshold();
        }
        return cacheFreePhysicalMemoryThreshold;
    }

    public LogConfig setCacheFreePhysicalMemoryThreshold(long cacheFreePhysicalMemoryThreshold) {
        this.cacheFreePhysicalMemoryThreshold = cacheFreePhysicalMemoryThreshold;
        return this;
    }

    public LogConfig setCleanDirectoryExpected(boolean cleanDirectoryExpected) {
        this.cleanDirectoryExpected = cleanDirectoryExpected;
        return this;
    }

    public boolean isClearInvalidLog() {
        return clearInvalidLog;
    }

    public LogConfig setClearInvalidLog(boolean clearInvalidLog) {
        this.clearInvalidLog = clearInvalidLog;
        return this;
    }

    public long getSyncPeriod() {
        if (syncPeriod == 0) {
            syncPeriod = EnvironmentConfig.DEFAULT.getLogSyncPeriod();
        }
        return syncPeriod;
    }

    public LogConfig setSyncPeriod(long syncPeriod) {
        this.syncPeriod = syncPeriod;
        return this;
    }

    public boolean isFullFileReadonly() {
        return fullFileReadonly;
    }

    public LogConfig setFullFileReadonly(boolean fullFileReadonly) {
        this.fullFileReadonly = fullFileReadonly;
        return this;
    }

    public static LogConfig create(@NotNull final DataReader reader, @NotNull final DataWriter writer) {
        return new LogConfig().setReader(reader).setWriter(writer);
    }

    private File checkDirectory(@NotNull final File directory) {
        if (directory.isFile()) {
            throw new ExodusException("A directory is required: " + directory);
        }
        if (directory.exists()) {
            if (cleanDirectoryExpected && LogUtil.listFiles(directory).length > 0) {
                throw new ExodusException("Clean directory expected (log configured to be newly created only)");
            }
        } else {
            if (!directory.mkdirs()) {
                throw new ExodusException("Failed to create directory: " + directory);
            }
        }
        return directory;
    }
}
