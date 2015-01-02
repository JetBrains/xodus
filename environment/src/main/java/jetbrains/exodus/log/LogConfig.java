/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class LogConfig {

    private static final int DEFAULT_FILE_SIZE = 1024; // in kilobytes
    private static final int DEFAULT_SYNC_PERIOD = 1000; // in milliseconds

    private File dir;
    private long fileSize;
    private long lockTimeout;
    private long memoryUsage;
    private int memoryUsagePercentage;
    private DataReader reader;
    private DataWriter writer;
    private boolean isDurableWrite;
    private boolean sharedCache;
    private boolean nonBlockingCache;
    private int cachePageSize;
    private int cacheOpenFilesCount;
    private boolean cleanDirectoryExpected;
    private boolean clearInvalidLog;
    private long syncPeriod;

    public void setDir(@NotNull final File dir) {
        this.dir = dir;
    }

    public long getFileSize() {
        if (fileSize == 0) {
            fileSize = DEFAULT_FILE_SIZE;
        }
        return fileSize;
    }

    public void setFileSize(final long fileSize) {
        this.fileSize = fileSize;
    }

    public long getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(final long memUsage) {
        memoryUsage = memUsage;
    }

    public int getMemoryUsagePercentage() {
        if (memoryUsagePercentage == 0) {
            memoryUsagePercentage = 50;
        }
        return memoryUsagePercentage;
    }

    public void setMemoryUsagePercentage(final int memoryUsagePercentage) {
        this.memoryUsagePercentage = memoryUsagePercentage;
    }

    public DataReader getReader() {
        if (reader == null) {
            reader = new FileDataReader(checkDirectory(dir), getCacheOpenFilesCount());
        }
        return reader;
    }

    public void setReader(@NotNull final DataReader reader) {
        this.reader = reader;
    }

    public DataWriter getWriter() {
        if (writer == null) {
            writer = new FileDataWriter(checkDirectory(dir));
        }
        return writer;
    }

    public void setWriter(@NotNull final DataWriter writer) {
        this.writer = writer;
    }

    public boolean isDurableWrite() {
        return isDurableWrite;
    }

    public void setDurableWrite(boolean durableWrite) {
        isDurableWrite = durableWrite;
    }

    public boolean isSharedCache() {
        return sharedCache;
    }

    public void setSharedCache(boolean sharedCache) {
        this.sharedCache = sharedCache;
    }

    public boolean isNonBlockingCache() {
        return nonBlockingCache;
    }

    public void setNonBlockingCache(boolean nonBlockingCache) {
        this.nonBlockingCache = nonBlockingCache;
    }

    public int getCachePageSize() {
        if (cachePageSize == 0) {
            cachePageSize = LogCache.MINIMUM_PAGE_SIZE;
        }
        return cachePageSize;
    }

    public void setCachePageSize(int cachePageSize) {
        this.cachePageSize = cachePageSize;
    }

    public int getCacheOpenFilesCount() {
        if (cacheOpenFilesCount == 0) {
            cacheOpenFilesCount = LogCache.DEFAULT_OPEN_FILES_COUNT;
        }
        return cacheOpenFilesCount;
    }

    public void setCacheOpenFilesCount(int cacheOpenFilesCount) {
        this.cacheOpenFilesCount = cacheOpenFilesCount;
    }

    public void setCleanDirectoryExpected(boolean cleanDirectoryExpected) {
        this.cleanDirectoryExpected = cleanDirectoryExpected;
    }

    public boolean isClearInvalidLog() {
        return clearInvalidLog;
    }

    public void setClearInvalidLog(boolean clearInvalidLog) {
        this.clearInvalidLog = clearInvalidLog;
    }

    public long getSyncPeriod() {
        if (syncPeriod == 0) {
            syncPeriod = DEFAULT_SYNC_PERIOD;
        }
        return syncPeriod;
    }

    public void setSyncPeriod(long syncPeriod) {
        this.syncPeriod = syncPeriod;
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
