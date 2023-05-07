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

import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.io.*;
import jetbrains.exodus.io.inMemory.MemoryDataReaderWriterProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LogConfig {

    private static final int DEFAULT_FILE_SIZE = 1024; // in kilobytes

    private String location;
    private String readerWriterProvider;
    private DataReaderWriterProvider readerWriterProviderInstance;
    private long fileSize;
    private long lockTimeout;
    private long memoryUsage;
    private int memoryUsagePercentage;
    private DataReader reader;
    private DataWriter writer;
    private boolean isDurableWrite;
    private boolean sharedCache;
    private boolean nonBlockingCache;
    private boolean cacheUseSoftReferences;
    private int cacheGenerationCount;
    private int cacheReadAheadMultiple;
    private int cachePageSize;
    private int cacheOpenFilesCount;
    private boolean cleanDirectoryExpected;
    private boolean clearInvalidLog;
    private boolean warmup;
    private long syncPeriod;
    private boolean fullFileReadonly;
    private StreamCipherProvider cipherProvider;
    private byte[] cipherKey;
    private long cipherBasicIV;
    private boolean lockIgnored;
    private boolean useV1Format;

    private boolean checkPagesAtRuntime;

    private boolean skipInvalidLoggableType;

    public LogConfig() {
        useV1Format = EnvironmentConfig.DEFAULT.getUseVersion1Format();
        checkPagesAtRuntime = EnvironmentConfig.DEFAULT.getCheckPagesAtRuntime();
    }

    public LogConfig setLocation(@NotNull final String location) {
        this.location = location;
        return this;
    }

    public LogConfig setReaderWriterProvider(@NotNull final String provider) {
        readerWriterProvider = provider;
        return this;
    }

    public LogConfig setSkipInvalidLoggableType(boolean skipInvalidLoggableType) {
        this.skipInvalidLoggableType = skipInvalidLoggableType;
        return this;
    }

    public boolean isSkipInvalidLoggableType() {
        return skipInvalidLoggableType;
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

    @SuppressWarnings("unused")
    public LogConfig setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
        return this;
    }

    public boolean isLockIgnored() {
        return lockIgnored;
    }

    public void setLockIgnored(boolean lockIgnored) {
        this.lockIgnored = lockIgnored;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    public LogConfig setMemoryUsagePercentage(final int memoryUsagePercentage) {
        this.memoryUsagePercentage = memoryUsagePercentage;
        return this;
    }

    public DataReader getReader() {
        if (reader == null) {
            createReaderWriter();
        }
        return reader;
    }

    @Deprecated
    public LogConfig setReader(@NotNull final DataReader reader) {
        this.reader = reader;
        return this;
    }

    public DataWriter getWriter() {
        if (writer == null) {
            createReaderWriter();
        }
        return writer;
    }

    @Deprecated
    public LogConfig setWriter(@NotNull final DataWriter writer) {
        this.writer = writer;
        return this;
    }

    public LogConfig setReaderWriter(@NotNull final DataReader reader,
                                     @NotNull final DataWriter writer) {
        this.reader = reader;
        this.writer = writer;
        return this;
    }

    public boolean isDurableWrite() {
        return isDurableWrite;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LogConfig setDurableWrite(boolean durableWrite) {
        isDurableWrite = durableWrite;
        return this;
    }

    public boolean isSharedCache() {
        return sharedCache;
    }

    @SuppressWarnings("unused")
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

    public boolean getCacheUseSoftReferences() {
        return cacheUseSoftReferences;
    }

    @SuppressWarnings("unused")
    public LogConfig setCacheUseSoftReferences(boolean cacheUseSoftReferences) {
        this.cacheUseSoftReferences = cacheUseSoftReferences;
        return this;
    }

    public int getCacheGenerationCount() {
        if (cacheGenerationCount == 0) {
            cacheGenerationCount = EnvironmentConfig.DEFAULT.getLogCacheGenerationCount();
        }
        return cacheGenerationCount;
    }

    @SuppressWarnings("unused")
    public LogConfig setCacheGenerationCount(int cacheGenerationCount) {
        this.cacheGenerationCount = cacheGenerationCount;
        return this;
    }

    @SuppressWarnings("unused")
    public int getCacheReadAheadMultiple() {
        if (cacheReadAheadMultiple == 0) {
            cacheReadAheadMultiple = EnvironmentConfig.DEFAULT.getLogCacheReadAheadMultiple();
        }
        return cacheReadAheadMultiple;
    }

    public void setCacheReadAheadMultiple(int cacheReadAheadMultiple) {
        this.cacheReadAheadMultiple = cacheReadAheadMultiple;
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

    @SuppressWarnings("unused")
    public LogConfig setCacheOpenFilesCount(int cacheOpenFilesCount) {
        this.cacheOpenFilesCount = cacheOpenFilesCount;
        return this;
    }

    public boolean isCleanDirectoryExpected() {
        return cleanDirectoryExpected;
    }

    @SuppressWarnings("unused")
    public LogConfig setCleanDirectoryExpected(boolean cleanDirectoryExpected) {
        this.cleanDirectoryExpected = cleanDirectoryExpected;
        return this;
    }

    public boolean isClearInvalidLog() {
        return clearInvalidLog;
    }

    @SuppressWarnings("unused")
    public LogConfig setClearInvalidLog(boolean clearInvalidLog) {
        this.clearInvalidLog = clearInvalidLog;
        return this;
    }

    public boolean isWarmup() {
        return warmup;
    }

    public LogConfig setWarmup(boolean warmup) {
        this.warmup = warmup;
        return this;
    }

    public long getSyncPeriod() {
        if (syncPeriod == 0) {
            syncPeriod = EnvironmentConfig.DEFAULT.getLogSyncPeriod();
        }
        return syncPeriod;
    }

    @SuppressWarnings("UnusedReturnValue")
    public LogConfig setSyncPeriod(long syncPeriod) {
        this.syncPeriod = syncPeriod;
        return this;
    }

    public boolean isFullFileReadonly() {
        return fullFileReadonly;
    }

    @SuppressWarnings("unused")
    public LogConfig setFullFileReadonly(boolean fullFileReadonly) {
        this.fullFileReadonly = fullFileReadonly;
        return this;
    }

    public StreamCipherProvider getCipherProvider() {
        return cipherProvider;
    }

    @SuppressWarnings("unused")
    public LogConfig setCipherProvider(StreamCipherProvider cipherProvider) {
        this.cipherProvider = cipherProvider;
        return this;
    }

    public byte[] getCipherKey() {
        return cipherKey;
    }

    public LogConfig setCipherKey(byte[] cipherKey) {
        this.cipherKey = cipherKey;
        return this;
    }

    public long getCipherBasicIV() {
        return cipherBasicIV;
    }

    public LogConfig setCipherBasicIV(long basicIV) {
        this.cipherBasicIV = basicIV;
        return this;
    }

    @Nullable
    public DataReaderWriterProvider getReaderWriterProvider() {
        if (readerWriterProviderInstance == null && readerWriterProvider != null) {
            readerWriterProviderInstance = DataReaderWriterProvider.getProvider(readerWriterProvider);
            if (readerWriterProviderInstance == null) {
                switch (readerWriterProvider) {
                    case DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER:
                        readerWriterProviderInstance = new AsyncFileDataReaderWriterProvider();
                        break;
                    case DataReaderWriterProvider.IN_MEMORY_READER_WRITER_PROVIDER:
                        readerWriterProviderInstance = new MemoryDataReaderWriterProvider();
                        break;
                    default:
                        throw new InvalidSettingException("Unknown DataReaderWriterProvider: " + readerWriterProvider);
                }
            }
        }
        return readerWriterProviderInstance;
    }

    public void setUseV1Format(boolean useV1Format) {
        this.useV1Format = useV1Format;
    }

    public boolean useV1Format() {
        return useV1Format;
    }

    public boolean isCheckPagesAtRuntime() {
        return checkPagesAtRuntime;
    }

    public void setCheckPagesAtRuntime(boolean checkPagesAtRuntime) {
        this.checkPagesAtRuntime = checkPagesAtRuntime;
    }

    public static LogConfig create(@NotNull final DataReader reader, @NotNull final DataWriter writer) {
        return new LogConfig().setReaderWriter(reader, writer);
    }

    private void createReaderWriter() {
        final String location = this.location;
        if (location == null) {
            throw new InvalidSettingException("Location for DataReader and DataWriter is not specified");
        }

        var provider = getReaderWriterProvider();
        assert provider != null;
        final Pair<DataReader, DataWriter> readerWriter = provider.newReaderWriter(location);
        reader = readerWriter.getFirst();
        writer = readerWriter.getSecond();
    }
}
