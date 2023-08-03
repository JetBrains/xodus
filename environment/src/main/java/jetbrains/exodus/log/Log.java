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
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.execution.IterableJob;
import jetbrains.exodus.core.execution.RunnableJob;
import jetbrains.exodus.crypto.InvalidCipherParametersException;
import jetbrains.exodus.env.DatabaseRoot;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.io.AsyncFileDataWriter;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.io.SharedOpenFilesCache;
import jetbrains.exodus.io.inMemory.MemoryDataReader;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IdGenerator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static jetbrains.exodus.crypto.EnvKryptKt.cryptBlocksMutable;

public final class Log implements Closeable, CacheDataProvider {
    private static final Logger logger = LoggerFactory.getLogger(Log.class);
    private static final IdGenerator identityGenerator = new IdGenerator();

    private static volatile SharedLogCache sharedCache = null;

    private static volatile Semaphore sharedWriteBoundarySemaphore = null;

    private static final Lock sharedResourcesLock = new ReentrantLock();


    private final long created = System.currentTimeMillis();

    private final LogCache cache;

    private volatile boolean isClosing;

    private int identity = 0;

    private final DataReader reader;
    private final DataWriter dataWriter;

    private final BufferedDataWriter writer;
    private Thread writeThread;


    private final CopyOnWriteArrayList<BlockListener> blockListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ReadBytesListener> readBytesListeners = new CopyOnWriteArrayList<>();


    private StartupMetadata startupMetadata;

    private boolean restoredFromBackup;

    /**
     * Size of single page in log cache.
     */
    private final int cachePageSize;

    /**
     * Indicate whether it is needed to perform migration to the format which contains
     * hash codes of content inside the pages.
     */
    private final boolean formatWithHashCodeIsUsed;

    /**
     * Size of a single file of the log in bytes.
     */
    private final long fileLengthBound;

    private LogTestConfig testConfigTestsOnly;

    private final String location;

    private volatile boolean rwIsReadonly;

    private final byte[] nullPage;

    private final LogConfig config;


    public Log(LogConfig config, int expectedEnvironmentVersion) throws IOException {
        this.config = config;
        this.reader = config.getReader();
        this.dataWriter = config.getWriter();
        this.location = reader.getLocation();

        tryLock();
        try {
            rwIsReadonly = false;

            var fileLength = config.getFileSize() * 1024L;

            var logContainsBlocks = reader.getBlocks().iterator().hasNext();
            StartupMetadata metadata;
            if (reader instanceof FileDataReader fileDataReader) {
                metadata = StartupMetadata.open(
                        fileDataReader, config.getCachePageSize(), expectedEnvironmentVersion,
                        fileLength, logContainsBlocks
                );
            } else {
                metadata = StartupMetadata.createStub(
                        config.getCachePageSize(), !logContainsBlocks,
                        expectedEnvironmentVersion, fileLength
                );
            }

            var needToPerformMigration = false;

            if (metadata != null) {
                startupMetadata = metadata;
            } else {
                startupMetadata = StartupMetadata.createStub(
                        config.getCachePageSize(), !logContainsBlocks, expectedEnvironmentVersion,
                        fileLength
                );
                needToPerformMigration = logContainsBlocks;
            }

            final Path loacationPath = Path.of(location);
            var backupLocation = loacationPath.resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME);
            if (!needToPerformMigration && !startupMetadata.isCorrectlyClosed()) {
                if (Files.exists(backupLocation)) {
                    logger.info("Database {}: trying to restore from dynamic backup...", location);
                    var backupMetadataBuffer = ByteBuffer.allocate(BackupMetadata.FILE_SIZE);
                    try (var channel = FileChannel.open(backupLocation, StandardOpenOption.READ)) {
                        while (backupMetadataBuffer.remaining() > 0) {
                            var r = channel.read(backupMetadataBuffer);
                            if (r == -1) {
                                throw new IOException("Unexpected end of file");
                            }
                        }
                    }

                    backupMetadataBuffer.rewind();
                    var backupMetadata = BackupMetadata.deserialize(
                            backupMetadataBuffer,
                            startupMetadata.getCurrentVersion(), startupMetadata.isUseFirstFile()
                    );
                    Files.deleteIfExists(backupLocation);

                    if (backupMetadata == null || backupMetadata.getRootAddress() < 0 ||
                            (backupMetadata.getLastFileOffset() % backupMetadata.getPageSize()) != 0L
                    ) {
                        logger.warn("Dynamic backup is not stored correctly for database {}.", location);
                    } else {
                        var lastFileName = LogUtil.getLogFilename(backupMetadata.getLastFileAddress());
                        var lastSegmentFile = loacationPath.resolve(lastFileName);

                        if (!Files.exists(lastSegmentFile)) {
                            logger.warn("Dynamic backup is not stored correctly for database {}.", location);
                        } else {
                            logger.info(
                                    "Found dynamic backup. " +
                                            "Database {} will be restored till file {}, " +
                                            " file length will be set too " +
                                            "{}. DB root address {}"
                                    , location, lastFileName, backupMetadata.getLastFileOffset(), backupMetadata.getRootAddress());

                            SharedOpenFilesCache.invalidate();
                            try {
                                SharedOpenFilesCache.invalidate();
                                dataWriter.close();

                                var blocks = new TreeMap<Long, FileDataReader.FileBlock>();

                                for (jetbrains.exodus.io.Block block : reader.getBlocks()) {
                                    blocks.put(block.getAddress(), (FileDataReader.FileBlock) block);
                                }

                                logger.info("Files found in database: {}", location);
                                for (Long address : blocks.keySet()) {
                                    logger.info(LogUtil.getLogFilename(address));
                                }

                                copyFilesInRestoreTempDir(
                                        blocks.tailMap(backupMetadata.getLastFileAddress(), true).values().stream().map(
                                                FileDataReader.FileBlock::toPath
                                        ).iterator()
                                );

                                var blocksToTruncateIterator = blocks.tailMap(
                                        backupMetadata.getLastFileAddress(),
                                        false
                                ).values().iterator();

                                truncateFile(lastSegmentFile.toFile(), backupMetadata.getLastFileOffset(), null);

                                if (blocksToTruncateIterator.hasNext()) {
                                    var blockToDelete = blocksToTruncateIterator.next();
                                    logger.info("File {} will be deleted.",
                                            LogUtil.getLogFilename(blockToDelete.getAddress()));


                                    if (!blockToDelete.canWrite()) {
                                        if (!blockToDelete.setWritable(true)) {
                                            throw new ExodusException(
                                                    "Can not write into file " + blockToDelete.getAbsolutePath()
                                            );
                                        }
                                    }

                                    Files.deleteIfExists(Path.of(blockToDelete.toURI()));
                                }

                                startupMetadata = backupMetadata;
                                restoredFromBackup = true;
                            } catch (Exception ex) {
                                logger.error("Failed to restore database $location from dynamic backup. ", ex);
                            }
                        }
                    }
                }
            }

            if (config.getCachePageSize() != startupMetadata.getPageSize()) {
                logger.warn(
                        "Environment {} was created with cache page size equals to " +
                                "{} but provided page size is {} " +
                                "page size will be updated to {}", location,
                        startupMetadata.getPageSize(), config.getCachePageSize(), startupMetadata.getPageSize()
                );

                config.setCachePageSize(startupMetadata.getPageSize());
            }

            if (fileLength != startupMetadata.getFileLengthBoundary()) {
                logger.warn(
                        "Environment {} was created with maximum files size equals to " +
                                "{} but provided file size is {} " +
                                "file size will be updated to {}"
                        , location, startupMetadata.getFileLengthBoundary(), fileLength,
                        startupMetadata.getFileLengthBoundary());

                config.setFileSize(startupMetadata.getFileLengthBoundary() / 1024);
                fileLength = startupMetadata.getFileLengthBoundary();
            }

            fileLengthBound = startupMetadata.getFileLengthBoundary();

            if (fileLengthBound % config.getCachePageSize() != 0L) {
                throw new InvalidSettingException("File size should be a multiple of cache page size.");
            }

            cachePageSize = startupMetadata.getPageSize();

            if (expectedEnvironmentVersion != startupMetadata.getEnvironmentFormatVersion()) {
                throw new ExodusException(
                        "For environment " + location +
                                " expected format version is " + expectedEnvironmentVersion +
                                " but  data are stored using version " + startupMetadata.getEnvironmentFormatVersion()
                );
            }


            var logWasChanged = false;

            formatWithHashCodeIsUsed = !needToPerformMigration;

            var tmpLeftovers = false;
            if (reader instanceof FileDataReader) {
                try (var files = LogUtil.listTlcFiles(new File(location))) {
                    var count = new int[1];

                    files.forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        count[0]++;
                    });

                    if (count[0] > 0) {
                        logger.error(
                                "Database : {}. Temporary files which are used during environment" +
                                        " auto-recovery have been found, typically it indicates that recovery routine finished " +
                                        "incorrectly, triggering database check."
                                , location);
                        tmpLeftovers = true;
                    }
                }
            }

            var blockSetMutable = new BlockSet.Immutable(fileLength).beginWrite();

            for (jetbrains.exodus.io.Block block : reader.getBlocks()) {
                blockSetMutable.add(block.getAddress(), block);
            }

            var incorrectLastSegmentSize = false;
            if (!needToPerformMigration && !blockSetMutable.isEmpty()) {
                var lastAddress = blockSetMutable.getMaximum();
                Objects.requireNonNull(lastAddress);

                var lastBlock = blockSetMutable.getBlock(lastAddress);
                var lastFileLength = lastBlock.length();
                if ((lastFileLength & (cachePageSize - 1)) > 0) {
                    logger.error(
                            "Database : {}. Unexpected size of the last segment {} , " +
                                    "segment size should be quantified by page size. Segment size {}. " +
                                    "Page size {}", location, lastBlock, lastFileLength, cachePageSize
                    );
                    incorrectLastSegmentSize = true;
                }
            }

            boolean checkDataConsistency;
            if (!rwIsReadonly) {
                if (reader instanceof FileDataReader) {
                    if (!startupMetadata.isCorrectlyClosed() || tmpLeftovers
                            || incorrectLastSegmentSize
                    ) {
                        logger.warn(
                                "Environment located at {} has been closed incorrectly. " +
                                        "Data check routine is started to assess data consistency ..."
                                , location);
                        checkDataConsistency = true;
                    } else if (needToPerformMigration) {
                        logger.warn(
                                "Environment located at {} has been created with different version. " +
                                        "Data check routine is started to assess data consistency ..."
                                , location);
                        checkDataConsistency = true;
                    } else if (config.isForceDataCheckOnStart()) {
                        logger.warn(
                                "Environment located at {} will be checked for data consistency " +
                                        "due to the forceDataCheckOnStart option is set to true.", location
                        );
                        checkDataConsistency = true;
                    } else {
                        checkDataConsistency = false;
                    }
                } else if (reader instanceof MemoryDataReader) {
                    checkDataConsistency = true;
                } else {
                    throw new IllegalStateException("Database : " + location + ". Unknown reader type " + reader);
                }
            } else {
                checkDataConsistency = false;
            }

            if (checkDataConsistency) {
                blockSetMutable = new BlockSet.Immutable(fileLength).beginWrite();
                logWasChanged = checkLogConsistencyAndUpdateRootAddress(blockSetMutable);

                logger.info("Data check is completed for environment {}.", location);
            }

            var blockSetImmutable = blockSetMutable.endWrite();

            var memoryUsage = config.getMemoryUsage();
            var generationCount = config.getCacheGenerationCount();

            if (memoryUsage != 0L) {
                cache = getSharedCache(
                        memoryUsage,
                        cachePageSize,
                        generationCount
                );
            } else {
                var memoryUsagePercentage = config.getMemoryUsagePercentage();
                cache = getSharedCache(
                        memoryUsagePercentage, cachePageSize,
                        generationCount
                );
            }

            DeferredIO.getJobProcessor();
            isClosing = false;

            var lastFileAddress = blockSetMutable.getMaximum();
            updateLogIdentity();

            byte[] page;
            long highAddress;

            if (lastFileAddress == null) {
                page = new byte[cachePageSize];
                highAddress = 0;
            } else {
                var lastBlock = blockSetMutable.getBlock(lastFileAddress);

                if (!dataWriter.isOpen()) {
                    dataWriter.openOrCreateBlock(lastFileAddress, lastBlock.length());
                }

                var lastFileLength = lastBlock.length();
                var currentHighAddress = lastFileAddress + lastFileLength;
                var highPageAddress = getHighPageAddress(currentHighAddress);
                var highPageContent = new byte[cachePageSize];


                if (currentHighAddress > 0) {
                    readFromBlock(lastBlock, highPageAddress, highPageContent, currentHighAddress);
                }

                page = highPageContent;
                highAddress = currentHighAddress;

                if (lastFileLength == fileLengthBound) {
                    dataWriter.close();
                }
            }

            if (reader instanceof FileDataReader fileDataReader) {
                fileDataReader.setLog(this);
            }

            var maxWriteBoundary = (int) (fileLengthBound / cachePageSize);
            this.writer = new BufferedDataWriter(
                    this,
                    dataWriter,
                    !needToPerformMigration,
                    getSharedWriteBoundarySemaphore(maxWriteBoundary),
                    maxWriteBoundary, blockSetImmutable, highAddress, page, config.getSyncPeriod()
            );

            var writtenInPage = highAddress & (cachePageSize - 1);
            nullPage = BufferedDataWriter.generateNullPage(cachePageSize);

            if (!rwIsReadonly && writtenInPage > 0) {
                logger.warn(
                        "Page {} is not written completely, fixing it. " +
                                "Environment : {}, file : {}."
                        , (highAddress & (long) (cachePageSize - 1)),
                        location, LogUtil.getLogFilename(getFileAddress(highAddress)));

                if (needToPerformMigration) {
                    padWholePageWithNulls();
                } else {
                    padPageWithNulls();
                }

                logWasChanged = true;
            }

            if (logWasChanged) {
                logger.info("Log {} content was changed during the initialization, data sync is performed.",
                        location);

                beginWrite();
                try {
                    writer.sync();
                } finally {
                    endWrite();
                }
            }

            if (config.isWarmup()) {
                warmup();
            }

            Files.deleteIfExists(backupLocation);

            if (needToPerformMigration) {
                switchToReadOnlyMode();
            }
        } catch (RuntimeException ex) {
            release();
            throw ex;
        }
    }

    public long[] getAllFileAddresses() {
        return writer.allFiles();
    }

    public long getHighAddress() {
        return writer.getHighAddress();
    }

    public long getHighFileAddress() {
        return getFileAddress(getHighAddress());
    }

    void warmup() {
        // do warmup asynchronously
        var processor = DeferredIO.getJobProcessor();
        processor.queue(new RunnableJob(() -> {
            // number of files to walk through at maximum
            var maxFiles = cache.getMemoryUsage() / fileLengthBound;
            var fileAddresses = getAllFileAddresses();
            var filesCount = (int) Math.min(fileAddresses.length, Math.max(1, maxFiles));
            var files = Arrays.copyOfRange(fileAddresses, fileAddresses.length - filesCount, fileAddresses.length);

            var size = files.length;
            var it = new DataIterator(this);
            var pageSize = config.getCachePageSize();
            logger.info("Warming LogCache up with newest {} file(s) at {}", size, location);

            processor.queue(new IterableJob<>(Arrays.stream(files).boxed().collect(Collectors.toList()),
                    address -> {
                        logger.info("Warming up {}", LogUtil.getLogFilename(address));
                        long pageAddress = address;
                        while (pageAddress < address + fileLengthBound && pageAddress + pageSize < getHighAddress()) {
                            it.checkPage(pageAddress);
                            pageAddress += pageSize;
                        }
                        return null;
                    }));
        }));
    }

    public void switchToReadOnlyMode() {
        rwIsReadonly = true;
    }

    private void padWholePageWithNulls() {
        beginWrite();
        try {
            writer.padWholePageWithNulls();
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
        } finally {
            endWrite();
        }
    }

    private void padPageWithNulls() {
        beginWrite();
        try {
            writer.padPageWithNulls();
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
        } finally {
            endWrite();
        }
    }

    public long beginWrite() {
        writeThread = Thread.currentThread();
        return writer.beginWrite();
    }

    public long endWrite() {
        writeThread = null;
        return writer.endWrite();
    }

    public long getFileAddress(long address) {
        return address - address % fileLengthBound;
    }

    public long getCreated() {
        return created;
    }

    public boolean isClosedCorrectly() {
        return startupMetadata.isCorrectlyClosed();
    }

    public boolean getFormatWithHashCodeIsUsed() {
        return formatWithHashCodeIsUsed;
    }

    public boolean getRestoredFromBackup() {
        return restoredFromBackup;
    }


    private void readFromBlock(
            Block block,
            long pageAddress,
            byte[] output,
            long highAddress
    ) {
        var readBytes = block.read(
                output,
                pageAddress - block.getAddress(), 0, output.length
        );

        var lastPage = highAddress & (-(long) cachePageSize);
        var checkConsistency = config.isCheckPagesAtRuntime() &&
                formatWithHashCodeIsUsed &&
                (!rwIsReadonly || pageAddress < lastPage);
        checkConsistency = formatWithHashCodeIsUsed &&
                (checkConsistency || readBytes == cachePageSize || readBytes == 0);

        if (checkConsistency) {
            if (readBytes < cachePageSize) {
                DataCorruptionException.raise(
                        "Page size less than expected. " +
                                "{actual : " + readBytes + ", expected " + cachePageSize + " }.", this, pageAddress
                );
            }

            BufferedDataWriter.checkPageConsistency(pageAddress, output, cachePageSize, this);
        }

        var cipherProvider = config.getCipherProvider();
        if (cipherProvider != null) {
            int encryptedBytes;
            if (readBytes < cachePageSize) {
                encryptedBytes = readBytes;
            } else {
                if (formatWithHashCodeIsUsed) {
                    encryptedBytes = cachePageSize - BufferedDataWriter.HASH_CODE_SIZE;
                } else {
                    encryptedBytes = cachePageSize;
                }
            }

            cryptBlocksMutable(
                    cipherProvider, config.getCipherKey(), config.getCipherBasicIV(),
                    pageAddress, output, 0, encryptedBytes, LogUtil.LOG_BLOCK_ALIGNMENT
            );
        }
        notifyReadBytes(output, readBytes);
    }

    public void addBlockListener(BlockListener listener) {
        blockListeners.add(listener);
    }

    public void addReadBytesListener(ReadBytesListener listener) {
        readBytesListeners.add(listener);
    }

    private void notifyReadBytes(byte[] bytes, int count) {
        readBytesListeners.forEach(listener -> listener.bytesRead(bytes, count));
    }

    void notifyBeforeBlockDeleted(Block block) {
        blockListeners.forEach(listener -> listener.beforeBlockDeleted(block));
    }

    void notifyAfterBlockDeleted(long address) {
        blockListeners.forEach(listener -> listener.afterBlockDeleted(address));
    }

    void notifyBlockCreated(Block block) {
        blockListeners.forEach(listener -> listener.blockCreated(block));

    }

    void notifyBlockModified(Block block) {
        blockListeners.forEach(listener -> listener.blockModified(block));
    }

    public void clearFileFromLogCache(long address, long offset) {
        var off = offset;
        while (off < fileLengthBound) {
            cache.removePage(this, address + off);
            off += cachePageSize;
        }
    }

    public boolean isImmutableFile(long fileAddress) {
        return fileAddress + fileLengthBound <= writer.getHighAddress();
    }

    public long getNextFileAddress(long fileAddress) {
        var files = writer.getFilesFrom(fileAddress);

        if (files.hasNext()) {
            var result = files.nextLong();
            if (result != fileAddress) {
                throw new ExodusException("Database " + getLocation() + ".There is no file by address " + fileAddress);
            }
            if (files.hasNext()) {
                return files.nextLong();
            }
        }

        return Loggable.NULL_ADDRESS;
    }

    public long getLowFileAddress() {
        var result = writer.getMinimumFile();
        if (result == null) {
            return Loggable.NULL_ADDRESS;
        }

        return result;
    }

    public void flush() {
        if (config.isDurableWrite()) {
            sync();
        } else {
            writer.flush();
        }
    }

    public void sync() {
        writer.sync();
        writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
    }

    private long getHighPageAddress(long highAddress) {
        var alignment = ((int) highAddress) & (cachePageSize - 1);
        if (alignment == 0 && highAddress > 0) {
            alignment = cachePageSize;
        }
        return highAddress - alignment; // aligned address
    }

    @Override
    public void close() throws IOException {
        isClosing = true;

        if (!rwIsReadonly) {
            var highAddress = writer.getHighAddress();
            beginWrite();
            try {
                if (formatWithHashCodeIsUsed && (((int) highAddress) & (cachePageSize - 1)) != 0) {
                    //we pad page with nulls to ensure that all pages could be checked on consistency
                    //by hash code which is stored at the end of the page.
                    var written = writer.padPageWithNulls();
                    if (written == 0) {
                        throw new ExodusException("Invalid value of tip of the log $highAddress");
                    }

                    writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());

                }

                sync();
            } finally {
                endWrite();
            }

            if (reader instanceof FileDataReader fileDataReader) {
                startupMetadata.closeAndUpdate(fileDataReader);
            }
        }

        writer.close(!rwIsReadonly);
        reader.close();

        var backupLocation = Path.of(location).resolve(BackupMetadata.BACKUP_METADATA_FILE_NAME);
        Files.deleteIfExists(backupLocation);

        release();
    }

    public void clear() {
        cache.clear();
        reader.close();
        writer.clear();

        updateLogIdentity();
    }


    private void tryLock() {
        if (!config.isLockIgnored()) {
            var lockTimeout = config.getLockTimeout();
            if (!dataWriter.lock(lockTimeout)) {
                var exceptionMessage = new StringBuilder();
                exceptionMessage.append(
                        "Can't acquire environment lock after "
                ).append(lockTimeout).append(" ms.\n\n Lock owner info: \n").append(dataWriter.lockInfo());
                if (dataWriter instanceof AsyncFileDataWriter asyncFileDataWriter) {
                    exceptionMessage.append("\n Lock file path: ").append(asyncFileDataWriter.lockFilePath());
                }
                throw new ExodusException(exceptionMessage.toString());
            }
        }
    }

    public void release() {
        if (!config.isLockIgnored()) {
            dataWriter.release();
        }
    }

    private boolean checkLogConsistencyAndUpdateRootAddress(BlockSet.Mutable blockSetMutable) throws IOException {
        var newRootAddress = checkLogConsistency(
                blockSetMutable,
                startupMetadata.getRootAddress()
        );

        var logWasChanged = false;
        if (newRootAddress != Long.MIN_VALUE) {
            startupMetadata.setRootAddress(newRootAddress);
            logWasChanged = true;

            SharedOpenFilesCache.invalidate();

            dataWriter.close();
            var lastBlockAddress = blockSetMutable.getMaximum();

            if (lastBlockAddress != null) {
                var lastBlock = blockSetMutable.getBlock(lastBlockAddress);
                dataWriter.openOrCreateBlock(lastBlockAddress, lastBlock.length());
            }
        }

        return logWasChanged;
    }

    private long checkLogConsistency(BlockSet.Mutable blockSetMutable, long loadedDbRootAddress) throws IOException {
        var blockIterator = reader.getBlocks().iterator();
        if (!blockIterator.hasNext()) {
            return Long.MIN_VALUE;
        }

        if (config.isCleanDirectoryExpected()) {
            throw new ExodusException("Database : " + location + " . Clean log is expected");
        }

        var blocks = new TreeMap<Long, Block>();
        while (blockIterator.hasNext()) {
            var block = blockIterator.next();
            blocks.put(block.getAddress(), block);
        }


        logger.info("Database {} . Data files found in directory ...", location);
        logger.info("------------------------------------------------------");
        for (Long address : blocks.keySet()) {
            logger.info(LogUtil.getLogFilename(address));
        }
        logger.info("------------------------------------------------------");

        var clearInvalidLog = config.isClearInvalidLog();
        boolean hasNext;

        var dbRootAddress = Long.MIN_VALUE;
        var dbRootEndAddress = Long.MIN_VALUE;

        String nextBlockCorruptionMessage = null;
        var corruptedFileAddress = -1L;
        var loggablesProcessed = 0;

        var fileBlockIterator = blocks.values().iterator();
        try {
            do {
                if (nextBlockCorruptionMessage != null) {
                    DataCorruptionException.raise(nextBlockCorruptionMessage, this, corruptedFileAddress);
                }

                var block = fileBlockIterator.next();
                var startBlockAddress = block.getAddress();
                var endBlockAddress = startBlockAddress + fileLengthBound;

                logger.info("Database {}. File {} is being verified.",
                        location, LogUtil.getLogFilename(startBlockAddress));

                var blockLength = block.length();
                // if it is not the last file and its size is not as expected
                hasNext = fileBlockIterator.hasNext();

                if (blockLength > fileLengthBound || hasNext && blockLength != fileLengthBound || blockLength == 0L) {
                    nextBlockCorruptionMessage = "Unexpected file length. " +
                            "Expected length : " + fileLengthBound + " , actual file length :" + blockLength + ".";
                    corruptedFileAddress = startBlockAddress;

                    if (blockLength == 0L) {
                        continue;
                    }
                }

                // if the file address is not a multiple of fileLengthBound
                if (startBlockAddress != getFileAddress(startBlockAddress)) {
                    DataCorruptionException.raise(
                            "Unexpected file address. Expected " + getFileAddress(startBlockAddress) +
                                    ", actual " + startBlockAddress,
                            this,
                            startBlockAddress
                    );
                }

                var blockDataIterator = new BlockDataIterator(
                        this, block, startBlockAddress,
                        formatWithHashCodeIsUsed
                );
                while (blockDataIterator.hasNext()) {
                    var loggableAddress = blockDataIterator.getAddress();

                    if (loggableAddress >= endBlockAddress) {
                        break;
                    }

                    var loggableType = (byte) (blockDataIterator.next() ^ (byte) 0x80);
                    if (loggableType < 0 && config.isSkipInvalidLoggableType()) {
                        continue;
                    }

                    checkLoggableType(loggableType, loggableAddress);

                    if (NullLoggable.isNullLoggable(loggableType)) {
                        loggablesProcessed++;
                        continue;
                    }

                    if (HashCodeLoggable.isHashCodeLoggable(loggableType)) {
                        for (var i = 0; i < Long.SIZE; i++) {
                            blockDataIterator.next();
                        }
                        loggablesProcessed++;
                        continue;
                    }

                    var structureId = CompressedUnsignedLongByteIterable.getInt(blockDataIterator);
                    checkStructureId(structureId, loggableAddress);

                    var dataLength = CompressedUnsignedLongByteIterable.getInt(blockDataIterator);
                    checkDataLength(dataLength, loggableAddress);

                    if (blockDataIterator.getAddress() >= endBlockAddress) {
                        break;
                    }

                    if (loggableType == DatabaseRoot.DATABASE_ROOT_TYPE) {
                        if (structureId != Loggable.NO_STRUCTURE_ID) {
                            DataCorruptionException.raise(
                                    "Invalid structure id " + structureId + " for root loggable.",
                                    this, loggableAddress
                            );
                        }

                        var loggableData = new byte[dataLength];
                        var dataAddress = blockDataIterator.getAddress();

                        for (var i = 0; i < dataLength; i++) {
                            loggableData[i] = blockDataIterator.next();
                        }

                        var rootLoggable = new SinglePageLoggable(
                                loggableAddress,
                                blockDataIterator.getAddress(),
                                loggableType,
                                structureId,
                                dataAddress,
                                loggableData, 0, dataLength
                        );

                        var dbRoot = new DatabaseRoot(rootLoggable);
                        if (dbRoot.isValid()) {
                            dbRootAddress = loggableAddress;
                            dbRootEndAddress = blockDataIterator.getAddress();
                        } else {
                            DataCorruptionException.raise(
                                    "Corrupted database root was found", this,
                                    loggableAddress
                            );
                        }
                    } else {
                        for (var i = 0; i < dataLength; i++) {
                            blockDataIterator.next();
                        }
                    }

                    loggablesProcessed++;
                }

                blockSetMutable.add(startBlockAddress, block);
                if (!hasNext && nextBlockCorruptionMessage != null) {
                    DataCorruptionException.raise(nextBlockCorruptionMessage, this, corruptedFileAddress);
                }
            } while (hasNext);
        } catch (Exception exception) {
            logger.error("Error during verification of database $location", exception);

            SharedOpenFilesCache.invalidate();
            try {
                if (clearInvalidLog) {
                    clearDataCorruptionLog(exception, blockSetMutable);
                    return -1;
                }

                if (dbRootEndAddress > 0) {
                    var endBlockAddress = getFileAddress(dbRootEndAddress);
                    var blocksToTruncate = blocks.tailMap(endBlockAddress, true);


                    if (!config.isProceedDataRestoreAtAnyCost() && blocksToTruncate.size() > 2) {
                        throw new DataCorruptionException(
                                "Database " + location + " is corrupted, " +
                                        "but to be restored till the valid state " + blocksToTruncate.size() +
                                        "segments out of " + blocks.size() +
                                        "should be truncated. Segment size " + fileLengthBound + " bytes. Data restore is aborted. " +
                                        "To proceed data restore with such data loss " +
                                        "please restart database with parameter " + EnvironmentConfig.LOG_PROCEED_DATA_RESTORE_AT_ANY_COST +
                                        " set to true"
                        );
                    }

                    if (reader instanceof FileDataReader) {
                        copyFilesInRestoreTempDir(
                                blocksToTruncate.values().stream().
                                        map(block -> ((FileDataReader.FileBlock) block).toPath()).iterator()
                        );
                    }

                    var blocksToTruncateIterator = blocksToTruncate.values().iterator();
                    var endBlock = blocksToTruncateIterator.next();
                    var endBlockLength = dbRootEndAddress % fileLengthBound;
                    var endBlockReminder = ((int) endBlockLength) & (cachePageSize - 1);

                    logger.error(
                            "Data corruption was detected. Reason : \"{}\". " +
                                    "Database '{}' will be truncated till address : {}. " +
                                    "Name of the file to be truncated : {}. " +
                                    "Initial file size {} bytes, final file size {} bytes."
                            , exception.getMessage(), location, dbRootEndAddress,
                            LogUtil.getLogFilename(endBlockAddress), endBlock.length(), endBlockLength);

                    if (blocksToTruncate.size() > 1) {
                        var filesToTruncate = blocksToTruncate.keySet().stream().skip(1).
                                map(LogUtil::getLogFilename).collect(Collectors.joining(", "));

                        logger.error("The following files will be deleted : {}.", filesToTruncate);
                    }


                    if (endBlock instanceof FileDataReader.FileBlock fileBlock && !fileBlock.canWrite()) {
                        if (!fileBlock.setWritable(true)) {
                            throw new ExodusException(
                                    "Can not write into file " + fileBlock.getAbsolutePath(),
                                    exception
                            );
                        }
                    }

                    var position = dbRootEndAddress % fileLengthBound - endBlockReminder;
                    byte[] lastPage;

                    if (endBlockReminder > 0) {
                        lastPage = new byte[cachePageSize];

                        var read = endBlock.read(lastPage, position, 0, endBlockReminder);
                        if (read != endBlockReminder) {
                            throw new ExodusException(
                                    "Can not read segment " + LogUtil.getLogFilename(endBlock.getAddress()),
                                    exception
                            );
                        }

                        Arrays.fill(lastPage, read, lastPage.length, (byte) 0x80);
                        var cipherProvider = config.getCipherProvider();
                        if (cipherProvider != null) {
                            cryptBlocksMutable(
                                    cipherProvider, config.getCipherKey(), config.getCipherBasicIV(),
                                    endBlock.getAddress() + position, lastPage, read, lastPage.length - read,
                                    LogUtil.LOG_BLOCK_ALIGNMENT
                            );
                        }
                        BufferedDataWriter.updatePageHashCode(lastPage);
                        SharedOpenFilesCache.invalidate();
                    } else {
                        lastPage = null;
                    }

                    if (reader instanceof FileDataReader) {
                        try {
                            assert endBlock instanceof FileDataReader.FileBlock;
                            truncateFile((FileDataReader.FileBlock) endBlock, position, lastPage);
                        } catch (IOException e) {
                            logger.error("Error during truncation of file " + endBlock, e);
                            throw new ExodusException("Can not restore log corruption", exception);
                        }
                    } else if (reader instanceof MemoryDataReader) {
                        assert endBlock instanceof MemoryDataReader.MemoryBlock;
                        var memoryBlock = (MemoryDataReader.MemoryBlock) endBlock;
                        memoryBlock.truncate((int) position);
                        if (lastPage != null) {
                            memoryBlock.write(lastPage, 0, cachePageSize);
                        }
                    } else {
                        throw new ExodusException("Invalid reader type : " + reader);
                    }

                    blockSetMutable.add(endBlockAddress, endBlock);

                    if (blocksToTruncateIterator.hasNext()) {
                        var blockToDelete = blocksToTruncateIterator.next();
                        logger.info("Database {}. File {} will be deleted.", location,
                                LogUtil.getLogFilename(blockToDelete.getAddress()));
                        blockSetMutable.remove(blockToDelete.getAddress());

                        if (reader instanceof FileDataReader) {
                            var fileBlockToDelete = getFileBlockToDelete(exception, (FileDataReader.FileBlock) blockToDelete);
                            Files.deleteIfExists(fileBlockToDelete.toPath());
                        } else {
                            MemoryDataReader memoryDataReader = (MemoryDataReader) reader;
                            memoryDataReader.getMemory().removeBlock(blockToDelete.getAddress());
                        }
                    }
                } else {
                    if (loggablesProcessed < 3) {
                        logger.error(
                                "Data corruption was detected. Reason : {} . Likely invalid cipher key/iv were used. ",
                                exception.getMessage()
                        );

                        blockSetMutable.clear();
                        throw new InvalidCipherParametersException();
                    } else {
                        clearDataCorruptionLog(exception, blockSetMutable);
                        return -1;
                    }
                }

                rwIsReadonly = false;
                logger.error("Data corruption was fixed for database {}.", location);

                if (loadedDbRootAddress != dbRootAddress) {
                    logger.warn(
                            "DB root address stored in log metadata and detected are different. " +
                                    "Root address will be fixed. Stored address : {} , detected  {} .",
                            loadedDbRootAddress, dbRootAddress);
                }

                return dbRootAddress;
            } catch (InvalidCipherParametersException | DataCorruptionException e) {
                throw e;
            } catch (Exception e) {
                logger.error("Error during attempt to restore database " + location, e);
                throw new ExodusException(exception);
            }
        }

        if (loadedDbRootAddress != dbRootAddress) {
            logger.warn(
                    "DB root address stored in log metadata and detected are different. " +
                            "Root address will be fixed. Stored address : {} , detected {}.",
                    loadedDbRootAddress, dbRootAddress);
            return dbRootAddress;
        }

        return Long.MIN_VALUE;
    }

    @NotNull
    private static FileDataReader.FileBlock getFileBlockToDelete(Exception exception, FileDataReader.FileBlock blockToDelete) {
        if (!blockToDelete.canWrite()) {
            if (!blockToDelete.setWritable(true)) {
                throw new ExodusException(
                        "Can not write into file " + blockToDelete.getAbsolutePath(),
                        exception
                );
            }
        }

        return blockToDelete;
    }

    private void clearDataCorruptionLog(Exception exception, BlockSet.Mutable blockSetMutable) {
        logger.error(
                "Data corruption was detected. Reason : " + exception.getMessage() + " . " +
                        "Environment " + location + " will be cleared."
        );

        blockSetMutable.clear();
        dataWriter.clear();

        rwIsReadonly = false;
    }


    private void checkDataLength(int dataLength, long loggableAddress) {
        if (dataLength < 0) {
            DataCorruptionException.raise(
                    "Loggable with negative length was encountered",
                    this, loggableAddress
            );
        }

        if (dataLength > fileLengthBound) {
            DataCorruptionException.raise(
                    "Loggable with length bigger than allowed value was discovered.",
                    this, loggableAddress
            );
        }
    }

    private void checkStructureId(int structureId, long loggableAddress) {
        if (structureId < 0) {
            DataCorruptionException.raise(
                    "Loggable with negative structure id was encountered",
                    this, loggableAddress
            );
        }
    }

    private void checkLoggableType(byte loggableType, long loggableAddress) {
        if (loggableType < 0) {
            DataCorruptionException.raise("Loggable with negative type", this, loggableAddress);
        }
    }


    private void copyFilesInRestoreTempDir(Iterator<Path> files) throws IOException {
        var tempDir = createTempRestoreDirectoryWithDate();
        logger.info("Database {} - copying files into the temp directory {} before data restore.", location, tempDir);

        while (files.hasNext()) {
            var file = files.next();
            logger.info("Database {} - file {} is copied into the temp directory {}", location, file, tempDir);
            Files.copy(file, tempDir.resolve(file.getFileName()));
        }

        logger.info("Database {} - copying of files into the temp directory {} is completed.", location, tempDir);
    }

    private Path createTempRestoreDirectoryWithDate() throws IOException {
        var dtf = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss");
        var now = LocalDateTime.now();
        var date = dtf.format(now);

        var dirName = "restore-" + date + "-";
        return Files.createTempDirectory(dirName).toAbsolutePath();
    }

    private void updateLogIdentity() {
        identity = identityGenerator.nextId();
    }

    private LogCache getSharedCache(
            int memoryUsagePercentage,
            int pageSize,
            int cacheGenerationCount
    ) {
        var result = sharedCache;
        if (result == null) {
            sharedResourcesLock.lock();
            try {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(
                            memoryUsagePercentage, pageSize,
                            cacheGenerationCount
                    );
                }
                result = sharedCache;
            } finally {
                sharedResourcesLock.unlock();
            }
        }
        checkCachePageSize(pageSize, result);
        return result;
    }

    private LogCache getSharedCache(
            long memoryUsage,
            int pageSize,
            int cacheGenerationCount
    ) {
        var result = sharedCache;
        if (result == null) {
            sharedResourcesLock.lock();
            try {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(
                            memoryUsage, pageSize,
                            cacheGenerationCount
                    );
                }
                result = sharedCache;
            } finally {
                sharedResourcesLock.unlock();
            }
        }

        checkCachePageSize(pageSize, result);
        return result;
    }

    private void truncateFile(
            File endBlock,
            long position,
            byte[] lastPage
    ) throws IOException {
        var endBlockBackupPath =
                Path.of(location).resolve(
                        endBlock.getName().substring(
                                0,
                                endBlock.getName().length() - LogUtil.LOG_FILE_EXTENSION_LENGTH
                        ) +
                                LogUtil.TMP_TRUNCATION_FILE_EXTENSION
                );


        Files.copy(
                Path.of(endBlock.toURI()),
                endBlockBackupPath,
                StandardCopyOption.REPLACE_EXISTING
        );

        try (var raf = new RandomAccessFile(endBlockBackupPath.toFile(), "rw")) {
            if (lastPage != null) {
                raf.seek(position);
                raf.write(lastPage);
                raf.setLength(position + cachePageSize);
            } else {
                raf.setLength(position);
            }

            raf.getFD().sync();
        }

        try {
            Files.move(
                    endBlockBackupPath, Path.of(endBlock.toURI()),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
            );
        } catch (AtomicMoveNotSupportedException moveNotSupported) {
            logger.warn(
                    "Database : " + location + ". " +
                            "Atomic move is not supported by the file system and can not be used during " +
                            "log restore. Falling back to the non-atomic move."
            );
            Files.move(
                    endBlockBackupPath, Path.of(endBlock.toURI()),
                    StandardCopyOption.REPLACE_EXISTING
            );
        }

        try (var raf = new RandomAccessFile(endBlock, "rw")) {
            raf.getFD().sync();
        }
    }

    public void forgetFileTestsOnly(long address) {
        beginWrite();
        forgetFiles(new long[]{address});
        endWrite();
    }

    public void forgetFiles(long[] files) {
        writer.forgetFiles(files, fileLengthBound);
    }

    public boolean needsToBeSynchronized() {
        return writer.needsToBeSynchronized();
    }

    public void removeFile(
            long address,
            RemoveBlockType rbt
    ) {
        writer.removeBlock(address, rbt);
    }

    int getWrittenFilesSize() {
        return writer.getFilesSize();
    }

    public boolean isReadOnly() {
        return rwIsReadonly;
    }

    public void updateStartUpDbRoot(long rootAddress) {
        startupMetadata.setRootAddress(rootAddress);
    }

    public void removeFile(long address) {
        removeFile(address, RemoveBlockType.Delete);
    }

    public float getCacheHitRate() {
        return cache.hitRate();
    }

    public long getNumberOfFiles() {
        return writer.numberOfFiles();
    }

    public long getFileLengthBound() {
        return fileLengthBound;
    }

    public String getLocation() {
        return location;
    }

    boolean isClosing() {
        return isClosing;
    }

    public int dataSpaceLeftInPage(long address) {
        var pageAddress = (~((long) (cachePageSize - 1))) & address;
        var writtenSpace = address - pageAddress;

        assert writtenSpace >= 0 && writtenSpace < cachePageSize - BufferedDataWriter.HASH_CODE_SIZE;
        return cachePageSize - BufferedDataWriter.HASH_CODE_SIZE - (int) writtenSpace;
    }

    @NotNull
    public LoggableIterator getLoggableIterator(long startAddress) {
        return new LoggableIterator(this, startAddress, getHighAddress());
    }

    public long tryWrite(byte type, int structureId, ByteIterable data, ExpiredLoggableCollection expiredLoggables) {
        // allow new file creation only if new file starts loggable
        var result = writeContinuously(type, structureId, data, expiredLoggables);
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls(expiredLoggables);
        }
        return result;
    }

    /**
     * Writes a loggable to the end of the log padding the log with nulls if necessary.
     * So auto-alignment guarantees the loggable to be placed in a single file.
     *
     * @param loggable - loggable to write.
     * @return address where the loggable was placed.
     */
    public long write(Loggable loggable, ExpiredLoggableCollection expiredLoggables) {
        return write(loggable.getType(), loggable.getStructureId(), loggable.getData(), expiredLoggables);
    }

    public long getWrittenHighAddress() {
        return writer.getCurrentHighAddress();
    }

    public boolean isLastWrittenFileAddress(long address) {
        return getFileAddress(address) == getFileAddress(getWrittenHighAddress());
    }

    public long write(byte type, int structureId, ByteIterable data, ExpiredLoggableCollection expiredLoggables) {
        // allow new file creation only if new file starts loggable
        var result = writeContinuously(type, structureId, data, expiredLoggables);
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            doPadWithNulls(expiredLoggables);
            result = writeContinuously(type, structureId, data, expiredLoggables);
            if (result < 0) {
                throw new TooBigLoggableException();
            }
        }
        return result;
    }

    public boolean hasAddressRange(long from, long to) {
        var fileAddress = getFileAddress(from);
        var files = writer.getFilesFrom(fileAddress);
        var highAddress = writer.getHighAddress();

        do {
            if (!files.hasNext() || files.nextLong() != fileAddress) {
                return false;
            }
            fileAddress += getFileSize(fileAddress, highAddress);
        } while (fileAddress >= (from + 1) && fileAddress <= to);

        return true;
    }

    public long writeContinuously(
            byte type, int structureId, ByteIterable data,
            ExpiredLoggableCollection expiredLoggables) {
        if (rwIsReadonly) {
            throw new ExodusException("Database " + location + "  is working in read-only mode. No writes are allowed.");
        }
        if (isClosing) {
            throw new ExodusException("Database " + location + "  is closed.");
        }

        var result = beforeWrite();

        var isNull = NullLoggable.isNullLoggable(type);
        var recordLength = 1;

        if (isNull) {
            writer.write((byte) (type ^ ((byte) 0x80)));
        } else {
            var structureIdIterable = CompressedUnsignedLongByteIterable.getIterable(structureId);
            var dataLength = data.getLength();
            var dataLengthIterable = CompressedUnsignedLongByteIterable.getIterable(dataLength);
            recordLength += structureIdIterable.getLength();
            recordLength += dataLengthIterable.getLength();
            recordLength += dataLength;

            var leftInPage =
                    cachePageSize - (((int) result) & (cachePageSize - 1)) - BufferedDataWriter.HASH_CODE_SIZE;
            int delta;
            if (leftInPage >= 1 && leftInPage < recordLength && recordLength < (cachePageSize >>> 4)) {
                delta = leftInPage + BufferedDataWriter.HASH_CODE_SIZE;
            } else {
                delta = 0;
            }

            if (!writer.fitsIntoSingleFile(fileLengthBound, recordLength + delta)) {
                return -1L;
            }

            if (delta > 0) {
                var gapAddress = writer.getCurrentHighAddress();
                var written = writer.padPageWithNulls();

                assert written == delta;
                result += written;
                expiredLoggables.add(gapAddress, written);

                assert result % cachePageSize == 0L;
            }

            writer.write((byte) (type ^ ((byte) 0x80)));

            writeByteIterable(writer, structureIdIterable);
            writeByteIterable(writer, dataLengthIterable);

            if (dataLength > 0) {
                writeByteIterable(writer, data);
            }
        }

        writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
        return result;
    }

    public ArrayByteIterable getPageIterable(long pageAddress) {
        return cache.getPageIterable(this, pageAddress, formatWithHashCodeIsUsed);
    }

    public byte[] getCachedPage(long pageAddress) {
        return cache.getPage(this, pageAddress, -1);
    }

    public long getStartUpDbRoot() {
        return startupMetadata.getRootAddress();
    }

    public byte getWrittenLoggableType(long address, byte max) {
        var pageOffset = address & (long) (cachePageSize - 1);
        var pageAddress = address - pageOffset;

        var page = writer.getCurrentlyWritten(pageAddress);
        if (page == null) {
            page = getCachedPage(pageAddress);
        }

        var type = (byte) (page[(int) pageOffset] ^ (byte) 0x80);
        if (type > max) {
            throw new ExodusException("Database " + location + ". Invalid loggable type : " + type + ".");
        }

        return type;
    }


    public boolean hasAddress(long address) {
        var fileAddress = getFileAddress(address);
        var files = writer.getFilesFrom(fileAddress);
        var highAddress = writer.getHighAddress();

        if (!files.hasNext()) {
            return false;
        }

        var leftBound = files.nextLong();
        return leftBound == fileAddress && leftBound + getFileSize(leftBound, highAddress) > address;
    }


    public long adjustLoggableAddress(long address, long offset) {
        if (!formatWithHashCodeIsUsed) {
            return address + offset;
        }

        var cachePageReminderMask = (long) (cachePageSize - 1);
        var writtenInPage = address & cachePageReminderMask;
        var pageAddress = address & (~cachePageReminderMask);

        var adjustedPageSize = cachePageSize - BufferedDataWriter.HASH_CODE_SIZE;
        var writtenSincePageStart = writtenInPage + offset;
        var fullPages = writtenSincePageStart / adjustedPageSize;

        return pageAddress + writtenSincePageStart + fullPages * BufferedDataWriter.HASH_CODE_SIZE;
    }


    /**
     * Reads a random access loggable by specified address in the log.
     *
     * @param address - location of a loggable in the log.
     * @return instance of a loggable.
     */
    public RandomAccessLoggable read(long address) {
        return read(readIteratorFrom(address), address);
    }

    /**
     * Returns iterator which reads raw bytes of the log starting from specified address.
     *
     * @return instance of ByteIterator
     */
    DataIterator readIteratorFrom(long address) {
        return new DataIterator(this, address);
    }


    public RandomAccessLoggable read(ByteIteratorWithAddress it) {
        return read(it, it.getAddress());
    }

    public LogCache getCache() {
        return cache;
    }

    public int getCachePageSize() {
        return cachePageSize;
    }

    public LogConfig getConfig() {
        return config;
    }

    RandomAccessLoggable read(ByteIteratorWithAddress it, long address) {
        var type = (byte) (it.next() ^ ((byte) 0x80));
        if (NullLoggable.isNullLoggable(type)) {
            return NullLoggable.create(address, adjustLoggableAddress(address, 1));
        }
        if (HashCodeLoggable.isHashCodeLoggable(type)) {
            return new HashCodeLoggable(address, it.getOffset(), it.getCurrentPage());
        }

        return read(type, it, address);
    }

    /**
     * Just like [.read] reads loggable which never can be a [NullLoggable].
     *
     * @return a loggable which is not[NullLoggable]
     */
    public RandomAccessLoggable readNotNull(ByteIteratorWithAddress it, long address) {
        return read((byte) (it.next() ^ (byte) 0x80), it, address);
    }

    private RandomAccessLoggable read(byte type, ByteIteratorWithAddress it, long address) {
        if (isClosing) {
            throw new ExodusException("Database " + location + " is closed.");
        }

        checkLoggableType(type, address);

        var structureId = CompressedUnsignedLongByteIterable.getInt(it);
        checkStructureId(structureId, address);

        var dataLength = CompressedUnsignedLongByteIterable.getInt(it);
        checkDataLength(dataLength, address);

        var dataAddress = it.getAddress();

        if (dataLength > 0 && it.availableInCurrentPage(dataLength)) {
            var end = dataAddress + dataLength;

            return new SinglePageLoggable(
                    address, end,
                    type, structureId, dataAddress, it.getCurrentPage(),
                    it.getOffset(), dataLength
            );
        }

        var data = new MultiPageByteIterableWithAddress(dataAddress, dataLength, this);

        return new MultiPageLoggable(
                address,
                type, data, dataLength, structureId, this
        );
    }


    void readBytes(byte[] output, long pageAddress) {
        var fileAddress = getFileAddress(pageAddress);

        var writerThread = Thread.currentThread() == writeThread;

        LongIterator files;
        if (writerThread) {
            files = writer.getWriterFiles(fileAddress);
        } else {
            files = writer.getFilesFrom(fileAddress);
        }

        var highAddress = getHighReadAddress();
        if (files.hasNext()) {
            var leftBound = files.nextLong();
            var fileSize = getFileSize(leftBound, highAddress);

            if (leftBound == fileAddress && fileAddress + fileSize > pageAddress) {
                Block block;
                if (writerThread) {
                    block = writer.getWriterBlock(fileAddress);
                } else {
                    block = writer.getBlock(fileAddress);
                }

                readFromBlock(block, pageAddress, output, highAddress);
                return;
            }

            var minimumFile = writer.getMinimumFile();
            if (minimumFile != null && fileAddress < minimumFile) {
                BlockNotFoundException.raise(
                        "Address is out of log space, underflow",
                        this, pageAddress
                );
            }

            Long maxFile;
            if (writerThread) {
                maxFile = writer.getMaximumFile();
            } else {
                maxFile = writer.getMaximumWritingFile();
            }

            if (maxFile == null || fileAddress >= maxFile) {
                BlockNotFoundException.raise(
                        "Address is out of log space, overflow",
                        this, pageAddress
                );
            }
        }

        BlockNotFoundException.raise(this, pageAddress);
    }

    public long getFileSize(long fileAddress) {
        return getFileSize(fileAddress, writer.getHighAddress());
    }

    private long getLastFileSize(long fileAddress, long highAddress) {
        var result = highAddress % fileLengthBound;
        if (result == 0L && highAddress != fileAddress) {
            return fileLengthBound;
        }

        return result;
    }

    private boolean isLastFileAddress(long address, long highAddress) {
        return getFileAddress(address) == getFileAddress(highAddress);
    }


    long getFileSize(long fileAddress, long highAddress) {
        // readonly files (not last ones) all have the same size
        if (!isLastFileAddress(fileAddress, highAddress)) {
            return fileLengthBound;
        }

        return getLastFileSize(fileAddress, highAddress);
    }

    public long getHighReadAddress() {
        if (writeThread != null && writeThread == Thread.currentThread()) {
            return writer.getCurrentHighAddress();
        }

        return writer.getHighAddress();
    }

    private static void checkCachePageSize(int pageSize, LogCache cache) {
        var cachePageSize = cache.getPageSize();
        if (cachePageSize != pageSize) {
            throw new ExodusException(
                    "SharedLogCache was created with page size " + cachePageSize +
                            " and then requested with page size " + pageSize + "." +
                            EnvironmentConfig.LOG_CACHE_PAGE_SIZE + " is set manually."
            );
        }
    }

    /**
     * Writes byte iterator to the log returning its length.
     *
     * @param writer   a writer
     * @param iterable byte iterable to write.
     */
    private static void writeByteIterable(BufferedDataWriter writer, ByteIterable iterable) {
        var length = iterable.getLength();

        if (iterable instanceof ArrayByteIterable arrayByteIterable) {
            var bytes = arrayByteIterable.getBaseBytes();
            var offset = iterable.baseOffset();

            if (length == 1) {
                writer.write(bytes[0]);
            } else {
                writer.write(bytes, offset, length);
            }
        } else if (length >= 3) {
            var bytes = iterable.getBaseBytes();
            var offset = iterable.baseOffset();

            writer.write(bytes, offset, length);
        } else {
            var iterator = iterable.iterator();
            writer.write(iterator.next());
            if (length == 2) {
                writer.write(iterator.next());
            }
        }
    }


    private Semaphore getSharedWriteBoundarySemaphore(int writeBoundary) {
        var result = sharedWriteBoundarySemaphore;

        if (result == null) {
            sharedResourcesLock.lock();
            try {
                sharedWriteBoundarySemaphore = new Semaphore(writeBoundary);
                result = sharedWriteBoundarySemaphore;
            } finally {
                sharedResourcesLock.unlock();
            }
        }

        return result;
    }

    @Override
    public int getIdentity() {
        return identity;
    }

    @Override
    public byte[] readPage(long pageAddress, long fileAddress) {
        return writer.readPage(pageAddress);
    }

    private long beforeWrite() {
        var result = writer.getCurrentHighAddress();

        // begin of test-only code
        var testConfig = this.testConfigTestsOnly;
        if (testConfig != null) {
            var maxHighAddress = testConfig.getMaxHighAddress();
            if (maxHighAddress >= 0 && maxHighAddress <= result) {
                throw new ExodusException("Can't write more than $maxHighAddress");
            }
        }
        // end of test-only code

        writer.openNewFileIfNeeded(fileLengthBound, this);
        return result;
    }

    void doPadWithNulls(ExpiredLoggableCollection expiredLoggables) {
        var address = writer.getCurrentHighAddress();
        var written = writer.padWithNulls(fileLengthBound, nullPage);

        if (written > 0) {
            expiredLoggables.add(address, written);
            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
        }
    }

    public void padPageWithNulls(ExpiredLoggableCollection expiredLoggables) {
        beginWrite();
        try {
            var currentAddress = writer.getCurrentHighAddress();
            var written = writer.padPageWithNulls();

            if (written > 0) {
                expiredLoggables.add(currentAddress, written);
            }

            writer.closeFileIfNecessary(fileLengthBound, config.isFullFileReadonly());
        } finally {
            endWrite();
        }
    }

    public long getDiskUsage() {
        var allFiles = writer.allFiles();
        var highAddress = writer.getHighAddress();

        var filesCount = allFiles.length;

        if (filesCount == 0) {
            return 0L;
        }

        return (filesCount - 1) * fileLengthBound + getFileSize(
                allFiles[filesCount - 1],
                highAddress
        );
    }

    public void clearCacheTestsOnly() {
        cache.clear();
    }

    public void setLogTestConfigTestOnly(LogTestConfig testConfig) {
        this.testConfigTestsOnly = testConfig;
    }

    public static IdGenerator getIdentityGenerator() {
        return identityGenerator;
    }

    public static void invalidateSharedCacheTestsOnly() {
        synchronized (Log.class) {
            sharedCache = null;
        }
    }
}
