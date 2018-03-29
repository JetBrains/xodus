/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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

import jetbrains.exodus.*;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.InvalidCipherParametersException;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IdGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"JavaDoc"})
public final class Log implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Log.class);

    private static IdGenerator identityGenerator = new IdGenerator();
    private static volatile LogCache sharedCache = null;

    @NotNull
    private final LogConfig config;
    private final long created;
    @NotNull
    private final String location;
    final LogCache cache;

    private volatile boolean isClosing;

    private int logIdentity;
    @NotNull
    private final DataWriter baseWriter;
    private final AtomicReference<LogTip> tip;
    @Nullable
    private BufferedDataWriter bufferedWriter;
    /**
     * Last ticks when the sync operation was performed.
     */
    private long lastSyncTicks;
    @NotNull
    private final DataReader reader;

    private final List<NewFileListener> newFileListeners;
    private final List<ReadBytesListener> readBytesListeners;
    private final List<RemoveFileListener> removeFileListeners;

    /**
     * Size of single page in log cache.
     */
    private final int cachePageSize;
    /**
     * Size of a single file of the log in kilobytes.
     */
    private final long fileSize;
    private final long fileLengthBound; // and in bytes

    @Nullable
    private LogTestConfig testConfig;

    @SuppressWarnings({"OverlyLongMethod", "ThisEscapedInObjectConstruction", "OverlyCoupledMethod"})
    public Log(@NotNull final LogConfig config) {
        this.config = config;
        baseWriter = config.getWriter();
        tryLock();
        created = System.currentTimeMillis();
        fileSize = config.getFileSize();
        cachePageSize = config.getCachePageSize();
        final long fileLength = fileSize * 1024;
        if (fileLength % cachePageSize != 0) {
            throw new InvalidSettingException("File size should be a multiple of cache page size.");
        }
        fileLengthBound = fileLength;
        final LogFileSet.Mutable fileSetMutable = new LogFileSet.Immutable(fileLength).beginWrite();
        reader = config.getReader();
        reader.setLog(this);
        location = reader.getLocation();

        checkLogConsistency(fileSetMutable);

        final LogFileSet.Immutable fileSetImmutable = fileSetMutable.endWrite();

        newFileListeners = new ArrayList<>(2);
        readBytesListeners = new ArrayList<>(2);
        removeFileListeners = new ArrayList<>(2);
        final long memoryUsage = config.getMemoryUsage();
        final boolean nonBlockingCache = config.isNonBlockingCache();
        if (memoryUsage != 0) {
            cache = config.isSharedCache() ?
                    getSharedCache(memoryUsage, cachePageSize, nonBlockingCache) :
                    new SeparateLogCache(memoryUsage, cachePageSize, nonBlockingCache);
        } else {
            final int memoryUsagePercentage = config.getMemoryUsagePercentage();
            cache = config.isSharedCache() ?
                    getSharedCache(memoryUsagePercentage, cachePageSize, nonBlockingCache) :
                    new SeparateLogCache(memoryUsagePercentage, cachePageSize, nonBlockingCache);
        }
        DeferredIO.getJobProcessor();
        isClosing = false;

        final Long lastFileAddress = fileSetMutable.getMaximum();
        updateLogIdentity();
        if (lastFileAddress == null) {
            tip = new AtomicReference<>(new LogTip(fileSize));
        } else {
            final long currentHighAddress = lastFileAddress + reader.getBlock(lastFileAddress).length();
            final long highPageAddress = getHighPageAddress(currentHighAddress);
            final byte[] highPageContent = new byte[cachePageSize];
            final LogTip tmpTip = new LogTip(highPageContent, highPageAddress, cachePageSize, currentHighAddress, currentHighAddress, fileSetImmutable);
            this.tip = new AtomicReference<>(tmpTip); // TODO: this is a hack to provide readBytes below with high address (for determining last file length)
            final int highPageSize = currentHighAddress == 0 ? 0 : readBytes(highPageContent, highPageAddress);
            final LogTip proposedTip = new LogTip(highPageContent, highPageAddress, highPageSize, currentHighAddress, currentHighAddress, fileSetImmutable);
            this.tip.set(proposedTip);
            // here we should check whether last loggable is written correctly
            final Iterator<RandomAccessLoggable> lastFileLoggables = new LoggableIterator(this, lastFileAddress);
            long approvedHighAddress = lastFileAddress;
            try {
                while (lastFileLoggables.hasNext()) {
                    final RandomAccessLoggable loggable = lastFileLoggables.next();
                    final int dataLength = NullLoggable.isNullLoggable(loggable) ? 0 : loggable.getDataLength();
                    if (dataLength > 0) {
                        // if not null loggable read all data to the end
                        final ByteIteratorWithAddress data = loggable.getData().iterator();
                        for (int i = 0; i < dataLength; ++i) {
                            if (!data.hasNext()) {
                                throw new ExodusException("Can't read loggable fully" + LogUtil.getWrongAddressErrorMessage(data.getAddress(), fileSize));
                            }
                            data.next();
                        }
                    }
                    approvedHighAddress = loggable.getAddress() + loggable.length();
                }
            } catch (ExodusException e) { // if an exception is thrown then last loggable wasn't read correctly
                logger.error("Exception on Log recovery. Approved high address = " + approvedHighAddress, e);
            }
            if (approvedHighAddress < lastFileAddress || approvedHighAddress > currentHighAddress) {
                close();
                throw new InvalidCipherParametersException();
            }
            this.tip.set(proposedTip.withApprovedAddress(approvedHighAddress));
        }
        sync();
    }

    private void checkLogConsistency(LogFileSet.Mutable fileSetMutable) {
        final Block[] blocks = reader.getBlocks();
        for (int i = 0; i < blocks.length; ++i) {
            final Block block = blocks[i];
            final long address = block.getAddress();
            final long blockLength = block.length();
            String clearLogReason = null;
            // if it is not the last file and its size is not as expected
            if (blockLength > fileLengthBound || (i < blocks.length - 1 && blockLength != fileLengthBound)) {
                clearLogReason = "Unexpected file length" + LogUtil.getWrongAddressErrorMessage(address, fileSize);
            }
            // if the file address is not a multiple of fileLengthBound
            if (clearLogReason == null && address != getFileAddress(address)) {
                if (!config.isClearInvalidLog()) {
                    throw new ExodusException("Unexpected file address " +
                            LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(address, fileSize));
                }
                clearLogReason = "Unexpected file address " +
                        LogUtil.getLogFilename(address) + LogUtil.getWrongAddressErrorMessage(address, fileSize);
            }
            if (clearLogReason != null) {
                if (!config.isClearInvalidLog()) {
                    throw new ExodusException(clearLogReason);
                }
                logger.error("Clearing log due to: " + clearLogReason);
                fileSetMutable.clear();
                reader.clear();
                break;
            }
            fileSetMutable.add(address);
        }
    }

    @NotNull
    public LogConfig getConfig() {
        return config;
    }

    public long getCreated() {
        return created;
    }

    @NotNull
    public String getLocation() {
        return location;
    }

    /**
     * @return size of single log file in kilobytes.
     */
    public long getFileSize() {
        return fileSize;
    }

    public long getFileLengthBound() {
        return fileLengthBound;
    }

    public long getNumberOfFiles() {
        return getTip().logFileSet.size();
    }

    /**
     * Returns addresses of log files from the newest to the oldest ones.
     *
     * @return array of file addresses.
     */
    public long[] getAllFileAddresses() {
        return getTip().logFileSet.getArray();
    }

    public long getHighAddress() {
        return getTip().highAddress;
    }

    @SuppressWarnings({"OverlyLongMethod"})
    public LogTip setHighAddress(final LogTip logTip, final long highAddress) {
        if (highAddress > logTip.highAddress) {
            throw new ExodusException("Only can decrease high address");
        }
        if (highAddress == logTip.highAddress) {
            if (bufferedWriter != null) {
                throw new IllegalStateException("Unexpected write in progress");
            }
            return logTip;
        }

        final LogFileSet.Mutable fileSetMutable = logTip.logFileSet.beginWrite();

        // begin of test-only code
        final LogTestConfig testConfig = this.testConfig;
        if (testConfig != null && testConfig.isSettingHighAddressDenied()) {
            throw new ExodusException("Setting high address is denied");
        }
        // end of test-only code

        // at first, remove all files which are higher than highAddress
        closeWriter();
        final LongArrayList blocksToDelete = new LongArrayList();
        long blockToTruncate = -1L;
        for (final long blockAddress : fileSetMutable.getArray()) {
            if (blockAddress <= highAddress) {
                blockToTruncate = blockAddress;
                break;
            }
            blocksToDelete.add(blockAddress);
        }

        // truncate log
        for (int i = 0; i < blocksToDelete.size(); ++i) {
            removeFile(blocksToDelete.get(i), RemoveBlockType.Delete, fileSetMutable);
        }
        if (blockToTruncate >= 0) {
            truncateFile(blockToTruncate, highAddress - blockToTruncate);
        }

        final LogTip updatedTip;
        if (fileSetMutable.isEmpty()) {
            updateLogIdentity();
            updatedTip = new LogTip(fileSize);
        } else {
            final long oldHighPageAddress = logTip.pageAddress;
            long approvedHighAddress = logTip.approvedHighAddress;
            if (highAddress < approvedHighAddress) {
                approvedHighAddress = highAddress;
            }
            final long highPageAddress = getHighPageAddress(highAddress);
            final LogFileSet.Immutable fileSetImmutable = fileSetMutable.endWrite();
            final int highPageSize = (int) (highAddress - highPageAddress);
            if (oldHighPageAddress == highPageAddress) {
                updatedTip = logTip.withResize(highPageSize, highAddress, approvedHighAddress, fileSetImmutable);
            } else {
                updateLogIdentity();
                final byte[] highPageContent = new byte[cachePageSize];
                if (highPageSize > 0 && readBytes(highPageContent, highPageAddress) < highPageSize) {
                    throw new ExodusException("Can't read expected high page bytes");
                }
                updatedTip = new LogTip(highPageContent, highPageAddress, highPageSize, highAddress, approvedHighAddress, fileSetImmutable);
            }
        }
        compareAndSetTip(logTip, updatedTip);
        this.bufferedWriter = null;
        return updatedTip;
    }

    private void closeWriter() {
        if (bufferedWriter != null) {
            throw new IllegalStateException("Unexpected write in progress");
        }
        baseWriter.close();
    }

    public LogTip getTip() {
        return tip.get();
    }

    public LogTip beginWrite() {
        BufferedDataWriter writer = new BufferedDataWriter(this, baseWriter, reader, getTip());
        this.bufferedWriter = writer;
        return writer.getStartingTip();
    }

    public void abortWrite() {
        this.bufferedWriter = null;
    }

    public long getWrittenHighAddress() {
        return ensureWriter().getHighAddress();
    }

    public LogTip endWrite() {
        final BufferedDataWriter writer = ensureWriter();
        final LogTip logTip = writer.getStartingTip();
        final LogTip updatedTip = writer.getUpdatedTip();
        compareAndSetTip(logTip, updatedTip);
        bufferedWriter = null;
        return updatedTip;
    }

    private void compareAndSetTip(final LogTip logTip, final LogTip updatedTip) {
        if (!tip.compareAndSet(logTip, updatedTip)) {
            throw new ExodusException("write start/finish mismatch");
        }
    }

    public long getLowAddress() {
        final Long result = getTip().logFileSet.getMinimum();
        return result == null ? Loggable.NULL_ADDRESS : result;
    }

    public long getFileAddress(final long address) {
        return address - address % fileLengthBound;
    }

    public long getHighFileAddress() {
        return getFileAddress(getHighAddress());
    }

    public long getNextFileAddress(final long fileAddress) {
        final LongIterator files = getTip().logFileSet.getFilesFrom(fileAddress);
        if (files.hasNext()) {
            final long result = files.nextLong();
            if (result != fileAddress) {
                throw new ExodusException("There is no file by address " + fileAddress);
            }
            if (files.hasNext()) {
                return files.nextLong();
            }
        }
        return Loggable.NULL_ADDRESS;
    }

    public boolean isLastFileAddress(final long address, final LogTip logTip) {
        return getFileAddress(address) == getFileAddress(logTip.highAddress);
    }

    public boolean isLastWrittenFileAddress(final long address) {
        return getFileAddress(address) == getWrittenHighAddress();
    }

    public boolean hasAddress(final long address) {
        final long fileAddress = getFileAddress(address);
        final LogTip logTip = getTip();
        final LongIterator files = logTip.logFileSet.getFilesFrom(fileAddress);
        if (!files.hasNext()) {
            return false;
        }
        final long leftBound = files.nextLong();
        return leftBound == fileAddress && leftBound + getFileSize(leftBound, logTip) > address;
    }

    public boolean hasAddressRange(final long from, final long to) {
        long fileAddress = getFileAddress(from);
        final LogTip logTip = getTip();
        final LongIterator files = logTip.logFileSet.getFilesFrom(fileAddress);
        do {
            if (!files.hasNext() || files.nextLong() != fileAddress) {
                return false;
            }
            fileAddress += getFileSize(fileAddress, logTip);
        } while (fileAddress > from && fileAddress <= to);
        return true;
    }

    public long getFileSize(final long fileAddress) {
        return getFileSize(fileAddress, getTip());
    }

    public long getFileSize(final long fileAddress, final LogTip logTip) {
        // readonly files (not last ones) all have the same size
        if (!isLastFileAddress(fileAddress, logTip)) {
            return fileLengthBound;
        }
        final long highAddress = logTip.highAddress;
        final long result = highAddress % fileLengthBound;
        if (result == 0 && highAddress != fileAddress) {
            return fileLengthBound;
        }
        return result;
    }

    byte[] getHighPage(long alignedAddress) {
        final LogTip tip = getTip();
        if (tip.pageAddress == alignedAddress && tip.count >= 0) {
            return tip.bytes;
        }
        return null;
    }

    public byte[] getCachedPage(final long pageAddress) {
        return cache.getPage(this, pageAddress);
    }

    public final int getCachePageSize() {
        return cachePageSize;
    }

    public float getCacheHitRate() {
        return cache == null ? 0 : cache.hitRate();
    }

    public void addNewFileListener(@NotNull final NewFileListener listener) {
        synchronized (newFileListeners) {
            newFileListeners.add(listener);
        }
    }

    public void addReadBytesListener(@NotNull final ReadBytesListener listener) {
        synchronized (readBytesListeners) {
            readBytesListeners.add(listener);
        }
    }

    /**
     * Reads a random access loggable by specified address in the log.
     *
     * @param address - location of a loggable in the log.
     * @return instance of a loggable.
     */
    @NotNull
    public RandomAccessLoggable read(final long address) {
        return read(readIteratorFrom(address), address);
    }

    @NotNull
    public RandomAccessLoggable read(final DataIterator it) {
        return read(it, it.getHighAddress());
    }

    public byte getWrittenLoggableType(final long address) {
        final byte writtenByte = ensureWriter().getByte(address);
        return (byte) (writtenByte ^ 0x80);
    }

    @NotNull
    public RandomAccessLoggable read(final DataIterator it, final long address) {
        final byte type = (byte) (it.next() ^ 0x80);
        if (NullLoggable.isNullLoggable(type)) {
            return new NullLoggable(address);
        }
        return read(type, it, address);
    }

    /**
     * Just like {@linkplain #read(DataIterator, long)} reads loggable which never can be a {@linkplain NullLoggable}.
     *
     * @return a loggable which is not{@linkplain NullLoggable}
     */
    @NotNull
    public RandomAccessLoggable readNotNull(final DataIterator it, final long address) {
        return read((byte) (it.next() ^ 0x80), it, address);
    }

    @NotNull
    private RandomAccessLoggable read(final byte type, final DataIterator it, final long address) {
        final int structureId = CompressedUnsignedLongByteIterable.getInt(it);
        final int dataLength = CompressedUnsignedLongByteIterable.getInt(it);
        final long dataAddress = it.getHighAddress();
        if (dataLength > 0 && it.availableInCurrentPage(dataLength)) {
            return new RandomAccessLoggableAndArrayByteIterable(
                    address, type, structureId, dataAddress, it.getCurrentPage(), it.getOffset(), dataLength);
        }
        final RandomAccessByteIterable data = new RandomAccessByteIterable(dataAddress, this);
        return new RandomAccessLoggableImpl(address, type, data, dataLength, structureId);
    }

    public LoggableIterator getLoggableIterator(final long startAddress) {
        return new LoggableIterator(this, startAddress);
    }

    /**
     * Writes a loggable to the end of the log
     * If padding is needed, it is performed, but loggable is not written
     *
     * @param loggable - loggable to write.
     * @return address where the loggable was placed.
     */
    public long tryWrite(final Loggable loggable) {
        return tryWrite(loggable.getType(), loggable.getStructureId(), loggable.getData());
    }

    public long tryWrite(final byte type, final int structureId, final ByteIterable data) {
        // allow new file creation only if new file starts loggable
        long result = writeContinuously(type, structureId, data);
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            padWithNulls();
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
    public long write(final Loggable loggable) {
        return write(loggable.getType(), loggable.getStructureId(), loggable.getData());
    }

    public long write(final byte type, final int structureId, final ByteIterable data) {
        // allow new file creation only if new file starts loggable
        long result = writeContinuously(type, structureId, data);
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            padWithNulls();
            result = writeContinuously(type, structureId, data);
            if (result < 0) {
                throw new TooBigLoggableException();
            }
        }
        return result;
    }

    /**
     * Returns the first loggable in the log of specified type.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    @Nullable
    public Loggable getFirstLoggableOfType(final int type) {
        final LogTip logTip = getTip();
        final LongIterator files = logTip.logFileSet.getFilesFrom(0);
        final long approvedHighAddress = logTip.approvedHighAddress;
        while (files.hasNext()) {
            final long fileAddress = files.nextLong();
            final Iterator<RandomAccessLoggable> it = getLoggableIterator(fileAddress);
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                if (loggable == null || loggable.getAddress() >= fileAddress + fileLengthBound) {
                    break;
                }
                if (loggable.getType() == type) {
                    return loggable;
                }
                if (loggable.getAddress() + loggable.length() == approvedHighAddress) {
                    break;
                }
            }
        }
        return null;
    }

    /**
     * Returns the last loggable in the log of specified type.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    @Nullable
    public Loggable getLastLoggableOfType(final int type) {
        Loggable result = null;
        final LogTip logTip = getTip();
        final long approvedHighAddress = logTip.approvedHighAddress;
        for (final long fileAddress : logTip.logFileSet.getArray()) {
            if (result != null) {
                break;
            }
            final Iterator<RandomAccessLoggable> it = getLoggableIterator(fileAddress);
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                if (loggable == null || loggable.getAddress() >= fileAddress + fileLengthBound) {
                    break;
                }
                if (loggable.getType() == type) {
                    result = loggable;
                }
                if (loggable.getAddress() + loggable.length() == approvedHighAddress) {
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Returns the last loggable in the log of specified type which address is less than beforeAddress.
     *
     * @param type type of loggable.
     * @return loggable or null if it doesn't exists.
     */
    public Loggable getLastLoggableOfTypeBefore(final int type, final long beforeAddress, final LogTip logTip) {
        Loggable result = null;
        for (final long fileAddress : logTip.logFileSet.getArray()) {
            if (result != null) {
                break;
            }
            if (fileAddress >= beforeAddress) {
                continue;
            }
            final Iterator<RandomAccessLoggable> it = getLoggableIterator(fileAddress);
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                if (loggable == null) {
                    break;
                }
                final long address = loggable.getAddress();
                if (address >= beforeAddress || address >= fileAddress + fileLengthBound) {
                    break;
                }
                if (loggable.getType() == type) {
                    result = loggable;
                }
            }
        }
        return result;
    }

    public boolean isImmutableFile(final long fileAddress) {
        return fileAddress + fileLengthBound <= getTip().approvedHighAddress;
    }

    public void flush() {
        flush(false);
    }

    public void flush(boolean forceSync) {
        final BufferedDataWriter writer = ensureWriter();
        writer.flush();
        if (forceSync || config.isDurableWrite()) {
            sync();
        }
    }

    public void sync() {
        if (!config.isFsyncSuppressed()) {
            baseWriter.sync();
            lastSyncTicks = System.currentTimeMillis();
        }
    }

    @Override
    public void close() {
        final LogTip logTip = getTip();
        isClosing = true;
        sync();
        reader.close();
        closeWriter();
        compareAndSetTip(logTip, new LogTip(fileSize, logTip.pageAddress, logTip.highAddress));
        release();
    }

    public boolean isClosing() {
        return isClosing;
    }

    public void release() {
        baseWriter.release();
    }

    public LogTip clear() {
        final LogTip logTip = getTip();
        closeWriter();
        cache.clear();
        reader.clear();
        final LogTip updatedTip = new LogTip(fileSize);
        compareAndSetTip(logTip, updatedTip);
        this.bufferedWriter = null;
        updateLogIdentity();
        return updatedTip;
    }

    // for tests only
    public void forgetFile(long address) {
        beginWrite();
        forgetFiles(new long[]{address});
        endWrite();
    }

    public void forgetFiles(long[] files) {
        LogFileSet.Mutable fileSetMutable = ensureWriter().getFileSetMutable();
        for (long file : files) {
            fileSetMutable.remove(file);
        }
    }

    // for tests only
    public void removeFile(final long address) {
        removeFile(address, RemoveBlockType.Delete, null);
    }

    public void removeFile(final long address, @NotNull final RemoveBlockType rbt) {
        removeFile(address, rbt, null);
    }

    public void removeFile(final long address, @NotNull final RemoveBlockType rbt, @Nullable final LogFileSet.Mutable logFileSetMutable) {
        final RemoveFileListener[] listeners;
        synchronized (removeFileListeners) {
            listeners = removeFileListeners.toArray(new RemoveFileListener[removeFileListeners.size()]);
        }
        for (final RemoveFileListener listener : listeners) {
            listener.beforeRemoveFile(address);
        }
        try {
            // remove physical file
            reader.removeBlock(address, rbt);
            // remove address of file of the list
            if (logFileSetMutable != null) {
                logFileSetMutable.remove(address);
            }
            // clear cache
            for (long offset = 0; offset < fileLengthBound; offset += cachePageSize) {
                cache.removePage(this, address + offset);
            }
        } finally {
            for (final RemoveFileListener listener : listeners) {
                listener.afterRemoveFile(address);
            }
        }
    }

    @NotNull
    public BufferedDataWriter ensureWriter() {
        final BufferedDataWriter writer = this.bufferedWriter;
        if (writer == null) {
            throw new ExodusException("write not in progress");
        }
        return writer;
    }

    private void truncateFile(final long address, final long length) {
        // truncate physical file
        reader.truncateBlock(address, length);
        // clear cache
        for (long offset = length - (length % cachePageSize); offset < fileLengthBound; offset += cachePageSize) {
            cache.removePage(this, address + offset);
        }
    }

    /**
     * Pad current file with null loggables. Null loggable takes only one byte in the log,
     * so each file of the log with arbitrary alignment can be padded with nulls.
     * Padding with nulls is automatically performed when a loggable to be written can't be
     * placed within the appendable file without overcome of the value of fileLengthBound.
     * This feature allows to guarantee that each file starts with a new loggable, no
     * loggable can begin in one file and end in another. Also, this simplifies reading algorithm:
     * if we started reading by address it definitely should finish within current file.
     */
    void padWithNulls() {
        final BufferedDataWriter writer = ensureWriter();
        long bytesToWrite = fileLengthBound - writer.getLastWrittenFileLength(fileLengthBound);
        if (bytesToWrite == 0L) {
            throw new ExodusException("Nothing to pad");
        }
        if (bytesToWrite >= cachePageSize) {
            final byte[] cachedTailPage = LogCache.getCachedTailPage(cachePageSize);
            if (cachedTailPage != null) {
                do {
                    writer.write(cachedTailPage, cachePageSize);
                    bytesToWrite -= cachePageSize;
                    writer.incHighAddress(cachePageSize);
                } while (bytesToWrite >= cachePageSize);
            }
        }
        if (bytesToWrite == 0) {
            writer.commit();
            closeFullFileFileIfNecessary(writer);
        } else {
            while (bytesToWrite-- > 0) {
                writeContinuously(NullLoggable.create());
            }
        }
    }

    /**
     * For tests only!!!
     */
    public static void invalidateSharedCache() {
        synchronized (Log.class) {
            sharedCache = null;
        }
    }

    public int getIdentity() {
        return logIdentity;
    }

    @SuppressWarnings("ConstantConditions")
    int readBytes(final byte[] output, final long address) {
        final long fileAddress = getFileAddress(address);
        final LogTip logTip = getTip();
        final LongIterator files = logTip.logFileSet.getFilesFrom(fileAddress);
        if (files.hasNext()) {
            final long leftBound = files.nextLong();
            final long fileSize = getFileSize(leftBound, logTip);
            if (leftBound == fileAddress && fileAddress + fileSize > address) {
                final Block block = reader.getBlock(fileAddress);
                final int readBytes = block.read(output, address - fileAddress, output.length);
                final StreamCipherProvider cipherProvider = config.getCipherProvider();
                if (cipherProvider != null) {
                    EnvKryptKt.cryptBlocksMutable(cipherProvider, config.getCipherKey(), config.getCipherBasicIV(),
                            address, output, 0, readBytes, LogUtil.LOG_BLOCK_ALIGNMENT);
                }
                notifyReadBytes(output, readBytes);
                return readBytes;
            }
            if (fileAddress < logTip.logFileSet.getMinimum()) {
                BlockNotFoundException.raise("Address is out of log space, underflow", this, address);
            }
            if (fileAddress >= logTip.logFileSet.getMaximum()) {
                BlockNotFoundException.raise("Address is out of log space, overflow", this, address);
            }
        }
        BlockNotFoundException.raise(this, address);
        return 0;
    }

    /**
     * Returns iterator which reads raw bytes of the log starting from specified address.
     *
     * @param address
     * @return instance of ByteIterator
     */
    DataIterator readIteratorFrom(final long address) {
        return new DataIterator(this, address);
    }

    @NotNull
    private static LogCache getSharedCache(final long memoryUsage, final int pageSize, final boolean nonBlocking) {
        LogCache result = sharedCache;
        if (result == null) {
            synchronized (Log.class) {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(memoryUsage, pageSize, nonBlocking);
                }
                result = sharedCache;
            }
        }
        checkCachePageSize(pageSize, result);
        return result;
    }

    @NotNull
    private static LogCache getSharedCache(final int memoryUsagePercentage, final int pageSize, final boolean nonBlocking) {
        LogCache result = sharedCache;
        if (result == null) {
            synchronized (Log.class) {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(memoryUsagePercentage, pageSize, nonBlocking);
                }
                result = sharedCache;
            }
        }
        checkCachePageSize(pageSize, result);
        return result;
    }

    private static void checkCachePageSize(final int pageSize, @NotNull final LogCache result) {
        if (result.pageSize != pageSize) {
            throw new ExodusException("SharedLogCache was created with page size " + result.pageSize +
                    " and then requested with page size " + pageSize + ". EnvironmentConfig.LOG_CACHE_PAGE_SIZE was set manually.");
        }
    }

    private void tryLock() {
        final long lockTimeout = config.getLockTimeout();
        if (!baseWriter.lock(lockTimeout)) {
            throw new ExodusException("Can't acquire environment lock after " +
                    lockTimeout + " ms.\n\n Lock owner info: \n" + baseWriter.lockInfo());
        }
    }

    long getHighPageAddress(final long highAddress) {
        int alignment = ((int) highAddress) & (cachePageSize - 1);
        if (alignment == 0 && highAddress > 0) {
            alignment = cachePageSize;
        }
        return highAddress - alignment; // aligned address
    }

    public LongIterator getFilesFrom(LogTip logTip, Long fileAddress) {
        return logTip.logFileSet.getFilesFrom(fileAddress);
    }

    /**
     * Writes specified loggable continuously in a single file.
     *
     * @param loggable the loggable to write.
     * @return address where the loggable was placed or less than zero value if the loggable can't be
     * written continuously in current appendable file.
     */
    public long writeContinuously(final Loggable loggable) {
        return writeContinuously(loggable.getType(), loggable.getStructureId(), loggable.getData());
    }

    public long writeContinuously(final byte type, final int structureId, final ByteIterable data) {
        final BufferedDataWriter writer = ensureWriter();

        final long result = writer.getHighAddress();

        // begin of test-only code
        final LogTestConfig testConfig = this.testConfig;
        if (testConfig != null) {
            final long maxHighAddress = testConfig.getMaxHighAddress();
            if (maxHighAddress >= 0 && result >= maxHighAddress) {
                throw new ExodusException("Can't write more than " + maxHighAddress);
            }
        }
        // end of test-only code

        if (!baseWriter.isOpen()) {
            final long fileAddress = getFileAddress(result);
            writer.openOrCreateBlock(fileAddress, writer.getLastWrittenFileLength(fileLengthBound));
            final boolean fileCreated = !writer.getFileSetMutable().contains(fileAddress);
            if (fileCreated) {
                writer.getFileSetMutable().add(fileAddress);
            }
            if (fileCreated) {
                // fsync the directory to ensure we will find the log file in the directory after system crash
                baseWriter.syncDirectory();
                notifyFileCreated(fileAddress);
            }
        }

        final boolean isNull = NullLoggable.isNullLoggable(type);
        int recordLength = 1;
        if (isNull) {
            writer.write((byte) (type ^ 0x80));
        } else {
            final ByteIterable structureIdIterable = CompressedUnsignedLongByteIterable.getIterable(structureId);
            final int dataLength = data.getLength();
            final ByteIterable dataLengthIterable = CompressedUnsignedLongByteIterable.getIterable(dataLength);
            recordLength += structureIdIterable.getLength();
            recordLength += dataLengthIterable.getLength();
            recordLength += dataLength;
            if (recordLength > fileLengthBound - writer.getLastWrittenFileLength(fileLengthBound)) {
                return -1L;
            }
            writer.write((byte) (type ^ 0x80));
            writeByteIterable(writer, structureIdIterable);
            writeByteIterable(writer, dataLengthIterable);
            if (dataLength > 0) {
                writeByteIterable(writer, data);
            }

        }
        writer.commit();
        writer.incHighAddress(recordLength);
        closeFullFileFileIfNecessary(writer);
        return result;
    }

    private void closeFullFileFileIfNecessary(BufferedDataWriter writer) {
        final boolean shouldCreateNewFile = writer.getLastWrittenFileLength(fileLengthBound) == 0;
        if (shouldCreateNewFile) {
            // Don't forget to fsync the old file before closing it, otherwise will get a corrupted DB in the case of a
            // system failure:
            flush(true);

            baseWriter.close();
            if (config.isFullFileReadonly()) {
                final Long lastFile = writer.getFileSetMutable().getMaximum();
                if (lastFile != null) {
                    Block block = reader.getBlock(lastFile);
                    if (block.length() < fileSize) {
                        throw new IllegalStateException("file too short");
                    }
                    block.setReadOnly();
                }
            }
        } else if (System.currentTimeMillis() > lastSyncTicks + config.getSyncPeriod()) {
            flush(true);
        }
    }

    /**
     * Sets LogTestConfig.
     * Is destined for tests only, please don't set a not-null value in application code.
     */
    public void setLogTestConfig(@Nullable final LogTestConfig testConfig) {
        this.testConfig = testConfig;
    }

    private void notifyFileCreated(long fileAddress) {
        final NewFileListener[] listeners;
        synchronized (newFileListeners) {
            listeners = newFileListeners.toArray(new NewFileListener[newFileListeners.size()]);
        }
        for (final NewFileListener listener : listeners) {
            listener.fileCreated(fileAddress);
        }
    }

    private void notifyReadBytes(final byte[] bytes, final int count) {
        final ReadBytesListener[] listeners;
        synchronized (readBytesListeners) {
            listeners = this.readBytesListeners.toArray(new ReadBytesListener[this.readBytesListeners.size()]);
        }
        for (final ReadBytesListener listener : listeners) {
            listener.bytesRead(bytes, count);
        }
    }

    /**
     * Writes byte iterator to the log returning its length.
     *
     * @param writer   a writer
     * @param iterable byte iterable to write.
     * @return
     */
    private static void writeByteIterable(final BufferedDataWriter writer, final ByteIterable iterable) {
        final int length = iterable.getLength();
        if (iterable instanceof ArrayByteIterable) {
            final byte[] bytes = iterable.getBytesUnsafe();
            if (length == 1) {
                writer.write(bytes[0]);
            } else {
                writer.write(bytes, length);
            }
        } else if (length >= 3) {
            writer.write(iterable.getBytesUnsafe(), length);
        } else {
            final ByteIterator iterator = iterable.iterator();
            writer.write(iterator.next());
            if (length == 2) {
                writer.write(iterator.next());
            }
        }
    }

    private void updateLogIdentity() {
        logIdentity = identityGenerator.nextId();
    }
}
