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

import jetbrains.exodus.*;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.skiplists.LongSkipList;
import jetbrains.exodus.io.*;
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
    private final LongSkipList blockAddrs;
    final LogCache cache;

    private volatile boolean isClosing;

    private int logIdentity;
    @SuppressWarnings("NullableProblems")
    @NotNull
    private TransactionalDataWriter bufferedWriter;
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
    private long highAddress;
    private long approvedHighAddress; // high address approved on a higher layer (by Environment transactions)

    @Nullable
    private LogTestConfig testConfig;

    @SuppressWarnings({"OverlyLongMethod", "ThisEscapedInObjectConstruction", "OverlyCoupledMethod"})
    public Log(@NotNull final LogConfig config) {
        this.config = config;
        tryLock();
        created = System.currentTimeMillis();
        blockAddrs = new LongSkipList();
        fileSize = config.getFileSize();
        cachePageSize = config.getCachePageSize();
        final long fileLength = fileSize * 1024;
        if (fileLength % cachePageSize != 0) {
            throw new InvalidSettingException("File size should be a multiple of cache page size.");
        }
        fileLengthBound = fileLength;
        reader = config.getReader();
        reader.setLog(this);
        location = reader.getLocation();

        checkLogConsistency();

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
        highAddress = 0;
        approvedHighAddress = 0;

        final DataWriter baseWriter = config.getWriter();
        final LongSkipList.SkipListNode lastFile = blockAddrs.getMaximumNode();
        if (lastFile == null) {
            setBufferedWriter(createEmptyBufferedWriter(baseWriter));
        } else {
            final long lastFileAddress = lastFile.getKey();
            highAddress = lastFileAddress + reader.getBlock(lastFileAddress).length();
            final long highPageAddress = getHighPageAddress();
            final byte[] highPageContent = new byte[cachePageSize];
            setBufferedWriter(createBufferedWriter(baseWriter, highPageAddress,
                highPageContent, highAddress == 0 ? 0 : readBytes(highPageContent, highPageAddress)));
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
            setHighAddress(approvedHighAddress);
            this.approvedHighAddress = approvedHighAddress;
        }
        flush(true);
    }

    private void checkLogConsistency() {
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
                blockAddrs.clear();
                reader.clear();
                break;
            }
            blockAddrs.add(address);
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
        return blockAddrs.size();
    }

    /**
     * Returns addresses of log files from the newest to the oldest ones.
     *
     * @return array of file addresses.
     */
    public long[] getAllFileAddresses() {
        synchronized (blockAddrs) {
            long[] result = new long[blockAddrs.size()];
            LongSkipList.SkipListNode node = blockAddrs.getMaximumNode();
            //noinspection ForLoopThatDoesntUseLoopVariable
            for (int i = 0; node != null; ++i) {
                result[i] = node.getKey();
                node = blockAddrs.getPrevious(node);
            }
            return result;
        }
    }

    public long getHighAddress() {
        return highAddress;
    }

    @SuppressWarnings({"OverlyLongMethod"})
    public void setHighAddress(final long highAddress) {
        if (highAddress > this.highAddress) {
            throw new ExodusException("Only can decrease high address");
        }
        if (highAddress == this.highAddress) {
            return;
        }

        // begin of test-only code
        final LogTestConfig testConfig = this.testConfig;
        if (testConfig != null && testConfig.isSettingHighAddressDenied()) {
            throw new ExodusException("Setting high address is denied");
        }
        // end of test-only code

        // at first, remove all files which are higher than highAddress
        bufferedWriter.close();
        final LongArrayList blocksToDelete = new LongArrayList();
        long blockToTruncate = -1L;
        synchronized (blockAddrs) {
            LongSkipList.SkipListNode node = blockAddrs.getMaximumNode();
            while (node != null) {
                long blockAddress = node.getKey();
                if (blockAddress <= highAddress) {
                    blockToTruncate = blockAddress;
                    break;
                }
                blocksToDelete.add(blockAddress);
                node = blockAddrs.getPrevious(node);
            }
        }

        // truncate log
        for (int i = 0; i < blocksToDelete.size(); ++i) {
            removeFile(blocksToDelete.get(i));
        }
        if (blockToTruncate >= 0) {
            truncateFile(blockToTruncate, highAddress - blockToTruncate);
        }

        // update buffered writer
        final DataWriter baseWriter = config.getWriter();
        if (blockAddrs.isEmpty()) {
            this.highAddress = 0;
            setBufferedWriter(createEmptyBufferedWriter(baseWriter));
        } else {
            final long oldHighPageAddress = getHighPageAddress();
            this.highAddress = highAddress;
            final long highPageAddress = getHighPageAddress();
            if (oldHighPageAddress != highPageAddress || !bufferedWriter.tryAndUpdateHighAddress(highAddress)) {
                final int highPageSize = (int) (highAddress - highPageAddress);
                final byte[] highPageContent = new byte[cachePageSize];
                if (highPageSize > 0 && readBytes(highPageContent, highPageAddress) < highPageSize) {
                    throw new ExodusException("Can't read expected high page bytes");
                }
                setBufferedWriter(createBufferedWriter(baseWriter, highPageAddress, highPageContent, highPageSize));
            }
        }
    }

    public long approveHighAddress() {
        return approvedHighAddress = highAddress;
    }

    public long getLowAddress() {
        final Long result = blockAddrs.getMinimum();
        return result == null ? Loggable.NULL_ADDRESS : result;
    }

    public long getFileAddress(final long address) {
        return address - address % fileLengthBound;
    }

    public long getHighFileAddress() {
        return getFileAddress(highAddress);
    }

    public long getNextFileAddress(final long fileAddress) {
        LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.search(fileAddress);
        }
        if (node == null) {
            throw new ExodusException("There is no file by address " + fileAddress);
        }
        node = blockAddrs.getNext(node);
        return node == null ? Loggable.NULL_ADDRESS : node.getKey();
    }

    public boolean isLastFileAddress(final long address) {
        return getFileAddress(address) == getHighFileAddress();
    }

    public boolean hasAddress(final long address) {
        final LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getLessOrEqual(address);
        }
        if (node == null) {
            return false;
        }
        final long leftBound = node.getKey();
        return leftBound + getFileSize(leftBound) > address;
    }

    public boolean hasAddressRange(final long from, final long to) {
        LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getLessOrEqual(from);
        }
        if (node == null) {
            return false;
        }
        while (true) {
            final long leftBound = node.getKey();
            if (leftBound + getFileSize(leftBound) > to) {
                return true;
            }
            node = blockAddrs.getNext(node);
            if (node == null || node.getKey() - fileLengthBound > leftBound) {
                return false;
            }
        }
    }

    public long getFileSize(long fileAddress) {
        // readonly files (not last ones) all have the same size
        if (!isLastFileAddress(fileAddress)) {
            return fileLengthBound;
        }
        final long highAddress = this.highAddress;
        final long result = highAddress % fileLengthBound;
        if (result == 0 && highAddress != fileAddress) {
            return fileLengthBound;
        }
        return result;
    }

    byte[] getHighPage(long alignedAddress) {
        return bufferedWriter.getHighPage(alignedAddress);
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
        LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getMinimumNode();
        }
        while (node != null) {
            final long fileAddress = node.getKey();
            final Iterator<RandomAccessLoggable> it = getLoggableIterator(fileAddress);
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                if (loggable.getAddress() >= fileAddress + fileLengthBound) {
                    break;
                }
                if (loggable.getType() == type) {
                    return loggable;
                }
            }
            node = blockAddrs.getNext(node);
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
        LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getMaximumNode();
        }
        Loggable result = null;
        if (node != null) {
            // last file can be empty due to recovery procedure
            if (getFileSize(node.getKey()) == 0) {
                node = blockAddrs.getPrevious(node);
            }
            while (result == null && node != null) {
                final long fileAddress = node.getKey();
                final Iterator<RandomAccessLoggable> it = getLoggableIterator(fileAddress);
                while (it.hasNext()) {
                    final Loggable loggable = it.next();
                    if (loggable.getAddress() >= fileAddress + fileLengthBound) {
                        break;
                    }
                    if (loggable.getType() == type) {
                        result = loggable;
                    }
                }
                node = blockAddrs.getPrevious(node);
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
    public Loggable getLastLoggableOfTypeBefore(final int type, final long beforeAddress) {
        LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getLessOrEqual(beforeAddress);
        }
        Loggable result = null;
        while (result == null && node != null) {
            final Iterator<RandomAccessLoggable> it = getLoggableIterator(node.getKey());
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                if (loggable.getAddress() >= beforeAddress) {
                    break;
                }
                if (loggable.getType() == type) {
                    result = loggable;
                }
            }
            node = blockAddrs.getPrevious(node);
        }
        return result;
    }

    public boolean isImmutableFile(final long fileAddress) {
        return fileAddress + fileLengthBound <= approvedHighAddress;
    }

    public void flush() {
        flush(false);
    }

    public void flush(boolean forceSync) {
        final TransactionalDataWriter bufferedWriter = this.bufferedWriter;
        bufferedWriter.flush();
        if ((forceSync || config.isDurableWrite()) && !config.isFsyncSuppressed()) {
            bufferedWriter.sync();
            lastSyncTicks = System.currentTimeMillis();
        }
    }

    @Override
    public void close() {
        isClosing = true;
        flush(true);
        reader.close();
        bufferedWriter.close();
        release();
        synchronized (blockAddrs) {
            blockAddrs.clear();
        }
    }

    public boolean isClosing() {
        return isClosing;
    }

    public void release() {
        bufferedWriter.release();
    }

    public void clear() {
        bufferedWriter.close();
        synchronized (blockAddrs) {
            blockAddrs.clear();
        }
        cache.clear();
        reader.clear();
        setBufferedWriter(createEmptyBufferedWriter(bufferedWriter.getChildWriter()));
        highAddress = 0;
    }

    public void removeFile(final long address) {
        removeFile(address, RemoveBlockType.Delete);
    }

    public void removeFile(final long address, @NotNull final RemoveBlockType rbt) {
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
            synchronized (blockAddrs) {
                blockAddrs.remove(address);
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
        long bytesToWrite = fileLengthBound - getLastFileLength();
        if (bytesToWrite == 0L) {
            throw new ExodusException("Nothing to pad");
        }
        if (bytesToWrite >= cachePageSize) {
            final byte[] cachedTailPage = LogCache.getCachedTailPage(cachePageSize);
            if (cachedTailPage != null) {
                do {
                    bufferedWriter.write(cachedTailPage, 0, cachePageSize);
                    bytesToWrite -= cachePageSize;
                    highAddress += cachePageSize;
                } while (bytesToWrite >= cachePageSize);
            }
        }
        if (bytesToWrite == 0) {
            bufferedWriter.commit();
            createNewFileIfNecessary();
        } else {
            while (bytesToWrite-- > 0) {
                writeContinuously(NullLoggable.create());
            }
        }
    }

    /**
     * For tests only!!!
     */
    public static synchronized void invalidateSharedCache() {
        synchronized (Log.class) {
            sharedCache = null;
        }
    }

    public int getIdentity() {
        return logIdentity;
    }

    int readBytes(final byte[] output, final long address) {
        final LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getLessOrEqual(address);
        }
        if (node == null) {
            BlockNotFoundException.raise("Address is out of log space, underflow", this, address);
        }
        final long leftBound = node.getKey();
        final Block block = reader.getBlock(leftBound);
        final long fileSize = getFileSize(leftBound);
        if (leftBound + fileSize <= address) {
            if (blockAddrs.getMaximumNode() == node) {
                BlockNotFoundException.raise("Address is out of log space, overflow", this, address);
            }
            BlockNotFoundException.raise(this, address);
        }
        final int readBytes = block.read(output, address - leftBound, output.length);
        notifyReadBytes(output, readBytes);
        return readBytes;
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
        final DataWriter writer = config.getWriter();
        if (!writer.lock(lockTimeout)) {
            throw new ExodusException("Can't acquire environment lock after " +
                lockTimeout + " ms.\n\n Lock owner info: \n" + writer.lockInfo());
        }
    }

    @NotNull
    private TransactionalDataWriter createEmptyBufferedWriter(final DataWriter writer) {
        notifyFileCreated(0);
        //return new BufferedDataWriter(cachePageSize, config.getWriteBufferSize(), writer);
        return new BufferedDataWriter(this, writer);
    }

    private TransactionalDataWriter createBufferedWriter(final DataWriter writer,
                                                         final long highPageAddress,
                                                         final byte[] highPageContent,
                                                         final int highPageSize) {
        //return new BufferedDataWriter(cachePageSize, config.getWriteBufferSize(), writer, highPageAddress, highPageContent, highPageSize);
        return new BufferedDataWriter(this, writer, highPageAddress, highPageContent, highPageSize);
    }

    private long getHighPageAddress() {
        final long highAddress = this.highAddress;
        int alignment = ((int) highAddress) & (cachePageSize - 1);
        if (alignment == 0 && highAddress > 0) {
            alignment = cachePageSize;
        }
        return highAddress - alignment; // aligned address
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
        final long result = highAddress;

        // begin of test-only code
        final LogTestConfig testConfig = this.testConfig;
        if (testConfig != null) {
            final long maxHighAddress = testConfig.getMaxHighAddress();
            if (maxHighAddress >= 0 && result >= maxHighAddress) {
                throw new ExodusException("Can't write more than " + maxHighAddress);
            }
        }
        // end of test-only code

        final TransactionalDataWriter bufferedWriter = this.bufferedWriter;
        if (!bufferedWriter.isOpen()) {
            final long fileAddress = getFileAddress(result);
            bufferedWriter.openOrCreateBlock(fileAddress, getLastFileLength());
            final boolean fileCreated;
            synchronized (blockAddrs) {
                fileCreated = blockAddrs.search(fileAddress) == null;
                if (fileCreated) {
                    blockAddrs.add(fileAddress);
                }
            }
            if (fileCreated) {
                // fsync the directory to ensure we will find the log file in the directory after system crash
                bufferedWriter.syncDirectory();
                notifyFileCreated(fileAddress);
            }
        }

        final boolean isNull = NullLoggable.isNullLoggable(type);
        int recordLength = 1;
        if (isNull) {
            bufferedWriter.write((byte) (type ^ 0x80));
        } else {
            final ByteIterable structureIdIterable = CompressedUnsignedLongByteIterable.getIterable(structureId);
            final int dataLength = data.getLength();
            final ByteIterable dataLengthIterable = CompressedUnsignedLongByteIterable.getIterable(dataLength);
            recordLength += structureIdIterable.getLength();
            recordLength += dataLengthIterable.getLength();
            recordLength += dataLength;
            if (recordLength > fileLengthBound - getLastFileLength()) {
                return -1L;
            }
            bufferedWriter.write((byte) (type ^ 0x80));
            writeByteIterable(bufferedWriter, structureIdIterable);
            writeByteIterable(bufferedWriter, dataLengthIterable);
            if (dataLength > 0) {
                writeByteIterable(bufferedWriter, data);
            }

        }
        bufferedWriter.commit();
        highAddress += recordLength;
        createNewFileIfNecessary();
        return result;
    }

    private void createNewFileIfNecessary() {
        final boolean shouldCreateNewFile = getLastFileLength() == 0;
        if (shouldCreateNewFile) {
            // Don't forget to fsync the old file before closing it, otherwise will get a corrupted DB in the case of a
            // system failure:
            flush(true);

            bufferedWriter.close();
            if (config.isFullFileReadonly()) {
                final Long lastFile;
                synchronized (blockAddrs) {
                    lastFile = blockAddrs.getMaximum();
                }
                if (lastFile != null) {
                    reader.getBlock(lastFile).setReadOnly();
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
    private static void writeByteIterable(final TransactionalDataWriter writer, final ByteIterable iterable) {
        final int length = iterable.getLength();
        if (iterable instanceof ArrayByteIterable) {
            final byte[] bytes = iterable.getBytesUnsafe();
            if (length == 1) {
                writer.write(bytes[0]);
            } else {
                writer.write(bytes, 0, length);
            }
        } else if (length >= 3) {
            writer.write(iterable.getBytesUnsafe(), 0, length);
        } else {
            final ByteIterator iterator = iterable.iterator();
            writer.write(iterator.next());
            if (length == 2) {
                writer.write(iterator.next());
            }
        }
    }

    private long getLastFileLength() {
        return highAddress % fileLengthBound;
    }

    private void setBufferedWriter(@NotNull final TransactionalDataWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
        logIdentity = identityGenerator.nextId();
    }
}
