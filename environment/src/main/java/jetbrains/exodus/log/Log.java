/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.util.DeferredIO;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"JavaDoc"})
public final class Log implements Closeable {

    private static final org.apache.commons.logging.Log logging = LogFactory.getLog(Log.class);

    private static final int LOGGABLE_CACHE_GENERATIONS = 4; // number of generations of loggable cache
    private static final int LOGGABLE_CACHE_SIZE = 2 << 10; // must be a power of 2

    private static AtomicInteger identityGenerator = new AtomicInteger();

    private static LogCache sharedCache = null;

    @NotNull
    private final LogConfig config;
    private final long created;
    @NotNull
    private final String location;
    private final LongSkipList blockAddrs;
    private final RandomAccessLoggable[] loggableCache;
    final LogCache cache;

    private int logIdentity;
    @NotNull
    private TransactionalDataWriter bufferedWriter;
    /**
     * Last ticks when the sync operation was performed.
     */
    private long lastSyncTicks;
    @NotNull
    private final DataReader reader;

    private final List<NewFileListener> newFileListeners;

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

    @SuppressWarnings({"OverlyLongMethod", "ThisEscapedInObjectConstruction", "OverlyCoupledMethod"})
    public Log(@NotNull final LogConfig config) {
        this.config = config;
        tryLock();
        created = System.currentTimeMillis();
        blockAddrs = new LongSkipList();
        loggableCache = new RandomAccessLoggable[LOGGABLE_CACHE_SIZE * LOGGABLE_CACHE_GENERATIONS];
        clearLoggableCache();
        fileSize = config.getFileSize();
        cachePageSize = config.getCachePageSize();
        final long fileLength = fileSize * 1024;
        if (fileLength % cachePageSize != 0) {
            throw new InvalidSettingException("File size should be a multiple of cache page size.");
        }
        fileLengthBound = fileLength;
        reader = config.getReader();
        location = reader.getLocation();
        final Block[] blocks = reader.getBlocks();
        for (int i = 0; i < blocks.length; ++i) {
            final Block block = blocks[i];
            final long address = block.getAddress();
            blockAddrs.add(address);
            // if it is not the last file and its size is not as expected
            final long blockLength = block.length();
            if (blockLength > fileLength || (i < blocks.length - 1 && blockLength != fileLength)) {
                if (config.isClearInvalidLog()) {
                    blockAddrs.clear();
                    reader.clear();
                    break;
                } else {
                    throw new ExodusException("Unexpected file length, address = " + address);
                }
            }
        }
        newFileListeners = new ArrayList<NewFileListener>(2);
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
        highAddress = 0;

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
                                throw new ExodusException("Can't read loggable fully, address = " + data.getAddress());
                            }
                            data.next();
                        }
                    }
                    approvedHighAddress = loggable.getAddress() + loggable.length();
                }
            } catch (ExodusException e) { // if an exception is thrown then last loggable wasn't read correctly
                logging.error("Exception on Log recovery. Approved high address = " + approvedHighAddress, e);
            }
            cache.clearRecentHits();
            setHighAddress(approvedHighAddress);
        }
        flush(true);
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
        // at first, remove all files which are higher than highAddress
        bufferedWriter.close();
        final LongArrayList blocksToDelete = new LongArrayList();
        synchronized (blockAddrs) {
            LongSkipList.SkipListNode node = blockAddrs.getMaximumNode();
            while (node != null) {
                long blockAddress = node.getKey();
                if (blockAddress <= highAddress) {
                    break;
                }
                blocksToDelete.add(blockAddress);
                node = blockAddrs.getPrevious(node);
            }
        }
        for (int i = 0; i < blocksToDelete.size(); ++i) {
            removeFile(blocksToDelete.get(i));
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
                for (long pageAddress = highPageAddress; pageAddress < oldHighPageAddress; pageAddress += cachePageSize) {
                    cache.removePage(this, pageAddress);
                }
                setBufferedWriter(createBufferedWriter(baseWriter, highPageAddress, highPageContent, highPageSize));
            }
        }
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

    ArrayByteIterable getHighPage(long alignedAddress) {
        return bufferedWriter.getHighPage(alignedAddress);
    }

    public final int getCachePageSize() {
        return cachePageSize;
    }

    public double getCacheHitRate() {
        return cache == null ? 0 : cache.hitRate();
    }

    public void addNewFileListener(@NotNull final NewFileListener listener) {
        synchronized (newFileListeners) {
            newFileListeners.add(listener);
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
        int cacheIndex = ((int) (address & (LOGGABLE_CACHE_SIZE - 1))) * LOGGABLE_CACHE_GENERATIONS;
        // at first check the highest (oldest) generation
        RandomAccessLoggable result = loggableCache[cacheIndex];
        if (result.getAddress() == address) {
            return result;
        }
        for (int i = 1; i < LOGGABLE_CACHE_GENERATIONS; ++i) {
            result = loggableCache[++cacheIndex];
            if (result.getAddress() == address) {
                final RandomAccessLoggable temp = loggableCache[cacheIndex - 1];
                loggableCache[cacheIndex - 1] = result;
                loggableCache[cacheIndex] = temp;
                return result;
            }
        }
        // put new loggable to probation generation
        return loggableCache[cacheIndex] = read(readIteratorFrom(address), address);
    }

    @NotNull
    public RandomAccessLoggable read(final DataIterator it) {
        return read(it, it.getHighAddress());
    }

    @NotNull
    public RandomAccessLoggable read(final DataIterator it, final long address) {
        return LoggableFactory.create(this, it, address);
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
        // allow new file creation only if new file starts loggable
        long result = writeContinuously(loggable);
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
        // allow new file creation only if new file starts loggable
        long result = writeContinuously(loggable);
        if (result < 0) {
            // rollback loggable and pad last file with nulls
            padWithNulls();
            result = writeContinuously(loggable);
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

    public void flush() {
        flush(false);
    }

    public void flush(boolean forceSync) {
        final TransactionalDataWriter bufferedWriter = this.bufferedWriter;
        bufferedWriter.flush();
        if (forceSync || config.isDurableWrite()) {
            bufferedWriter.sync();
            lastSyncTicks = System.currentTimeMillis();
        }
    }

    @Override
    public void close() {
        flush(true);
        reader.close();
        bufferedWriter.close();
        bufferedWriter.release();
        synchronized (blockAddrs) {
            blockAddrs.clear();
        }
    }

    public void clear() {
        bufferedWriter.close();
        synchronized (blockAddrs) {
            blockAddrs.clear();
        }
        clearLoggableCache();
        cache.clear();
        reader.clear();
        setBufferedWriter(createEmptyBufferedWriter(bufferedWriter.getChildWriter()));
        highAddress = 0;
    }

    public void removeFile(final long address) {
        removeFile(address, RemoveBlockType.Delete);
    }

    public void removeFile(final long address, @NotNull final RemoveBlockType rbt) {
        // force fsync in order to fix XD-249
        // in order to avoid data loss , it's necessary to make sure that any GC transaction is flushed
        // to underlying physical storage before any file is deleted
        bufferedWriter.sync();
        //remove physical file
        reader.removeBlock(address, rbt);
        // remove address of file of the list
        synchronized (blockAddrs) {
            if (!blockAddrs.remove(address)) {
                throw new ExodusException("There is no file by address " + address);
            }
        }
        // clear cache
        for (long offset = 0; offset < fileLengthBound; offset += cachePageSize) {
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
    public void padWithNulls() {
        while (getLastFileLength() != 0) {
            write(NullLoggable.create());
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

    int getIdentity() {
        return logIdentity;
    }

    int readBytes(final byte[] output, final long address) throws BlockNotFoundException {
        final LongSkipList.SkipListNode node;
        synchronized (blockAddrs) {
            node = blockAddrs.getLessOrEqual(address);
        }
        if (node == null) {
            throw new BlockNotFoundException("Address is out of log space, underflow", address);
        }
        final long leftBound = node.getKey();
        final Block block = reader.getBlock(leftBound);
        final long fileSize = getFileSize(leftBound);
        if (leftBound + fileSize <= address) {
            if (blockAddrs.getMaximumNode() == node) {
                throw new BlockNotFoundException("Address is out of log space, overflow", address);
            }
            throw new BlockNotFoundException(address);
        }
        return block.read(output, address - leftBound, output.length);
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

    private static LogCache getSharedCache(final long memoryUsage, final int pageSize, final boolean nonBlocking) {
        if (sharedCache == null) {
            synchronized (Log.class) {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(memoryUsage, pageSize, nonBlocking);
                }
            }
        }
        return sharedCache;
    }

    private static LogCache getSharedCache(final int memoryUsagePercentage, final int pageSize, final boolean nonBlocking) {
        if (sharedCache == null) {
            synchronized (Log.class) {
                if (sharedCache == null) {
                    sharedCache = new SharedLogCache(memoryUsagePercentage, pageSize, nonBlocking);
                }
            }
        }
        return sharedCache;
    }

    private void tryLock() {
        final long lockTimeout = config.getLockTimeout();
        if (!config.getWriter().lock(lockTimeout)) {
            throw new ExodusException("Can't acquire environment lock after " + lockTimeout + " ms.");
        }
    }

    private void clearLoggableCache() {
        final NullLoggable nullLoggable = new NullLoggable(Loggable.NULL_ADDRESS);
        for (int i = 0; i < loggableCache.length; i++) {
            loggableCache[i] = nullLoggable;
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
        int alignment = (int) (highAddress % cachePageSize);
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
    @SuppressWarnings({"ThrowCaughtLocally", "OverlyLongMethod"})
    public long writeContinuously(final Loggable loggable) {
        final long result = highAddress;
        final TransactionalDataWriter bufferedWriter = this.bufferedWriter;
        if (!bufferedWriter.isOpen()) {
            final long fileAddress = getFileAddress(result);
            this.bufferedWriter.openOrCreateBlock(fileAddress, getLastFileLength());
            final boolean fileCreated;
            synchronized (blockAddrs) {
                fileCreated = blockAddrs.search(fileAddress) == null;
                if (fileCreated) {
                    blockAddrs.add(result);
                }
            }
            if (fileCreated) {
                notifyFileCreated(fileAddress);
            }
        }
        try {
            bufferedWriter.setMaxBytesToWrite((int) (fileLengthBound - getLastFileLength()));
            final byte type = loggable.getType();
            bufferedWriter.write((byte) (type ^ 0x80));
            int recordLength = 1;
            // NullLoggable doesn't contain data
            if (!NullLoggable.isNullLoggable(type)) {
                recordLength += writeByteIterable(bufferedWriter, CompressedUnsignedLongByteIterable.getIterable(loggable.getStructureId()));
                final int length = loggable.getDataLength();
                if (length < 0) {
                    throw new ExodusException("Negative length of loggable data");
                }
                recordLength += writeByteIterable(bufferedWriter, CompressedUnsignedLongByteIterable.getIterable(length));
                final ByteIterable data = loggable.getData();
                final int actualLength = writeByteIterableAsArray(bufferedWriter, data);
                if (actualLength != length) {
                    throw new IllegalArgumentException("Loggable contains invalid length descriptor");
                }
                recordLength += actualLength;
            }
            bufferedWriter.commit();
            highAddress += recordLength;
            if (getLastFileLength() == 0 || System.currentTimeMillis() > lastSyncTicks + config.getSyncPeriod()) {
                flush(true);
                if (getLastFileLength() == 0) {
                    bufferedWriter.close();
                }
            }
            return result;
        } catch (Throwable e) {
            highAddress = result;
            bufferedWriter.rollback();
            if (!(e instanceof NewFileCreationDeniedException)) {
                throw ExodusException.toExodusException(e);
            }
            return -1;
        }
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

    /**
     * Writes byte iterator to the log returning its length.
     *
     * @param writer   a writer
     * @param iterable byte iterable to write.
     * @return
     */
    private static int writeByteIterable(final TransactionalDataWriter writer, final ByteIterable iterable) {
        final ByteIterator iterator = iterable.iterator();
        int length = 0;
        while (iterator.hasNext()) {
            if (writer.write(iterator.next())) {
                ++length;
            } else {
                throw new NewFileCreationDeniedException();
            }
        }
        return length;
    }

    private static int writeByteIterableAsArray(final TransactionalDataWriter writer, final ByteIterable iterable) {
        int length = iterable.getLength();
        if (!writer.write(iterable.getBytesUnsafe(), 0, length)) {
            throw new NewFileCreationDeniedException();
        }
        return length;
    }

    private long getLastFileLength() {
        return highAddress % fileLengthBound;
    }

    public void setBufferedWriter(@NotNull final TransactionalDataWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
        // it's better for cached log cache to have always nonzero log identity:
        logIdentity = identityGenerator.incrementAndGet();
    }

    /**
     * Auxiliary exception necessary for aligned writing of loggables. It is thrown when a loggable
     * is trying to be placed into two files. In that case, we rollback the loggable, pad the last
     * file with nulls and write the loggable into the new file.
     */
    @SuppressWarnings({"serial", "SerializableClassInSecureContext", "SerializableHasSerializationMethods", "DeserializableClassInSecureContext", "EmptyClass"})
    private static final class NewFileCreationDeniedException extends RuntimeException {
    }
}
