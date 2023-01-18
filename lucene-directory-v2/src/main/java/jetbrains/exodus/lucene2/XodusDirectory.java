/**
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
package jetbrains.exodus.lucene2;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.io.SharedOpenFilesCache;
import jetbrains.exodus.log.CacheDataProvider;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.SharedLogCache;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class XodusDirectory extends Directory implements CacheDataProvider {
    private static final VarHandle SHORT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_LE_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final String NAME_TO_ADDRESS_STORE_NAME = "xodus.lucene.v2.nameToAddressStore";

    private final SharedLogCache sharedLogCache;
    private final StreamCipherProvider cipherProvider;

    private final int identity;

    private final Store nameToAddressStore;

    private final Path luceneOutputPath;

    private final Path luceneIndex;

    private final AtomicLong nextAddress;

    private final EnvironmentImpl environment;

    private final ConcurrentHashMap<String, Path> pendingDeletes = new ConcurrentHashMap<>();

    private final AtomicInteger opsSinceLastDelete = new AtomicInteger();

    private final AtomicLong outputIndex = new AtomicLong();

    private final AtomicLong ticks = new AtomicLong(System.nanoTime());
    private final Path path;

    private final int pageSize;

    protected XodusDirectory(Environment environment) {
        this.environment = (EnvironmentImpl) environment;
        var log = this.environment.getLog();
        var logConfig = log.getConfig();

        var path = Path.of(log.getLocation());

        if (!Files.isDirectory(path)) {
            throw new ExodusException("Path " + path + " does not exist in file system.");
        }

        if (logConfig.isSharedCache()) {
            sharedLogCache = (SharedLogCache) log.cache;
        } else {
            throw new ExodusException("Lucene directory : " + log.getLocation() +
                    " . Only environments with shared cache are supported.");
        }

        this.cipherProvider = logConfig.getCipherProvider();
        this.identity = Log.Companion.getIdentityGenerator().nextId();

        this.nameToAddressStore = environment.computeInTransaction(txn ->
                environment.openStore(NAME_TO_ADDRESS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));

        this.luceneOutputPath = path.resolve("luceneOutput");
        this.luceneIndex = path.resolve("luceneIndex");

        try {
            if (!Files.exists(luceneOutputPath)) {
                Files.createDirectory(luceneOutputPath);
            }

            if (!Files.exists(luceneIndex)) {
                Files.createDirectory(luceneIndex);
            }
        } catch (IOException e) {
            throw new ExodusException("Can not initialize lucene directory", e);
        }

        try {
            this.nextAddress = new AtomicLong(DirUtil.getNextAddress(luceneIndex));
        } catch (IOException e) {
            throw new ExodusException("Error of fetching of next file address for directory" + path, e);
        }

        this.path = path;
        this.pageSize = log.getCachePageSize();
    }

    @Override
    public String[] listAll() {
        ensureOpen();

        return environment.computeInReadonlyTransaction(txn -> {
            final ArrayList<String> names = new ArrayList<>();

            try (var cursor = nameToAddressStore.openCursor(txn)) {
                while (cursor.getNext()) {
                    var key = cursor.getKey();
                    var address = LongBinding.entryToLong(cursor.getValue());

                    if (address >= 0) {
                        names.add(StringBinding.entryToString(key));
                    }
                }
            }

            return names.toArray(new String[0]);
        });
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();

        var address = mapNameFileAddress(name);
        var indexFileName = DirUtil.getFileNameByAddress(address);
        var indexFilePath = luceneIndex.resolve(indexFileName);

        return Files.size(indexFilePath);
    }

    private long mapNameFileAddress(String name) throws FileNotFoundException {
        var address = environment.computeInReadonlyTransaction(txn -> {
            var key = StringBinding.stringToEntry(name);
            var addr = nameToAddressStore.get(txn, key);

            if (addr == null) {
                return null;
            }

            var result = LongBinding.entryToLong(addr);
            if (result < 0) {
                return null;
            }

            return Long.valueOf(result);
        });

        if (address == null) {
            throw new FileNotFoundException("File " + name + " does not exist");
        }

        return address.longValue();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        maybeDeletePendingFiles();

        var exist = environment.computeInTransaction(txn -> {
            var key = StringBinding.stringToEntry(name);
            var value = nameToAddressStore.get(txn, key);

            if (value != null) {
                return Boolean.TRUE;
            }

            nameToAddressStore.put(txn, key, LongBinding.longToEntry(-1));
            return Boolean.FALSE;
        });

        if (exist.booleanValue()) {
            throw new FileAlreadyExistsException("File " + name + " already exists");
        }

        return new XodusIndexOutput(name + outputIndex.getAndIncrement(), name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        maybeDeletePendingFiles();

        var fileName = IndexFileNames.segmentFileName(prefix,
                suffix + '_' + ticks.getAndIncrement(), "tmp");
        return createOutput(fileName, context);
    }

    @Override
    public void sync(Collection<String> names) {
        ensureOpen();

        environment.flushAndSync();

        environment.executeInReadonlyTransaction(txn -> {
            try (var cursor = nameToAddressStore.openCursor(txn)) {
                for (var fileName : names) {
                    var key = StringBinding.stringToEntry(fileName);
                    var value = cursor.getSearchKey(key);

                    if (value == null) {
                        throw new ExodusException("File " + fileName + " does not exist.");
                    }

                    var address = LongBinding.entryToLong(value);

                    if (address >= 0) {
                        var indexName = DirUtil.getFileNameByAddress(address);
                        try {
                            IOUtils.fsync(luceneIndex.resolve(indexName), false);
                        } catch (IOException e) {
                            throw new ExodusException("Error during syncing of file " + fileName, e);
                        }
                    } else {
                        throw new ExodusException("File " + fileName + " does not exist.");
                    }
                }
            }
        });

        maybeDeletePendingFiles();
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();

        environment.flushAndSync();

        IOUtils.fsync(luceneIndex, true);

        maybeDeletePendingFiles();
    }

    @Override
    public void rename(String source, String dest) {
        ensureOpen();

        maybeDeletePendingFiles();

        environment.executeInTransaction(txn -> {
            var fromKey = StringBinding.stringToEntry(source);
            var value = nameToAddressStore.get(txn, fromKey);

            if (value == null) {
                throw new ExodusException("File " + source + " does not exist.");
            }

            var address = LongBinding.entryToLong(value);
            if (address < 0) {
                throw new ExodusException("File " + source + " does not exist.");
            }

            nameToAddressStore.delete(txn, fromKey);

            var toKey = StringBinding.stringToEntry(dest);
            nameToAddressStore.put(txn, toKey, value);
        });
    }

    @Override
    public synchronized void close() throws IOException {
        deletePendingFiles();
        environment.close();

        var filesCache = SharedOpenFilesCache.getInstance();
        filesCache.removeDirectory(luceneIndex.toFile());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + path;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();

        var addr = environment.computeInTransaction(txn -> {
            var key = StringBinding.stringToEntry(name);
            var byteAddr = nameToAddressStore.get(txn, key);

            if (byteAddr == null) {
                return null;
            }

            var address = LongBinding.entryToLong(byteAddr);
            if (address >= 0) {
                nameToAddressStore.delete(txn, key);
            } else {
                return null;
            }

            return Long.valueOf(address);
        });

        if (addr == null) {
            throw new FileNotFoundException("File " + name + " does not exist");
        }

        var indexFile = DirUtil.getFileNameByAddress(addr.longValue());
        pendingDeletes.put(name, luceneIndex.resolve(indexFile));
        privateDeleteFile(name);

        maybeDeletePendingFiles();
    }

    @Override
    public synchronized Set<String> getPendingDeletions() {
        deletePendingFiles();

        if (pendingDeletes.isEmpty()) {
            return Collections.emptySet();
        }

        return Set.copyOf(pendingDeletes.keySet());
    }

    private void maybeDeletePendingFiles() {
        if (!pendingDeletes.isEmpty()) {
            // This is a silly heuristic to try to avoid O(N^2), where N = number of files pending deletion, behaviour on Windows:
            int count = opsSinceLastDelete.incrementAndGet();

            if (count >= pendingDeletes.size()) {
                opsSinceLastDelete.addAndGet(-count);
                deletePendingFiles();
            }
        }
    }

    private synchronized void deletePendingFiles() {
        if (!pendingDeletes.isEmpty()) {
            // Clone the set since we mutate it in privateDeleteFile:
            for (final String fileToDelete : new HashSet<>(pendingDeletes.keySet())) {
                privateDeleteFile(fileToDelete);
            }
        }
    }

    private void privateDeleteFile(String file) {
        var cache = SharedOpenFilesCache.getInstance();
        var path = pendingDeletes.get(file);
        try {
            cache.removeFile(path.toFile());
            Files.deleteIfExists(path);
            pendingDeletes.remove(file);
        } catch (IOException ioe) {
            // On windows, a file delete can fail because there's still an open
            // file handle against it.  We record this in pendingDeletes and
            // try again later.

            pendingDeletes.put(file, path);
        }
    }


    @Override
    public int getIdentity() {
        return identity;
    }

    @Override
    public byte[] readPage(long pageAddress, long fileAddress) {
        var filesCache = SharedOpenFilesCache.getInstance();
        var fileName = DirUtil.getFileNameByAddress(fileAddress);
        var pageOffset = pageAddress - fileAddress;
        var filePath = luceneIndex.resolve(fileName);

        var page = new byte[pageSize];

        try (var file = filesCache.getCachedFile(filePath.toFile())) {
            DirUtil.readFully(file, pageOffset, page);
        } catch (IOException e) {
            throw new ExodusException("Can not access file " + fileName, e);
        }

        return page;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();

        var fileAddress = mapNameFileAddress(name);
        var indexFileName = DirUtil.getFileNameByAddress(fileAddress);

        var indexFilePath = luceneIndex.resolve(indexFileName);
        var fileSize = Files.size(indexFilePath);

        return new XodusIndexInput("XodusIndexInput(path=\"" + indexFilePath + "\")", fileAddress,
                0, fileSize);
    }

    float hitRate() {
        return sharedLogCache.hitRate();
    }

    @Override
    public Lock obtainLock(String name) {
        return NoLockFactory.INSTANCE.obtainLock(this, name);
    }

    final class XodusIndexOutput extends OutputStreamIndexOutput {
        /**
         * The maximum chunk size is 8192 bytes, because file channel mallocs
         * a native buffer outside of stack if the write buffer size is larger.
         */
        static final int CHUNK_SIZE = 8192;

        final Path filePath;
        final String indexName;

        boolean closed;

        public XodusIndexOutput(String fileName, final String indexName) throws IOException {
            this(fileName, indexName, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        }

        XodusIndexOutput(String fileName, String indexName, OpenOption... options) throws IOException {
            super("XodusIndexOutput(path=\"" + luceneOutputPath.resolve(fileName) + "\")", fileName,
                    new FilterOutputStream(Files.newOutputStream(luceneOutputPath.resolve(fileName), options)) {
                        // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
                        @Override
                        public void write(byte @NotNull [] b, int offset, int length) throws IOException {
                            while (length > 0) {
                                final int chunk = Math.min(length, CHUNK_SIZE);
                                out.write(b, offset, chunk);
                                length -= chunk;
                                offset += chunk;
                            }
                        }
                    }, CHUNK_SIZE);

            filePath = luceneOutputPath.resolve(fileName);
            this.indexName = indexName;
        }

        @Override
        public String getName() {
            return indexName;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                super.close();

                var fileLength = Files.size(filePath);

                var fileAddress = nextAddress.getAndAdd(fileLength + 1);
                var indexPath = luceneIndex.resolve(DirUtil.getFileNameByAddress(fileAddress));

                assert !Files.exists(indexPath);
                try {
                    Files.move(filePath, indexPath, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(filePath, indexPath);
                }

                environment.executeInTransaction(txn -> {
                    var key = StringBinding.stringToEntry(indexName);
                    var value = LongBinding.longToEntry(fileAddress);

                    nameToAddressStore.put(txn, key, value);
                });

                closed = true;
            }
        }
    }

    final class XodusIndexInput extends IndexInput {

        private final long fileAddress;
        private long position;

        private final long basePosition;

        private final long end;

        private byte[] page;
        private long pageAddress = -1;


        private XodusIndexInput(String resourceDescription, long fileAddress, long position, long end) {
            super(resourceDescription);

            this.fileAddress = fileAddress;
            this.end = end;
            this.basePosition = position;
            this.position = position;
        }

        @Override
        public byte readByte() throws IOException {
            if (position >= end) {
                throw new EOFException("Read past EOF. Position: " + position + ".");
            }

            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            readPageIfNeeded(pageAddress);

            position++;
            return page[pageOffset];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            if (position + len > end) {
                throw new EOFException("Read past EOF. Position: " + position + ", requested bytes : " + len);
            }

            int bytesRead = 0;

            while (bytesRead < len) {
                var pageOffset = (int) position & (pageSize - 1);
                var pageAddress = fileAddress + position - pageOffset;

                readPageIfNeeded(pageAddress);

                var bytesToCopy = Math.min(len - bytesRead, pageSize - pageOffset);

                System.arraycopy(page, pageOffset, b, offset + bytesRead, bytesToCopy);

                bytesRead += bytesToCopy;
                position += bytesToCopy;
            }
        }

        private void readPageIfNeeded(long pageAddress) {
            if (this.pageAddress != pageAddress) {
                this.page = sharedLogCache.getPage(XodusDirectory.this, pageAddress, fileAddress);
                this.pageAddress = pageAddress;
            }
        }

        @Override
        public void close() {
            //nothing
        }

        @Override
        public long getFilePointer() {
            return position - basePosition;
        }

        @Override
        public void seek(long pos) throws IOException {
            pos += basePosition;

            if (pos > end) {
                throw new EOFException("Position past the file size was requested. Position : "
                        + pos + ", file size : " + end);
            }

            position = pos;

            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            if (pageAddress != this.pageAddress) {
                page = null;
            }
        }

        @Override
        public long length() {
            return end - basePosition;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription +
                        " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }

            return new XodusIndexInput(sliceDescription, fileAddress, basePosition + offset,
                    basePosition + offset + length);
        }

        @Override
        public short readShort() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            final long remaining = Math.min(end - position, pageSize - pageOffset);

            if (remaining >= Short.BYTES) {
                readPageIfNeeded(pageAddress);
                var value = (short) SHORT_VAR_HANDLE.get(page, pageOffset);
                position += Short.BYTES;

                return value;
            }

            return super.readShort();
        }

        @Override
        public int readInt() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            final long remaining = Math.min(end - position, pageSize - pageOffset);
            if (remaining >= Integer.BYTES) {
                readPageIfNeeded(pageAddress);

                var value = (int) INT_VAR_HANDLE.get(page, pageOffset);
                position += Integer.BYTES;

                return value;
            }

            return super.readInt();
        }

        @Override
        public int readVInt() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;


            final long remaining = Math.min(end - position, pageSize - pageOffset);

            if (5 <= remaining) {
                readPageIfNeeded(pageAddress);

                byte b = page[pageOffset];
                position++;

                if (b >= 0) {
                    return b;
                }

                pageOffset++;
                int i = b & 0x7F;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7F) << 7;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7F) << 14;

                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7F) << 21;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
                i |= (b & 0x0F) << 28;
                if ((b & 0xF0) == 0) {
                    return i;
                }
                throw new IOException("Invalid vInt detected (too many bits)");
            } else {
                return super.readVInt();
            }
        }

        @Override
        public long readLong() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            final long remaining = Math.min(end - position, pageSize - pageOffset);

            if (remaining >= Long.BYTES) {
                readPageIfNeeded(pageAddress);

                var value = (long) LONG_VAR_HANDLE.get(page, pageOffset);
                position += Long.BYTES;

                return value;
            }

            return super.readLong();
        }

        @Override
        public long readVLong() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            final long remaining = Math.min(end - position, pageSize - pageOffset);

            if (9 <= remaining) {
                readPageIfNeeded(pageAddress);

                byte b = page[pageOffset];
                position++;

                if (b >= 0) {
                    return b;
                }

                pageOffset++;
                long i = b & 0x7FL;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 7;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 14;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 21;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 28;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 35;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 42;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 49;
                if (b >= 0) {
                    return i;
                }

                pageOffset++;
                b = page[pageOffset];
                position++;

                i |= (b & 0x7FL) << 56;
                if (b >= 0) {
                    return i;
                }

                throw new IOException("Invalid vLong detected (negative values disallowed)");
            } else {
                return super.readVLong();
            }
        }

        @Override
        public void readLELongs(long[] dst, int offset, int length) throws IOException {
            FutureObjects.checkFromIndexSize(offset, length, dst.length);

            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            final int bytesNeeded = length * Long.BYTES;
            final long remaining = Math.min(length - position, pageSize - pageOffset);

            if (remaining >= bytesNeeded) {
                readPageIfNeeded(pageAddress);

                for (int i = offset; i < length; i++, pageOffset += Long.BYTES) {
                    dst[i] = (long) LONG_LE_VAR_HANDLE.get(page, pageOffset);
                }

                position += bytesNeeded;
            } else {
                super.readLELongs(dst, offset, length);
            }
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            if (numBytes < 0) {
                throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
            }

            var bytesLeft = end - position;
            if (bytesLeft < numBytes) {
                throw new EOFException("Amount of bytes to skip is bigger than file size. Position : " + position +
                        " file size : " + end + ", bytes to skip : " + numBytes);
            }

            this.position += numBytes;

            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            readPageIfNeeded(pageAddress);
        }
    }

    @Override
    protected void ensureOpen() throws AlreadyClosedException {
        if (!environment.isOpen()) {
            throw new AlreadyClosedException("Directory " + path + " already closed");
        }
    }
}
