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
package jetbrains.exodus.lucene2;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.io.SharedOpenFilesCache;
import jetbrains.exodus.log.CacheDataProvider;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.SharedLogCache;
import jetbrains.exodus.util.IOUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class XodusDirectory extends Directory implements CacheDataProvider {
    private static final Logger logger = LoggerFactory.getLogger(XodusDirectory.class);
    private static final VarHandle SHORT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_LE_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final String NAME_TO_ADDRESS_STORE_NAME = "xodus.lucene.v2.nameToAddressStore";

    private static final AtomicLong ticks = new AtomicLong(System.nanoTime());

    private final SharedLogCache sharedLogCache;
    private final StreamCipherProvider cipherProvider;

    private final byte[] cipherKey;

    private final int identity;

    private final Store nameToAddressStore;

    private final Path luceneOutputPath;

    private final Path luceneIndex;

    private final AtomicLong nextAddress;

    private final EnvironmentImpl environment;

    private final ConcurrentHashMap<String, Path> pendingDeletes = new ConcurrentHashMap<>();

    private final AtomicInteger opsSinceLastDelete = new AtomicInteger();

    private final AtomicLong outputIndex = new AtomicLong();

    private final Path path;

    private final int pageSize;

    private final AtomicLong ivGen;

    private final Random ivRnd = new Random();

    public XodusDirectory(Environment environment) throws IOException {
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
        this.cipherKey = logConfig.getCipherKey();

        this.identity = Log.Companion.getIdentityGenerator().nextId();

        this.luceneOutputPath = path.resolve("luceneOutput");
        this.luceneIndex = path.resolve("luceneIndex");

        if (Files.exists(luceneOutputPath)) {
            IOUtil.deleteRecursively(luceneOutputPath.toFile());
        }

        if (!environment.computeInTransaction(txn -> environment.storeExists(NAME_TO_ADDRESS_STORE_NAME, txn))) {
            var removalMessage = NAME_TO_ADDRESS_STORE_NAME +
                    " store does not exist. All Lucene index files will be removed.";

            if (Files.exists(luceneIndex)) {
                logger.warn(removalMessage);
                IOUtil.deleteRecursively(luceneIndex.toFile());
            }
        }

        this.nameToAddressStore = environment.computeInTransaction(txn ->
                environment.openStore(NAME_TO_ADDRESS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));

        if (!Files.exists(luceneOutputPath)) {
            Files.createDirectory(luceneOutputPath);
        }

        if (!Files.exists(luceneIndex)) {
            Files.createDirectory(luceneIndex);
        }

        var runCheck = ((EnvironmentImpl) environment).isCheckLuceneDirectory();
        LongOpenHashSet storedFiles = new LongOpenHashSet();

        if (runCheck) {
            logger.warn("Xodus directory " + log.getLocation() + " : clearing broken links between files and indexes.");

            var namesToDelete = environment.computeInReadonlyTransaction(txn -> {
                var toDelete = new ArrayList<ByteIterable>();

                try (var cursor = nameToAddressStore.openCursor(txn)) {
                    while (cursor.getNext()) {
                        final var address = LongBinding.entryToLong(cursor.getValue());
                        final var indexFileName = DirUtil.getFileNameByAddress(address);

                        storedFiles.add(address);

                        if (!Files.exists(luceneIndex.resolve(indexFileName))) {
                            var key = cursor.getKey();
                            logger.info("File " + StringBinding.entryToString(key) +
                                    " is absent and will be removed from index.");

                            toDelete.add(key);
                        }
                    }
                }

                return toDelete;
            });

            environment.executeInTransaction(txn -> {
                for (var name : namesToDelete) {
                    nameToAddressStore.delete(txn, name);
                }
            });
        }

        long[] maxAddressLengthIv = new long[]{-1, -1, -1};
        var fetchIvs = cipherKey != null;

        LongOpenHashSet filesToDelete = new LongOpenHashSet();
        try (var fileStream = DirUtil.listLuceneFiles(luceneIndex)) {
            fileStream.forEach(p -> {
                        var indexFile = p.getFileName().toString();
                        var address = DirUtil.getFileAddress(indexFile);

                        if (runCheck && !storedFiles.contains(address)) {
                            logger.info("File " + indexFile + " is absent in index and will be removed.");
                            filesToDelete.add(address);
                            return;
                        }

                        if (address > maxAddressLengthIv[0]) {
                            maxAddressLengthIv[0] = address;
                            try {
                                maxAddressLengthIv[1] = Files.size(p);
                            } catch (IOException e) {
                                throw new ExodusException("Error during fetching of size of file " + p, e);
                            }
                        }

                        if (fetchIvs) {
                            var ivFileName = DirUtil.getIvFileName(indexFile);
                            var ivPath = luceneIndex.resolve(ivFileName);

                            if (Files.exists(ivPath)) {
                                try (var ivStream = new DataInputStream(Files.newInputStream(ivPath))) {
                                    var iv = ivStream.readLong();

                                    if (iv > maxAddressLengthIv[2]) {
                                        maxAddressLengthIv[2] = iv;
                                    }
                                } catch (EOFException eof) {
                                    //ignore
                                } catch (IOException e) {
                                    throw new ExodusException("Can not read iv file " + ivRnd, e);
                                }
                            }
                        }
                    }
            );
        }

        if (!filesToDelete.isEmpty()) {
            var addressIterator = filesToDelete.longIterator();

            while (addressIterator.hasNext()) {
                var address = addressIterator.nextLong();
                var indexFileName = DirUtil.getFileNameByAddress(address);
                var indexFile = luceneIndex.resolve(indexFileName);

                var ivFileName = DirUtil.getIvFileName(indexFileName);
                var ivFile = luceneIndex.resolve(ivFileName);

                try {
                    Files.deleteIfExists(indexFile);
                    Files.deleteIfExists(ivFile);
                } catch (IOException e) {
                    throw new ExodusException("Can not delete file " + indexFile, e);
                }
            }
        }

        this.pageSize = log.getCachePageSize();

        if (maxAddressLengthIv[0] >= 0) {
            var nextAddress = maxAddressLengthIv[0] + maxAddressLengthIv[1];
            var pages = (nextAddress + pageSize - 1) / pageSize;

            if (pages == 0) {
                pages = 1;
            }

            this.nextAddress = new AtomicLong(pages * pageSize);
        } else {
            this.nextAddress = new AtomicLong(0);
        }

        this.ivGen = new AtomicLong(maxAddressLengthIv[2] + 1);
        this.path = path;
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
        var fileSize = Files.size(indexFilePath);

        if (cipherKey == null) {
            return fileSize;
        }

        return subtractWithIvSpace(Long.BYTES, fileSize);
    }

    private long subtractWithIvSpace(long start, long end) {
        if (cipherKey == null) {
            return end - start;
        }

        var spaceLeftInPage = pageSize - (start & (pageSize - 1));
        var diff = end - start;

        if (diff > spaceLeftInPage) {
            start += spaceLeftInPage;
            var pages = (end - start + pageSize - 1) / pageSize;

            return end - start - pages * Long.BYTES + spaceLeftInPage;
        }

        return diff;
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

        var index = outputIndex.getAndIncrement();
        var fileName = name + index;
        if (cipherKey == null) {
            return new XodusIndexOutput(fileName, name, null, openOutputStream(fileName));
        }

        return new XodusIndexOutput(fileName,
                name, name + index + ".iv", openOutputStream(fileName));
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

                        if (cipherKey != null) {
                            var ivFileName = DirUtil.getIvFileName(indexName);
                            try {
                                var ivFilePath = luceneIndex.resolve(ivFileName);
                                if (Files.exists(ivFilePath)) {
                                    IOUtils.fsync(ivFilePath, false);
                                }
                            } catch (IOException e) {
                                throw new ExodusException("Error during syncing of file " + ivFileName, e);
                            }
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
            if (cache != null) {
                cache.removeFile(path.toFile());
            }

            Files.deleteIfExists(path);

            if (cipherKey != null) {
                var ivPath = luceneIndex.resolve(DirUtil.getIvFileName(path.getFileName().toString()));
                Files.deleteIfExists(ivPath);
            }

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

        int dataRead;
        try (var file = filesCache.getCachedFile(filePath.toFile())) {
            dataRead = DirUtil.readFully(file, pageOffset, page);
        } catch (IOException e) {
            throw new ExodusException("Can not access file " + fileName, e);
        }

        if (cipherKey != null) {
            assert dataRead > Long.BYTES;

            var cipher = cipherProvider.newCipher();
            var iv = (long) LONG_VAR_HANDLE.get(page, 0);

            cipher.init(cipherKey, iv);
            for (int i = Long.BYTES; i < dataRead; i++) {
                page[i] = cipher.crypt(page[i]);
            }
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

        if (cipherKey != null) {
            return new XodusIndexInput("XodusIndexInput(path=\"" + indexFilePath + "\")", fileAddress,
                    Long.BYTES, fileSize);

        }

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


    private static long asHashedIv(long iv) {
        return iv * 6364136223846793005L - 4019793664819917546L;
    }

    private OutputStream openOutputStream(String fileName) throws IOException {
        var fileStream = Files.newOutputStream(luceneOutputPath.resolve(fileName), StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        if (cipherKey == null) {
            return fileStream;
        }

        return new StreamCipherOutputStream(fileStream, cipherKey, cipherProvider,
                ivGen, ivRnd, pageSize);
    }


    final class XodusIndexOutput extends OutputStreamIndexOutput {
        /**
         * The maximum chunk size is 8192 bytes, because file channel mallocs
         * a native buffer outside of stack if the write buffer size is larger.
         */
        static final int CHUNK_SIZE = 8192;

        final Path filePath;
        final Path ivFilePath;

        final String indexName;

        boolean closed;

        final OutputStream os;


        XodusIndexOutput(String fileName, String indexName, String ivFileName, OutputStream stream) {
            super("XodusIndexOutput(path=\"" + luceneOutputPath.resolve(fileName) + "\")", fileName,
                    new FilterOutputStream(stream) {
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
            if (ivFileName != null) {
                ivFilePath = luceneIndex.resolve(ivFileName);
            } else {
                ivFilePath = null;
            }

            this.indexName = indexName;
            this.os = stream;
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
                var pages = (fileLength + pageSize - 1) / pageSize;
                if (pages == 0) {
                    pages = 1;
                }

                var fileAddress = nextAddress.getAndAdd(pages * pageSize);
                var indexFileName = DirUtil.getFileNameByAddress(fileAddress);
                var indexPath = luceneIndex.resolve(indexFileName);

                try {
                    Files.move(filePath, indexPath, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(filePath, indexPath);
                }

                if (ivFilePath != null) {
                    try (var ivStream = new DataOutputStream(Files.newOutputStream(ivFilePath))) {
                        ivStream.writeLong(((StreamCipherOutputStream) os).maxIv);
                    }
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

    private static final class StreamCipherOutputStream extends FilterOutputStream {
        private final byte[] cipherKey;
        private final StreamCipher cipher;

        private final AtomicLong ivGen;

        private final Random ivRnd;


        private final int pageSize;

        private long position;

        private long ivPosition;

        private long maxIv;


        public StreamCipherOutputStream(final @NotNull OutputStream out,
                                        final byte @NotNull [] cipherKey,
                                        final @NotNull StreamCipherProvider cipherProvider,
                                        final @NotNull AtomicLong ivGen,
                                        final Random ivRnd, final int pageSize) throws IOException {
            super(out);

            this.ivGen = ivGen;
            this.ivRnd = ivRnd;
            this.cipherKey = cipherKey;

            cipher = cipherProvider.newCipher();

            this.pageSize = pageSize;

            generateAndStoreCipher();
        }


        private void generateAndStoreCipher() throws IOException {
            final long iv = asHashedIv(ivGen.getAndAdd(ivRnd.nextInt(16)));

            cipher.init(cipherKey, iv);
            maxIv = iv;
            var rawIv = new byte[Long.BYTES];

            LONG_VAR_HANDLE.set(rawIv, 0, iv);
            out.write(rawIv);
            position += Long.BYTES;
        }


        @Override
        public void write(int b) throws IOException {
            if (position - ivPosition == pageSize) {
                ivPosition = position;
                generateAndStoreCipher();
            }

            b = cipher.crypt((byte) b);

            out.write(b);
            position++;
        }

        @Override
        public void write(byte @NotNull [] b) throws IOException {
            write(b, 0, b.length);
        }

        public void write(byte @NotNull [] b, int off, int len) throws IOException {
            b = Arrays.copyOfRange(b, off, off + len);

            if (position - ivPosition == pageSize) {
                ivPosition = position;
                generateAndStoreCipher();
            }

            int offset = -1;
            for (int i = 0; i < b.length; i++) {
                b[i] = cipher.crypt(b[i]);
                position++;

                if (position - ivPosition == pageSize && i < b.length - 1) {
                    out.write(b, offset + 1, i - offset);

                    ivPosition = position;
                    generateAndStoreCipher();

                    offset = i;
                }
            }

            if (offset < b.length - 1) {
                out.write(b, offset + 1, b.length - (offset + 1));
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
            movePosition(1);

            return page[pageOffset];
        }

        private void movePosition(int diff) {
            position += diff;
            if (cipherKey != null && (position & (pageSize - 1)) == 0) {
                position += Long.BYTES;
            }
        }


        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            var lastPos = addWithIvSpace(position, len);
            lastPos = correctEndPosition(lastPos);

            if (lastPos > end) {
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

                movePosition(bytesToCopy);
            }
        }

        private long correctEndPosition(long lastPos) {
            if (lastPos > end && (lastPos & (pageSize - 1)) == Long.BYTES) {
                lastPos -= Long.BYTES;
            }
            return lastPos;
        }

        private void readPageIfNeeded(long pageAddress) {
            if (this.page == null || this.pageAddress != pageAddress) {
                this.page = sharedLogCache.getPage(XodusDirectory.this, pageAddress, fileAddress
                );
                this.pageAddress = pageAddress;
            }
        }

        @Override
        public void close() {
            //nothing
        }

        @Override
        public long getFilePointer() {
            return subtractWithIvSpace(basePosition, position);
        }

        private long addWithIvSpace(final long position, long len) {
            if (cipherKey == null) {
                return position + len;
            }

            var spaceLeftInPage = pageSize - (position & (pageSize - 1));
            if (len >= spaceLeftInPage) {
                len -= spaceLeftInPage;

                var dataPageSize = pageSize - Long.BYTES;
                var pages = (len + dataPageSize - 1) / dataPageSize;
                var result = position + spaceLeftInPage + len + pages * Long.BYTES;

                if ((result & (pageSize - 1)) == 0) {
                    return result + Long.BYTES;
                }

                return result;
            }

            return position + len;
        }

        @Override
        public void seek(long pos) throws IOException {
            pos = addWithIvSpace(basePosition, pos);
            pos = correctEndPosition(pos);

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
            return subtractWithIvSpace(basePosition, end);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription +
                        " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }

            var start = correctEndPosition(addWithIvSpace(basePosition, offset));
            var end = correctEndPosition(addWithIvSpace(start, length));

            return new XodusIndexInput(sliceDescription, fileAddress, start,
                    end);
        }

        @Override
        public short readShort() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            long remaining = Math.min(end - position, pageSize - pageOffset);

            if (remaining >= Short.BYTES) {
                readPageIfNeeded(pageAddress);

                pageOffset = (int) position & (pageSize - 1);
                remaining = Math.min(end - position, pageSize - pageOffset);
                if (remaining < Short.BYTES) {
                    return super.readShort();
                }

                var value = (short) SHORT_VAR_HANDLE.get(page, pageOffset);
                movePosition(Short.BYTES);

                return value;
            }

            return super.readShort();
        }

        @Override
        public int readInt() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;

            long remaining = Math.min(end - position, pageSize - pageOffset);
            if (remaining >= Integer.BYTES) {
                readPageIfNeeded(pageAddress);
                pageOffset = (int) position & (pageSize - 1);

                var value = (int) INT_VAR_HANDLE.get(page, pageOffset);
                movePosition(Integer.BYTES);

                return value;
            }

            return super.readInt();
        }

        @Override
        public int readVInt() throws IOException {
            var pageOffset = (int) position & (pageSize - 1);
            var pageAddress = fileAddress + position - pageOffset;


            long remaining = Math.min(end - position, pageSize - pageOffset);

            int positionOffset = 0;
            if (5 <= remaining) {
                readPageIfNeeded(pageAddress);
                try {
                    pageOffset = (int) position & (pageSize - 1);
                    remaining = Math.min(end - position, pageSize - pageOffset);
                    if (remaining < 5) {
                        return super.readVInt();
                    }

                    byte b = page[pageOffset];
                    positionOffset++;

                    if (b >= 0) {
                        return b;
                    }

                    pageOffset++;
                    int i = b & 0x7F;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7F) << 7;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7F) << 14;

                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7F) << 21;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
                    i |= (b & 0x0F) << 28;
                    if ((b & 0xF0) == 0) {
                        return i;
                    }
                } finally {
                    movePosition(positionOffset);
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
                movePosition(Long.BYTES);

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

                int positionOffset = 0;
                try {
                    byte b = page[pageOffset];
                    positionOffset++;

                    if (b >= 0) {
                        return b;
                    }

                    pageOffset++;
                    long i = b & 0x7FL;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 7;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 14;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 21;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 28;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 35;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 42;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 49;
                    if (b >= 0) {
                        return i;
                    }

                    pageOffset++;
                    b = page[pageOffset];
                    positionOffset++;

                    i |= (b & 0x7FL) << 56;
                    if (b >= 0) {
                        return i;
                    }
                } finally {
                    movePosition(positionOffset);
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

                movePosition(bytesNeeded);
            } else {
                super.readLELongs(dst, offset, length);
            }
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            if (numBytes < 0) {
                throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
            }

            var bytesLeft = subtractWithIvSpace(position, end);
            if (bytesLeft < numBytes) {
                throw new EOFException("Amount of bytes to skip is bigger than file size. Position : " + position +
                        " file size : " + end + ", bytes to skip : " + numBytes);
            }

            this.position = addWithIvSpace(position, numBytes);

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
