/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.SharedOpenFilesCache;
import jetbrains.exodus.log.CacheDataProvider;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.SharedLogCache;
import jetbrains.exodus.lucene2.DirectoryFileNamesRegistry.FileDescription;
import jetbrains.exodus.util.IOUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
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


public class XodusNonXodusDirectory extends Directory implements CacheDataProvider {
    private static final Logger logger = LoggerFactory.getLogger(XodusNonXodusDirectory.class);
    private static final VarHandle SHORT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_LE_VAR_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final int[] SHIFTS = {0, 7, 14, 21, 28, 35, 42, 49, 56, 63};

    private static final AtomicLong ticks = new AtomicLong(System.nanoTime());

    private final SharedLogCache sharedLogCache;
    private final StreamCipherProvider cipherProvider;

    private final byte[] cipherKey;

    private final int identity;

    private final Path luceneOutputPath;

    private final Path luceneIndex;

    private final AtomicLong nextAddress;

    private final ConcurrentHashMap<String, FileDescription> pendingDeletes = new ConcurrentHashMap<>();

    private final DirectoryFileNamesRegistry fileNameRegistry;

    private final AtomicInteger opsSinceLastDelete = new AtomicInteger();

    private final AtomicLong outputIndex = new AtomicLong();

    private final Path rootPath;

    private final int pageSize;

    private final AtomicLong ivGen;

    private final Random ivRnd = new Random();

    private volatile boolean closed = false;

    public XodusNonXodusDirectory(
            Path rootPath, SharedLogCache sharedLogCache, int cachePageSize,
            StreamCipherProvider cipherProvider, byte[] cipherKey
    ) throws IOException {

        if (!Files.isDirectory(rootPath)) {
            throw new ExodusException("Path " + rootPath + " does not exist in file system.");
        }
        this.sharedLogCache = sharedLogCache;

        this.cipherProvider = cipherProvider;
        this.cipherKey = cipherKey;

        this.identity = Log.Companion.getIdentityGenerator().nextId();

        this.luceneOutputPath = rootPath.resolve("luceneOutput");
        this.luceneIndex = rootPath.resolve("luceneIndex");

        if (Files.exists(luceneOutputPath)) {
            IOUtil.deleteRecursively(luceneOutputPath.toFile());
        } else {
            Files.createDirectory(luceneOutputPath);
        }

        if (!Files.exists(luceneIndex)) {
            Files.createDirectory(luceneIndex);
        }

        fileNameRegistry = new DirectoryFileNamesRegistry(luceneIndex);
        long[] maxAddressLengthIv = new long[]{-1, -1, -1};
        var fetchIvs = cipherKey != null;

        final Set<Path> filesToDelete = new HashSet<>();
        final Map<String, Path> indexFilesWithIv = new HashMap<>();

        try (var fileStream = Files.newDirectoryStream(luceneIndex)) {
            fileStream.forEach(p -> {
                final var realFileName = p.getFileName().toString();

                final var nameFromIvFile = DirectoryFileNamesRegistry.indexNameFromIvFileName(realFileName);
                if (nameFromIvFile != null) {
                    indexFilesWithIv.put(nameFromIvFile, p);
                    return;
                }

                final var fileDesc = fileNameRegistry.tryRegisterFile(realFileName);

                if (fileDesc == null) {
                    filesToDelete.add(p);
                    return;
                }

                if (fileDesc.address() > maxAddressLengthIv[0]) {
                    maxAddressLengthIv[0] = fileDesc.address();
                    try {
                        maxAddressLengthIv[1] = Files.size(p);
                    } catch (IOException e) {
                        throw new ExodusException("Error during fetching of size of file " + p, e);
                    }
                }

                if (fetchIvs) {
                    var ivPath = fileDesc.ivFilePath();

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
            });
        }

        for (Path toDelete : filesToDelete) {
            try {
                Files.deleteIfExists(toDelete);
            } catch (IOException e) {
                throw new ExodusException("Can not delete file " + toDelete, e);
            }
        }

        indexFilesWithIv.keySet().removeAll(fileNameRegistry.indexNames());
        for (Path ivPath : indexFilesWithIv.values()) {
            try {
                Files.deleteIfExists(ivPath);
            } catch (IOException e) {
                throw new ExodusException("Can not delete orphan iv file " + ivPath, e);
            }
        }

        this.pageSize = cachePageSize;

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
        this.rootPath = rootPath;
    }

    @Override
    public String[] listAll() {
        ensureOpen();
        return fileNameRegistry.indexNames()
                .stream()
                .sorted()
                .toArray(String[]::new);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();

        final var fileDesc = fileNameRegistry.lookupByIndexName(name);
        final var fileSize = Files.size(fileDesc.filePath());

        return cipherKey == null ? fileSize : subtractWithIvSpace(Long.BYTES, fileSize);
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

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        maybeDeletePendingFiles(); // todo: explore

        // todo: what if we call this twice with the same name, before the actual file is saved?
        // uniqueness of files is not guaranteed anymore.
        if (fileNameRegistry.existsIndexName(name)) {
            throw new FileAlreadyExistsException("File " + name + " already exists");
        }

        // name of the file in the output folder. it will be moved to the index folder once the output is closed.
        final var outputFileName = name + outputIndex.getAndIncrement();

        return new XodusIndexOutput(outputFileName, name, cipherKey != null, openOutputStream(outputFileName));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        maybeDeletePendingFiles();

        final var fileName =
                IndexFileNames.segmentFileName(prefix, suffix + '_' + ticks.getAndIncrement(), "tmp");
        return createOutput(fileName, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

        ensureOpen();
        for (final var indexName : names) {
            final var fileDesc = fileNameRegistry.lookupByIndexName(indexName);

            try {
                IOUtils.fsync(fileDesc.filePath(), false);
            } catch (IOException e) {
                throw new ExodusException("Error during syncing of file " + fileDesc.filePath(), e);
            }

            if (cipherKey != null) {
                try {
                    if (Files.exists(fileDesc.ivFilePath())) {
                        IOUtils.fsync(fileDesc.ivFilePath(), false);
                    }
                } catch (IOException e) {
                    throw new ExodusException("Error during syncing of file " + fileDesc.filePath(), e);
                }
            }
        }

        maybeDeletePendingFiles();
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        IOUtils.fsync(luceneIndex, true);
        maybeDeletePendingFiles();
    }

    @Override
    public void rename(String source, String dest) throws IOException {

        ensureOpen();
        // todo: ATOMICITY!
        if (pendingDeletes.containsKey(source)) {
            throw new NoSuchFileException(
                    "file \"" + source + "\" is pending delete and cannot be moved");
        }
        final var sourceDesc = fileNameRegistry.lookupByIndexName(source);
        maybeDeletePendingFiles();
        if (pendingDeletes.remove(dest) != null) {
            privateDeleteFile(dest); // try again to delete it - this is the best effort
            pendingDeletes.remove(dest); // watch out if the delete fails, it's back in here
        }

        final var destDesc = fileNameRegistry.createFileDescription(dest, sourceDesc.address());
        Files.move(sourceDesc.filePath(), destDesc.filePath(), StandardCopyOption.ATOMIC_MOVE);
        deleteFile(source);
        fileNameRegistry.register(destDesc);
    }

    @Override
    public synchronized void close() throws IOException {
        deletePendingFiles();

        closed = true;
        var filesCache = SharedOpenFilesCache.getInstance();
        filesCache.removeDirectory(luceneIndex.toFile());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + rootPath;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        // todo: files can be deleted later, so what happens if the directory is closed before the
        // actual deletion.

        final var fileDesc = fileNameRegistry.removeByIndexName(name);

        pendingDeletes.put(name, fileDesc);
        privateDeleteFile(name);

        maybeDeletePendingFiles();
    }

    @Override
    public synchronized Set<String> getPendingDeletions() {
        ensureOpen();
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

    private synchronized void privateDeleteFile(String indexFileName) {
        var cache = SharedOpenFilesCache.getInstance();
        pendingDeletes.compute(indexFileName, (k, fileDesc) -> {
            if (fileDesc == null) {
                return null;
            }

            try {
                if (cache != null) {
                    cache.removeFile(fileDesc.filePath().toFile());
                }

                Files.deleteIfExists(fileDesc.filePath());

                if (cipherKey != null) {
                    Files.deleteIfExists(fileDesc.ivFilePath());
                }

                return null;
            } catch (IOException ioe) {
                // On windows, a file delete can fail because there's still an open
                // file handle against it.  We record this in pendingDeletes and
                // try again later.
                return fileDesc;
            }
        });
    }


    @Override
    public int getIdentity() {
        return identity;
    }

    @Override
    public byte[] readPage(long pageAddress, long fileAddress) {
        var filesCache = SharedOpenFilesCache.getInstance();
        final var fileDesc = fileNameRegistry.lookupByAddress(fileAddress);
        var pageOffset = pageAddress - fileAddress;

        var page = new byte[pageSize];

        int dataRead;
        try (var file = filesCache.getCachedFile(fileDesc.filePath().toFile())) {
            dataRead = DirUtil.readFully(file, pageOffset, page);
        } catch (IOException e) {
            throw new ExodusException("Can not access file " + fileDesc.filePath(), e);
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
        var fileDesc = fileNameRegistry.lookupByIndexName(name);
        var fileSize = Files.size(fileDesc.filePath());

        return new XodusIndexInput(
                "XodusIndexInput(path=\"" + fileDesc.filePath() + "\")",
                fileDesc.address(), cipherKey == null ? 0 : Long.BYTES, fileSize
        );
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

        final Path outputFilePath;
        final boolean storeivFile;

        final String indexName;

        boolean closed;

        final OutputStream os;

        XodusIndexOutput(String outputFileName, String indexName, boolean storeivFile, OutputStream stream) {
            // todo: should we really pass outputFileName as "name" here?
            super("XodusIndexOutput(path=\"" + luceneOutputPath.resolve(outputFileName) + "\")", outputFileName,
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

            outputFilePath = luceneOutputPath.resolve(outputFileName);
            this.storeivFile = storeivFile;

            this.indexName = indexName;
            this.os = stream;
        }


        @Override
        public String getName() {
            return indexName;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            super.close();

            var fileAddress = occupyNextFileAddress(outputFilePath);
            var fileDesc = fileNameRegistry.createFileDescription(indexName, fileAddress);

            try {
                Files.move(outputFilePath, fileDesc.filePath(), StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(outputFilePath, fileDesc.filePath());
            }

            if (storeivFile) {
                try (var ivStream = new DataOutputStream(Files.newOutputStream(fileDesc.filePath()))) {
                    ivStream.writeLong(((StreamCipherOutputStream) os).maxIv);
                }
            }

            fileNameRegistry.register(fileDesc);

            closed = true;
        }


    }

    private long occupyNextFileAddress(Path filePath) throws IOException {
        var fileLength = Files.size(filePath);
        var pages = (fileLength + pageSize - 1) / pageSize;
        if (pages == 0) {
            pages = 1;
        }

        return nextAddress.getAndAdd(pages * pageSize);
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
                this.page = sharedLogCache.getPage(XodusNonXodusDirectory.this, pageAddress, fileAddress
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
                    long i = 0;
                    for (int shift : SHIFTS) {
                        byte b = page[pageOffset++];
                        positionOffset++;
                        i |= (b & 0x7FL) << shift;
                        if (b >= 0) {
                            return i;
                        }
                    }
                    throw new IOException("Invalid vLong detected (negative values disallowed)");
                } finally {
                    movePosition(positionOffset);
                }

//                throw new IOException("Invalid vLong detected (negative values disallowed)");
            } else {
                return super.readVLong();
            }
        }

        @Override
        public void readLongs(long[] dst, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, dst.length);

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
                super.readLongs(dst, offset, length);
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
        if (closed) {
            throw new AlreadyClosedException("Directory " + rootPath + " already closed");
        }
    }
}
