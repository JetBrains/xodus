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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedureThrows;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


public class FileSystemBlobVaultOld extends BlobVault implements DiskBasedBlobVault {
    protected static final Logger logger = LoggerFactory.getLogger("FileSystemBlobVault");

    @NonNls
    public static final String VERSION_FILE = "version";

    private static final int EXPECTED_VERSION = 0;
    private static final long UNKNOWN_SIZE = -1L;

    @NonNls
    private final String blobsDirectory;
    @NonNls
    private final String blobExtension;
    private final Path location;
    private final BlobHandleGenerator blobHandleGenerator;
    private final int version;
    @Nullable
    private VaultSizeFunctions sizeFunctions;

    private final Path tmpBlobsDir;


    /**
     * Blob size is calculated by inspecting directory files only on first request
     * After that, it's been calculated incrementally
     */
    private final AtomicLong size;

    FileSystemBlobVaultOld(@NotNull final PersistentEntityStoreConfig config,
                           @NotNull final Path location,
                           @NotNull final String blobExtension,
                           @NotNull final BlobHandleGenerator blobHandleGenerator) throws IOException {
        this(config, location, blobExtension, blobHandleGenerator, EXPECTED_VERSION);
    }

    FileSystemBlobVaultOld(@NotNull final PersistentEntityStoreConfig config,
                           final @NotNull Path location,
                           @NotNull final String blobExtension,
                           @NotNull final BlobHandleGenerator blobHandleGenerator,
                           final int expectedVersion) throws IOException {
        super(config);
        this.blobExtension = blobExtension;
        this.location = location;
        this.blobsDirectory = location.getFileName().toString();

        this.blobHandleGenerator = blobHandleGenerator;
        size = new AtomicLong(UNKNOWN_SIZE);
        Files.createDirectories(this.location);
        // load version
        final Path versionFile = location.resolve(VERSION_FILE);

        if (Files.exists(versionFile)) {
            try (DataInputStream input = new DataInputStream(Files.newInputStream(versionFile))) {
                version = input.readInt();
            }
            if (expectedVersion != version) {
                throw new UnexpectedBlobVaultVersionException("Unexpected FileSystemBlobVault version: " + version);
            }
        } else {
            if (isEmptyDir(this.location.toFile())) {
                version = expectedVersion;
            } else {
                version = EXPECTED_VERSION;
                if (expectedVersion != version) {
                    throw new UnexpectedBlobVaultVersionException("Unexpected FileSystemBlobVault version: " + version);
                }
            }
            try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(versionFile))) {
                output.writeInt(expectedVersion);
            }
        }

        tmpBlobsDir = location.resolve("blob-tmp-dir");
        if (Files.exists(tmpBlobsDir)) {
            IOUtil.deleteRecursively(tmpBlobsDir.toFile());
        }

        Files.createDirectories(tmpBlobsDir);
    }

    @Override
    @NotNull
    public BlobVaultItem getBlob(long blobHandle) {
        return new FileBasedBlobValueItem(getBlobLocation(blobHandle), blobHandle);
    }

    public File getVaultLocation() {
        return location.toFile();
    }

    public int getVersion() {
        return version;
    }

    @Override
    public long nextHandle(@NotNull final Transaction txn) {
        return blobHandleGenerator.nextHandle(txn);
    }

    private void setContent(final long blobHandle, @NotNull final InputStream content) throws Exception {
        final File location = getBlobLocation(blobHandle, false);
        setContentImpl(content, location);
        if (size.get() != UNKNOWN_SIZE) {
            size.addAndGet(IOUtil.getAdjustedFileLength(location));
        }
    }

    private void setContent(final long blobHandle, @NotNull final Path file) throws Exception {
        final File location = getBlobLocation(blobHandle, false);
        try {
            Files.move(file, location.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(file, location.toPath());
        }

        if (size.get() != UNKNOWN_SIZE) {
            size.addAndGet(IOUtil.getAdjustedFileLength(location));
        }
    }

    @Override
    @Nullable
    public InputStream getContent(final long blobHandle, @NotNull final Transaction txn, @Nullable Long expectedLength) {
        try {
            var location = getBlobLocation(blobHandle);
            if (expectedLength != null && location.length() != expectedLength.longValue()) {
                return null;
            }

            return new FileInputStream(location);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public long getSize(long blobHandle, @NotNull Transaction txn) {
        return sizeFunctions == null ? getBlobLocation(blobHandle).length() : sizeFunctions.getBlobSize(blobHandle, txn);
    }

    public boolean delete(final long blobHandle) {
        final File file = getBlobLocation(blobHandle);
        if (file.exists()) {
            if (size.get() != UNKNOWN_SIZE) {
                size.addAndGet(-IOUtil.getAdjustedFileLength(file));
            }
            return deleteRecursively(file);
        }
        return true;
    }

    public boolean requiresTxn() {
        return false;
    }

    @Override
    public void flushBlobs(@Nullable final LongHashMap<InputStream> blobStreams,
                           final @Nullable LongHashMap<Path> blobFiles,
                           @Nullable LongHashMap<Path> tmpBlobFiles, @Nullable final LongSet deferredBlobsToDelete,
                           @NotNull final Transaction txn) throws Exception {
        if (blobStreams != null) {
            blobStreams.forEachEntry((ObjectProcedureThrows<Map.Entry<Long, InputStream>, Exception>) object -> {
                setContent(object.getKey(), object.getValue());
                return true;
            });
        }

        // if there were blob files then move them
        if (blobFiles != null) {
            blobFiles.forEachEntry((ObjectProcedureThrows<Map.Entry<Long, Path>, Exception>) object -> {
                setContent(object.getKey(), object.getValue());
                return true;
            });
        }

        if (tmpBlobFiles != null) {
            tmpBlobFiles.forEachEntry((ObjectProcedureThrows<Map.Entry<Long, Path>, Exception>) object -> {
                setContent(object.getKey(), object.getValue());
                return true;
            });
        }

        // if there are deferred blobs to delete then defer their deletion
        if (deferredBlobsToDelete != null) {
            final LongArrayList copy = new LongArrayList(deferredBlobsToDelete.size());
            final LongIterator it = deferredBlobsToDelete.iterator();
            while (it.hasNext()) {
                copy.add(it.nextLong());
            }
            final Environment environment = txn.getEnvironment();
            environment.executeTransactionSafeTask(() -> DeferredIO.getJobProcessor().queueIn(new Job() {
                @Override
                protected void execute() {
                    final long[] blobHandles = copy.getInstantArray();
                    for (int i = 0; i < copy.size(); ++i) {
                        delete(blobHandles[i]);
                    }
                }

                @Override
                public String getName() {
                    return "Delete obsolete blob files";
                }

                @Override
                public String getGroup() {
                    return environment.getLocation();
                }
            }, environment.getEnvironmentConfig().getGcFilesDeletionDelay()));
        }
    }

    @Override
    public long size() {
        long result = size.get();
        if (result == UNKNOWN_SIZE) {
            result = sizeFunctions == null ? calculateBlobVaultSize() : sizeFunctions.getBlobVaultSize();
            size.set(result);
        }
        return result;
    }

    @Override
    public void clear() {
        IOUtil.deleteRecursively(location.toFile());
    }

    @Override
    public void close() {
        IOUtil.deleteRecursively(tmpBlobsDir.toFile());
    }

    @NotNull
    @Override
    public BackupStrategy getBackupStrategy() {
        return new BackupStrategy() {

            @Override
            public Iterable<VirtualFileDescriptor> getContents() {
                return () -> {
                    final Deque<FileDescriptor> queue = new LinkedList<>();
                    queue.add(new FileDescriptor(location.toFile(), blobsDirectory + File.separator));
                    return new Iterator<VirtualFileDescriptor>() {
                        int i = 0;
                        int n = 0;
                        File[] files;
                        FileDescriptor next;
                        String currentPrefix;

                        @Override
                        public boolean hasNext() {
                            if (next != null) {
                                return true;
                            }
                            while (i < n) {
                                final File file = files[i++];
                                final String name = file.getName();
                                if (file.isDirectory()) {
                                    queue.push(new FileDescriptor(file, currentPrefix + file.getName() + File.separator));
                                } else if (file.isFile()) {
                                    final long fileSize = file.length();
                                    if (fileSize == 0) continue;
                                    if (name.endsWith(blobExtension)) {
                                        next = new FileDescriptor(file, currentPrefix, fileSize);
                                        return true;
                                    } else if (name.equalsIgnoreCase(VERSION_FILE)) {
                                        next = new FileDescriptor(file, currentPrefix, fileSize, false);
                                        return true;
                                    }
                                } else if (file.exists()) {
                                    // something strange with filesystem
                                    throw new EntityStoreException("File or directory expected: " + file);
                                }
                            }
                            if (queue.isEmpty()) {
                                return false;
                            }
                            final FileDescriptor fd = queue.pop();
                            files = IOUtil.listFiles(fd.getFile());
                            currentPrefix = fd.getPath();
                            i = 0;
                            n = files.length;
                            next = fd;
                            return true;
                        }

                        @Override
                        public FileDescriptor next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            final FileDescriptor result = next;
                            next = null;
                            return result;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                };
            }
        };
    }

    @NotNull
    public File getBlobLocation(long blobHandle) {
        return getBlobLocation(blobHandle, true);
    }

    @Override
    public String getBlobKey(long blobHandle) {
        return blobsDirectory + super.getBlobKey(blobHandle) + blobExtension;
    }

    @Override
    public @NotNull BufferedInputStream copyToTemporaryStore(long handle, @NotNull final InputStream stream,
                                                             @Nullable StoreTransaction transaction) throws IOException {
        Path tempFile = Files.createTempFile(tmpBlobsDir, "xodus", "tmp-blob");

        try (var out = Files.newOutputStream(tempFile)) {
            IOUtil.copyStreams(stream, out, IOUtil.getBUFFER_ALLOCATOR());
        }
        stream.close();

        return new TmpBlobVaultBufferedInputStream(Files.newInputStream(tempFile), tempFile, handle,
                (PersistentStoreTransaction) transaction);
    }

    @Override
    public @NotNull InputStream openTmpStream(long handle, @NotNull Path path) throws IOException {
        return new BufferedInputStream(Files.newInputStream(path));
    }

    File getTmpDirLocation() {
        return tmpBlobsDir.toFile();
    }

    @NotNull
    public File getBlobLocation(long blobHandle, boolean readonly) {
        File dir = location.toFile();
        String file;
        while (true) {
            file = Integer.toHexString((int) (blobHandle & 0xff));
            if (blobHandle <= 0xff) {
                break;
            }
            dir = new File(dir, file);
            blobHandle >>= 8;
        }
        if (!readonly) {
            //noinspection ResultOfMethodCallIgnored
            dir.mkdirs();
        }
        final String path = file + blobExtension;
        final File result = new File(dir, path);
        if (!readonly && result.exists()) {
            throw new EntityStoreException("Can't update existing blob file: " + result);
        }
        return result;
    }

    void setSizeFunctions(@Nullable final VaultSizeFunctions sizeFunction) {
        this.sizeFunctions = sizeFunction;
    }

    final String getBlobExtension() {
        return blobExtension;
    }

    private void setContentImpl(@NotNull final InputStream content,
                                @NotNull final File location) throws IOException {
        try (OutputStream blobOutput = new BufferedOutputStream(Files.newOutputStream(location.toPath()))) {
            IOUtil.copyStreams(content, blobOutput, bufferAllocator);
        }

        content.close();
    }

    private long calculateBlobVaultSize() {
        return IOUtil.getDirectorySize(location.toFile(), blobExtension, true);
    }

    private boolean deleteRecursively(@NotNull final File file) {
        if (!file.delete()) {
            file.deleteOnExit();
            return false;
        }
        final File dir = file.getParentFile();
        if (dir != null && location.toFile().compareTo(dir) != 0 && isEmptyDir(dir)) {
            deleteRecursively(dir);
        }
        return true;
    }

    private static boolean isEmptyDir(@NotNull final File dir) {
        final File[] files = dir.listFiles();
        return files != null && files.length == 0;
    }
}
