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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.DeferredIO;
import jetbrains.exodus.util.IOUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class FileSystemBlobVaultOld extends BlobVault {

    protected static final Log log = LogFactory.getLog("FileSystemBlobVault");

    @NonNls
    private static final String VERSION_FILE = "version";
    private static final int EXPECTED_VERSION = 0;
    private static final long UNKNOWN_SIZE = -1L;

    @NonNls
    private final String blobsDirectory;
    @NonNls
    private final String blobExtension;
    private final File location;
    private final BlobHandleGenerator blobHandleGenerator;
    private final int version;

    /**
     * Blob size is calculated by inspecting directory files only on first request
     * After that, it's been calculated incrementally
     */
    private final AtomicLong size;

    public FileSystemBlobVaultOld(@NotNull final String parentDirectory,
                                  @NotNull final String blobsDirectory,
                                  @NotNull final String blobExtension,
                                  @NotNull final BlobHandleGenerator blobHandleGenerator) throws IOException {
        this(parentDirectory, blobsDirectory, blobExtension, blobHandleGenerator, EXPECTED_VERSION);
    }

    protected FileSystemBlobVaultOld(@NotNull final String parentDirectory,
                                     @NotNull final String blobsDirectory,
                                     @NotNull final String blobExtension,
                                     @NotNull final BlobHandleGenerator blobHandleGenerator,
                                     final int expectedVersion) throws IOException {
        this.blobsDirectory = blobsDirectory;
        this.blobExtension = blobExtension;
        location = new File(parentDirectory, blobsDirectory);
        this.blobHandleGenerator = blobHandleGenerator;
        size = new AtomicLong(UNKNOWN_SIZE);
        //noinspection ResultOfMethodCallIgnored
        location.mkdirs();
        // load version
        final File versionFile = new File(location, VERSION_FILE);
        if (versionFile.exists()) {
            final DataInputStream input = new DataInputStream(new FileInputStream(versionFile));
            try {
                version = input.readInt();
            } finally {
                input.close();
            }
            if (expectedVersion != version) {
                throw new UnexpectedBlobVaultVersionException("Unexpected FileSystemBlobVault version: " + version);
            }
        } else {
            final File[] files = location.listFiles();
            final boolean hasFiles = files != null && files.length > 0;
            if (!hasFiles) {
                version = expectedVersion;
            } else {
                version = EXPECTED_VERSION;
                if (expectedVersion != version) {
                    throw new UnexpectedBlobVaultVersionException("Unexpected FileSystemBlobVault version: " + version);
                }
            }
            final DataOutputStream output = new DataOutputStream(new FileOutputStream(versionFile));
            try {
                output.writeInt(expectedVersion);
            } finally {
                output.close();
            }
        }
    }

    public File getVaultLocation() {
        return location;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public long generateNextHandle(@NotNull final Transaction txn) {
        return blobHandleGenerator.generateNextHandle(txn);
    }

    public void setContent(final long blobHandle, @NotNull final InputStream content) throws Exception {
        final File location = getBlobLocation(blobHandle, false);
        setContentImpl(content, location);
        if (size.get() != UNKNOWN_SIZE) {
            size.addAndGet(IOUtil.getAdjustedFileLength(location));
        }
    }

    public void setContent(final long blobHandle, @NotNull final File file) throws Exception {
        final File location = getBlobLocation(blobHandle, false);
        if (!file.renameTo(location)) {
            final FileInputStream content = new FileInputStream(file);
            try {
                setContentImpl(content, location);
            } finally {
                content.close();
            }
        }
        if (size.get() != UNKNOWN_SIZE) {
            size.addAndGet(IOUtil.getAdjustedFileLength(location));
        }
    }

    @Override
    @Nullable
    public InputStream getContent(final long blobHandle, @NotNull final Transaction txn) {
        try {
            return new FileInputStream(getBlobLocation(blobHandle));
        } catch (FileNotFoundException e) {
            log.error("File not found", e);
            return null;
        }
    }

    @Override
    public long getSize(long blobHandle, @NotNull Transaction txn) {
        return getBlobLocation(blobHandle).length();
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
                           @Nullable final LongHashMap<File> blobFiles,
                           @Nullable final LongSet deferredBlobsToDelete,
                           @NotNull final Transaction txn) throws Exception {
        if (blobStreams != null) {
            blobStreams.forEachEntry(new ObjectProcedureThrows<Map.Entry<Long, InputStream>, Exception>() {
                @Override
                public boolean execute(final Map.Entry<Long, InputStream> object) throws Exception {
                    final InputStream stream = object.getValue();
                    stream.reset();
                    setContent(object.getKey(), stream);
                    return true;
                }
            });
        }
        // if there were blob files then move them
        if (blobFiles != null) {
            blobFiles.forEachEntry(new ObjectProcedureThrows<Map.Entry<Long, File>, Exception>() {
                @Override
                public boolean execute(final Map.Entry<Long, File> object) throws Exception {
                    setContent(object.getKey(), object.getValue());
                    return true;
                }
            });
        }
        // if there are deferred blobs to delete then defer their deletion
        if (deferredBlobsToDelete != null) {
            final LongSet copy = new LongHashSet();
            final LongIterator it = deferredBlobsToDelete.iterator();
            while (it.hasNext()) {
                copy.add(it.nextLong());
            }
            final Environment environment = txn.getEnvironment();
            environment.executeTransactionSafeTask(new Runnable() {
                @Override
                public void run() {
                    DeferredIO.getJobProcessor().queue(new Job() {
                        @Override
                        protected void execute() throws Throwable {
                            final LongIterator it = copy.iterator();
                            while (it.hasNext()) {
                                delete(it.nextLong());
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
                    });
                }
            });
        }
    }

    @Override
    public long size() {
        long result = size.get();
        if (result == UNKNOWN_SIZE) {
            result = calculateBlobSize();
            size.set(result);
        }
        return result;
    }

    @Override
    public void close() {
    }

    @Override
    public BackupStrategy getBackupStrategy() {
        return new BackupStrategy() {
            @Override
            public void beforeBackup() {
            }

            @Override
            public Iterable<Pair<File, String>> listFiles() {
                return new Iterable<Pair<File, String>>() {
                    @Override
                    public Iterator<Pair<File, String>> iterator() {
                        final Deque<Pair<File, String>> queue = new LinkedList<Pair<File, String>>();
                        queue.add(new Pair<File, String>(location, blobsDirectory + File.separator));
                        return new Iterator<Pair<File, String>>() {
                            int i = 0;
                            int n = 0;
                            File[] files;
                            Pair<File, String> nextPair;
                            String currentPrefix;

                            @Override
                            public boolean hasNext() {
                                if (nextPair != null) {
                                    return true;
                                }
                                while (i < n) {
                                    final File file = files[i++];
                                    final String name = file.getName();
                                    if (file.isDirectory()) {
                                        queue.push(new Pair<File, String>(file, currentPrefix + file.getName() + File.separator));
                                    } else if (file.isFile()) {
                                        if (file.length() == 0) continue;
                                        if (name.endsWith(blobExtension) || name.equalsIgnoreCase(VERSION_FILE)) {
                                            nextPair = new Pair<File, String>(file, currentPrefix);
                                            return true;
                                        }
                                    } else {
                                        // something strange with filesystem
                                        throw new RuntimeException("File or directory expected: " + file.toString());
                                    }
                                }
                                if (queue.isEmpty()) {
                                    return false;
                                }
                                final Pair<File, String> pair = queue.pop();
                                files = IOUtil.listFiles(pair.getFirst());
                                currentPrefix = pair.getSecond();
                                i = 0;
                                n = files.length;
                                nextPair = pair;
                                return true;
                            }

                            @Override
                            public Pair<File, String> next() {
                                if (!hasNext()) {
                                    throw new NoSuchElementException();
                                }
                                final Pair<File, String> result = nextPair;
                                nextPair = null;
                                return result;
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                };
            }

            @Override
            public void afterBackup() {
            }

            @Override
            public void onError(Throwable t) {
            }
        };
    }

    @NotNull
    public File getBlobLocation(long blobHandle) {
        return getBlobLocation(blobHandle, true);
    }

    @NotNull
    protected File getBlobLocation(long blobHandle, boolean readonly) {
        File dir = location;
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
        final File result = new File(dir, file + blobExtension);
        if (!readonly && result.exists()) {
            throw new EntityStoreException("Can't update existing blob file: " + result);
        }
        return result;
    }

    protected final String getBlobExtension() {
        return blobExtension;
    }

    private void setContentImpl(@NotNull final InputStream content,
                                @NotNull final File location) throws IOException {
        OutputStream blobOutput = null;
        try {
            blobOutput = new BufferedOutputStream(new FileOutputStream(location));
            IOUtil.copyStreams(content, blobOutput, bufferAllocator);
        } finally {
            if (blobOutput != null) {
                blobOutput.close();
            }
        }
    }

    private long calculateBlobSize() {
        return IOUtil.getDirectorySize(location, blobExtension, true);
    }

    private boolean deleteRecursively(@NotNull final File file) {
        if (!file.delete()) {
            file.deleteOnExit();
            return false;
        }
        final File dir = file.getParentFile();
        if (dir != null && location.compareTo(dir) != 0 && dir.listFiles().length == 0) {
            deleteRecursively(dir);
        }
        return true;
    }
}