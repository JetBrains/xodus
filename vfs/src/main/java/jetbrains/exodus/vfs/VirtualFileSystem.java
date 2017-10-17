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
package jetbrains.exodus.vfs;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code VirtualFileSystem} allows to deal with data in terms of {@linkplain File files}, input and output streams.
 * {@code VirtualFileSystem} works over an {@linkplain Environment} instance, so {@code VirtualFileSystem} is
 * a transactional file system with strong isolation guarantees. Any {@code VirtualFileSystem} operation requires
 * a {@linkplain Transaction} instance. Even creation of {@code VirtualFileSystem} can be transactional (using
 * constructors {@linkplain #VirtualFileSystem(Environment, VfsConfig, Transaction)} and
 * {@linkplain #VirtualFileSystem(Environment, VfsConfig, StoreConfig, Transaction)}). An application working with
 * {@code VirtualFileSystem} should {@linkplain #shutdown()} file system before stopping itself.
 *
 * <p>To create a {@linkplain File file}, use {@linkplain #createFile(Transaction, long, String)} and {@linkplain
 * #createFile(Transaction, long, String)} methods. To create file with unique pathname and specified path prefix,
 * use {@linkplain #createUniqueFile(Transaction, String)}. To open existing file or to create the new file,
 * use {@linkplain #openFile(Transaction, String, boolean)}.
 *
 * To read {@linkplain File} contents, open {@linkplain java.io.InputStream} using {@linkplain #readFile(Transaction, File)},
 * {@linkplain #readFile(Transaction, File, long)} and {@linkplain #readFile(Transaction, long)} methods. To write
 * {@linkplain File} contents, open {@linkplain OutputStream} using {@linkplain #appendFile(Transaction, File)},
 * {@linkplain #writeFile(Transaction, File)}, {@linkplain #writeFile(Transaction, File, long)},
 * {@linkplain #writeFile(Transaction, long)} and {@linkplain #writeFile(Transaction, long, long)} methods.
 *
 * @see File
 * @see VfsConfig
 * @see Environment
 * @see Transaction
 */
public class VirtualFileSystem {

    private static final String SETTINGS_STORE_NAME = "jetbrains.exodus.vfs.settings";
    private static final String PATHNAMES_STORE_NAME = "jetbrains.exodus.vfs.pathnames";
    private static final String CONTENTS_STORE_NAME = "jetbrains.exodus.vfs.contents";

    private final Environment env;
    private final VfsConfig config;
    private final VfsSettings settings;
    private final Store pathnames;
    private final Store contents;
    private final AtomicLong fileDescriptorSequence;
    @Nullable
    private IOCancellingPolicyProvider cancellingPolicyProvider;
    @Nullable
    private ClusterConverter clusterConverter;

    /**
     * Creates {@code VirtualFileSystem} over specified {@linkplain Environment} with default settings
     * {@linkplain VfsConfig#DEFAULT}.
     *
     * @param env {@linkplain Environment} instance
     * @see Environment
     */
    public VirtualFileSystem(@NotNull final Environment env) {
        this(env, VfsConfig.DEFAULT);
    }

    /**
     * Creates {@code VirtualFileSystem} over specified {@linkplain Environment} with specified {@linkplain VfsConfig}.
     *
     * @param env    {@linkplain Environment} instance
     * @param config {@linkplain VfsConfig} instance
     * @see Environment
     * @see VfsConfig
     */
    public VirtualFileSystem(@NotNull final Environment env, @NotNull final VfsConfig config) {
        this(env, config, StoreConfig.WITHOUT_DUPLICATES);
    }

    /**
     * Creates {@code VirtualFileSystem} over specified {@linkplain Environment} with specified {@linkplain VfsConfig}
     * inside specified {@linkplain Transaction}.
     *
     * @param env    {@linkplain Environment} instance
     * @param config {@linkplain VfsConfig} instance
     * @param txn    {@linkplain Transaction} instance
     * @see Environment
     * @see VfsConfig
     * @see Transaction
     */
    public VirtualFileSystem(@NotNull final Environment env,
                             @NotNull final VfsConfig config,
                             @Nullable final Transaction txn) {
        this(env, config, StoreConfig.WITHOUT_DUPLICATES, txn);
    }

    /**
     * Creates {@code VirtualFileSystem} over specified {@linkplain Environment} with specified {@linkplain VfsConfig}
     * and {@linkplain StoreConfig}. {@code StoreConfig} is used to open the {@linkplain Store} for contents
     * of {@code VirtualFileSystem}'s {@linkplain File files}.
     *
     * @param env                 {@linkplain Environment} instance
     * @param config              {@linkplain VfsConfig} instance
     * @param contentsStoreConfig {@linkplain StoreConfig} instance
     * @see Environment
     * @see VfsConfig
     * @see StoreConfig
     */
    public VirtualFileSystem(@NotNull final Environment env,
                             @NotNull final VfsConfig config,
                             @NotNull final StoreConfig contentsStoreConfig) {
        this(env, config, contentsStoreConfig, null);
    }

    /**
     * Creates {@code VirtualFileSystem} over specified {@linkplain Environment} with specified {@linkplain VfsConfig}
     * and {@linkplain StoreConfig} inside specified {@linkplain Transaction}. {@code StoreConfig} is used to open
     * the {@linkplain Store} for contents of {@code VirtualFileSystem}'s {@linkplain File files}.
     *
     * @param env                 {@linkplain Environment} instance
     * @param config              {@linkplain VfsConfig} instance
     * @param contentsStoreConfig {@linkplain StoreConfig} instance
     * @param txn                 {@linkplain Transaction} instance
     * @see Environment
     * @see VfsConfig
     * @see StoreConfig
     * @see Transaction
     */
    public VirtualFileSystem(@NotNull final Environment env,
                             @NotNull final VfsConfig config,
                             @NotNull final StoreConfig contentsStoreConfig,
                             @Nullable final Transaction txn) {
        this.env = env;
        this.config = config;
        if (txn != null) {
            settings = new VfsSettings(env, env.openStore(
                SETTINGS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn));
            pathnames = env.openStore(
                PATHNAMES_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
            contents = env.openStore(CONTENTS_STORE_NAME, contentsStoreConfig, txn);
        } else {
            settings = env.computeInTransaction(new TransactionalComputable<VfsSettings>() {
                @Override
                public VfsSettings compute(@NotNull final Transaction txn) {
                    return new VfsSettings(env, env.openStore(
                        SETTINGS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn));
                }
            });
            pathnames = env.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull final Transaction txn) {
                    return env.openStore(PATHNAMES_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
                }
            });
            contents = env.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull final Transaction txn) {
                    return env.openStore(CONTENTS_STORE_NAME, contentsStoreConfig, txn);
                }
            });
        }
        fileDescriptorSequence = new AtomicLong();
        final ByteIterable bi = settings.get(txn, VfsSettings.NEXT_FREE_PATH_ID);
        if (bi != null) {
            fileDescriptorSequence.set(LongBinding.compressedEntryToLong(bi));
        }
    }

    /**
     * @return {@linkplain Environment} instance which the {@code VirtualFileSystem} works over.
     */
    public Environment getEnvironment() {
        return env;
    }

    /**
     * Creates new file inside specified {@linkplain Transaction} with specified path and returns
     * the {@linkplain File} instance.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param path file path
     * @return new {@linkplain File}
     * @throws FileExistsException if a {@linkplain File} with specified path already exists
     * @see #createFile(Transaction, long, String)
     * @see File
     */
    @NotNull
    public File createFile(@NotNull final Transaction txn, @NotNull String path) {
        return doCreateFile(txn, fileDescriptorSequence.getAndIncrement(), path);
    }

    /**
     * Creates new file inside specified {@linkplain Transaction} with specified file descriptor and path and returns
     * the {@linkplain File} instance.
     *
     * @param txn            {@linkplain Transaction} instance
     * @param fileDescriptor file descriptor
     * @param path           file path
     * @return new {@linkplain File}
     * @throws FileExistsException if a {@linkplain File} with specified path already exists
     * @see #createFile(Transaction, String)
     * @see File
     * @see File#getDescriptor()
     */
    @NotNull
    public File createFile(@NotNull final Transaction txn, final long fileDescriptor, @NotNull final String path) {
        while (true) {
            long current = fileDescriptorSequence.get();
            long next = Math.max(fileDescriptor + 1, current);
            if (fileDescriptorSequence.compareAndSet(current, next))
                break;
        }
        return doCreateFile(txn, fileDescriptor, path);
    }

    /**
     * Creates new {@linkplain File} with unique auto-generated path starting with specified {@code pathPrefix}.
     *
     * @param txn        {@linkplain Transaction} instance
     * @param pathPrefix prefix which the path of the {@linkplain File result} will start from
     * @return new {@linkplain File}
     * @see File
     * @see File#getPath()
     */
    public File createUniqueFile(@NotNull final Transaction txn, @NotNull final String pathPrefix) {
        while (true) {
            try {
                return createFile(txn, pathPrefix + new Object().hashCode());
            } catch (FileExistsException ignored) {
            }
        }
    }

    /**
     * Returns existing {@linkplain File} with specified path or creates the new one if {@code create} is {@code true},
     * otherwise returns {@code null}. If {@code create} is {@code true} it never returns {@code null}.
     *
     * @param txn    {@linkplain Transaction} instance
     * @param path   file path
     * @param create {@code true} if new file creation is allowed
     * @return existing or newly created {@linkplain File} if if {@code create} is {@code true}, or {@code null}
     * @see File
     */
    @Nullable
    public File openFile(@NotNull final Transaction txn, @NotNull final String path, boolean create) {
        final ArrayByteIterable key = StringBinding.stringToEntry(path);
        final ByteIterable value = pathnames.get(txn, key);
        if (value != null) {
            return new File(path, value);
        }
        if (create) {
            return createFile(txn, path);
        }
        return null;
    }

    /**
     * Renames {@code origin} file to the specified {@code newPath} and returns {@code true} if the file was actually
     * renamed. Otherwise another file with the path {@code newPath} exists. File contents and file descriptor are
     * not affected.
     *
     * @param txn     {@linkplain Transaction} instance
     * @param origin  origin {@linkplain File}
     * @param newPath new name of the file
     * @return {@code true} if the file was actually renamed, otherwise another file with the path {@code newPath} exists
     * @see File
     * @see File#getDescriptor()
     */
    public boolean renameFile(@NotNull final Transaction txn, @NotNull final File origin, @NotNull final String newPath) {
        final ArrayByteIterable key = StringBinding.stringToEntry(newPath);
        final ByteIterable value = pathnames.get(txn, key);
        if (value != null) {
            return false;
        }
        final File newFile = new File(newPath, origin.getDescriptor(), origin.getCreated(), System.currentTimeMillis());
        pathnames.put(txn, key, newFile.toByteIterable());
        pathnames.delete(txn, StringBinding.stringToEntry(origin.getPath()));
        return true;
    }

    /**
     * Deletes existing file with the specified {@code path}.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param path file path
     * @return deleted {@linkplain File} or {@code null} if no file with specified {@code path}exists.
     * @see File
     */
    @Nullable
    public File deleteFile(@NotNull final Transaction txn, @NotNull final String path) {
        final ArrayByteIterable key = StringBinding.stringToEntry(path);
        final ByteIterable fileMetadata;
        try (Cursor cursor = pathnames.openCursor(txn)) {
            fileMetadata = cursor.getSearchKey(key);
            if (fileMetadata != null) {
                cursor.deleteCurrent();
            }
        }
        if (fileMetadata != null) {
            final File result = new File(path, fileMetadata);
            // at first delete contents
            try (ClusterIterator iterator = new ClusterIterator(this, txn, result)) {
                while (iterator.hasCluster()) {
                    iterator.deleteCurrent();
                    iterator.moveToNext();
                }
            }
            return result;
        }
        return null;
    }

    /**
     * @param txn {@linkplain Transaction} instance
     * @return total number of files in the {@code VirtualFileSystem}
     */
    public long getNumberOfFiles(@NotNull final Transaction txn) {
        return pathnames.count(txn);
    }

    /**
     * @param txn {@linkplain Transaction} instance
     * @return {@linkplain Iterable} to iterate over all the {@linkplain File files} in the {@code VirtualFileSystem}
     * @see File
     */
    @NotNull
    public Iterable<File> getFiles(@NotNull final Transaction txn) {
        try (Cursor cursor = pathnames.openCursor(txn)) {
            return new Iterable<File>() {
                @Override
                public Iterator<File> iterator() {
                    return new Iterator<File>() {
                        @Override
                        public boolean hasNext() {
                            return cursor.getNext();
                        }

                        @Override
                        public File next() {
                            return new File(StringBinding.entryToString(cursor.getKey()), cursor.getValue());
                        }

                        @Override
                        public void remove() {
                            deleteFile(txn, StringBinding.entryToString(cursor.getKey()));
                        }
                    };
                }
            };
        }
    }

    /**
     * @param txn  {@linkplain Transaction} instance
     * @param file {@linkplain File} instance
     * @return length of specified {@linkplain File file} in bytes
     * @see File
     */
    public long getFileLength(@NotNull final Transaction txn, @NotNull final File file) {
        return getFileLength(txn, file.getDescriptor());
    }

    /**
     * @param txn            {@linkplain Transaction} instance
     * @param fileDescriptor file descriptor
     * @return length of specified file in bytes
     * @see File
     * @see File#getDescriptor()
     */
    public long getFileLength(@NotNull final Transaction txn, final long fileDescriptor) {
        // todo: compute length without traversing all clusters at least in case of linear clustering strategy
        long result = 0;
        try (ClusterIterator it = new ClusterIterator(this, txn, fileDescriptor)) {
            // if clustering strategy is linear we can avoid reading cluster size for each cluster
            final ClusteringStrategy cs = config.getClusteringStrategy();
            if (cs.isLinear()) {
                Cluster previous = null;
                while (it.hasCluster()) {
                    if (previous != null) {
                        result += cs.getFirstClusterSize();
                    }
                    previous = it.getCurrent();
                    it.moveToNext();
                }
                if (previous != null) {
                    result += previous.getSize();
                }
            } else {
                while (it.hasCluster()) {
                    result += it.getCurrent().getSize();
                    it.moveToNext();
                }
            }
        }
        return result;
    }

    /**
     * Returns {@linkplain InputStream} to read contents of the specified file from the beginning.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param file {@linkplain File} instance
     * @return {@linkplain java.io.InputStream} to read contents of the specified file from the beginning
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     */
    public VfsInputStream readFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsInputStream(this, txn, file.getDescriptor());
    }

    /**
     * Returns {@linkplain InputStream} to read contents of the specified file from the beginning.
     *
     * @param txn            {@linkplain Transaction} instance
     * @param fileDescriptor file descriptor
     * @return {@linkplain java.io.InputStream} to read contents of the specified file from the beginning
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     * @see File#getDescriptor()
     */
    public VfsInputStream readFile(@NotNull final Transaction txn, final long fileDescriptor) {
        return new VfsInputStream(this, txn, fileDescriptor);
    }

    /**
     * Returns {@linkplain InputStream} to read contents of the specified file from the specified position.
     *
     * @param txn          {@linkplain Transaction} instance
     * @param file         {@linkplain File} instance
     * @param fromPosition file position to read from
     * @return {@linkplain java.io.InputStream} to read contents of the specified file from the specified position
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     */
    public VfsInputStream readFile(@NotNull final Transaction txn,
                                   @NotNull final File file,
                                   final long fromPosition) {
        return new VfsInputStream(this, txn, file.getDescriptor(), fromPosition);
    }

    /**
     * Touches the specified {@linkplain File}, i.e. sets its {@linkplain File#getLastModified() last modified time} to
     * current time.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param file {@linkplain File} instance
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see File#getLastModified()
     */
    public void touchFile(@NotNull final Transaction txn, @NotNull final File file) {
        new LastModifiedTrigger(txn, file, pathnames).run();
    }

    /**
     * Returns {@linkplain OutputStream} to write the contents of the specified file from the beginning.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param file {@linkplain File} instance
     * @return {@linkplain OutputStream} to write the contents of the specified file from the beginning
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     */
    public OutputStream writeFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsOutputStream(this, txn, file.getDescriptor(), new LastModifiedTrigger(txn, file, pathnames));
    }

    /**
     * Returns {@linkplain OutputStream} to write the contents of the specified file from the specified position.
     * If the position is greater than the file length then the method returns the same stream as
     * {@linkplain #appendFile(Transaction, File)} does.
     *
     * @param txn          {@linkplain Transaction} instance
     * @param file         {@linkplain File} instance
     * @param fromPosition file position to write from
     * @return {@linkplain OutputStream} to write the contents of the specified file from the specified position
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     */
    public OutputStream writeFile(@NotNull final Transaction txn,
                                  @NotNull final File file,
                                  final long fromPosition) {
        return new VfsOutputStream(this, txn, file.getDescriptor(), fromPosition, new LastModifiedTrigger(txn, file, pathnames));
    }

    /**
     * Returns {@linkplain OutputStream} to write the contents of the specified file from the beginning. Writing to
     * the returned stream doesn't change the {@linkplain File}'s last modified time.
     *
     * @param txn            {@linkplain Transaction} instance
     * @param fileDescriptor file descriptor
     * @return {@linkplain OutputStream} to write the contents of the specified file from the beginning
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     * @see File#getDescriptor()
     * @see File#getLastModified()
     */
    public OutputStream writeFile(@NotNull final Transaction txn, final long fileDescriptor) {
        return new VfsOutputStream(this, txn, fileDescriptor, null);
    }

    /**
     * Returns {@linkplain OutputStream} to write the contents of the specified file from the specified position.
     * If the position is greater than the file length then the method returns the same stream as
     * {@linkplain #appendFile(Transaction, File)} does. Writing to the returned stream doesn't change the
     * {@linkplain File}'s last modified time.
     *
     * @param txn            {@linkplain Transaction} instance
     * @param fileDescriptor file descriptor
     * @param fromPosition   file position to write from
     * @return {@linkplain OutputStream} to write the contents of the specified file from the specified position
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #appendFile(Transaction, File)
     * @see #touchFile(Transaction, File)
     * @see File#getDescriptor()
     * @see File#getLastModified()
     */
    public OutputStream writeFile(@NotNull final Transaction txn, final long fileDescriptor, final long fromPosition) {
        return new VfsOutputStream(this, txn, fileDescriptor, fromPosition, null);
    }

    /**
     * Returns {@linkplain OutputStream} to write the contents of the specified file from the end of the file. If the
     * file is empty the contents is written from the beginning.
     *
     * @param txn  {@linkplain Transaction} instance
     * @param file {@linkplain File} instance
     * @return @linkplain OutputStream} to write the contents of the specified file from the end of the file
     * @see #readFile(Transaction, File)
     * @see #readFile(Transaction, long)
     * @see #readFile(Transaction, File, long)
     * @see #writeFile(Transaction, File)
     * @see #writeFile(Transaction, long)
     * @see #writeFile(Transaction, File, long)
     * @see #writeFile(Transaction, long, long)
     * @see #touchFile(Transaction, File)
     */
    public OutputStream appendFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsAppendingStream(this, txn, file, new LastModifiedTrigger(txn, file, pathnames));
    }

    /**
     * Shuts down the {@code VirtualFileSystem}.
     */
    public void shutdown() {
        saveFileDescriptorSequence(null);
    }

    /**
     * @return {@linkplain VfsConfig} used to create the {@code VirtualFileSystem}
     */
    public VfsConfig getConfig() {
        return config;
    }

    @Nullable
    public IOCancellingPolicyProvider getCancellingPolicyProvider() {
        return cancellingPolicyProvider;
    }

    public void setCancellingPolicyProvider(@NotNull final IOCancellingPolicyProvider cancellingPolicyProvider) {
        this.cancellingPolicyProvider = cancellingPolicyProvider;
    }

    @Nullable
    public ClusterConverter getClusterConverter() {
        return clusterConverter;
    }

    public void setClusterConverter(@Nullable final ClusterConverter clusterConverter) {
        this.clusterConverter = clusterConverter;
    }

    public void dump(@NotNull final Transaction txn, @NotNull final Path directory) throws IOException {
        for (final File file : getFiles(txn)) {
            try (InputStream content = readFile(txn, file)) {
                Files.copy(content, Paths.get(directory.toString(), file.getPath()));
            }
        }
    }

    Store getContents() {
        return contents;
    }

    private File doCreateFile(@NotNull final Transaction txn, final long fileDescriptor, @NotNull String path) {
        path = String.format(path, fileDescriptor);
        final ArrayByteIterable key = StringBinding.stringToEntry(path);
        final ByteIterable value = pathnames.get(txn, key);
        if (value != null) {
            throw new FileExistsException(path);
        }
        final long currentTime = System.currentTimeMillis();
        final File result = new File(path, fileDescriptor, currentTime, currentTime);
        pathnames.put(txn, key, result.toByteIterable());
        saveFileDescriptorSequence(txn);
        return result;
    }

    private void saveFileDescriptorSequence(@Nullable final Transaction txn) {
        settings.put(txn, VfsSettings.NEXT_FREE_PATH_ID, LongBinding.longToCompressedEntry(fileDescriptorSequence.get()));
    }

    private static class LastModifiedTrigger implements Runnable {

        private final File file;
        private final Transaction txn;
        private final Store pathnames;

        private LastModifiedTrigger(@NotNull final Transaction txn, @NotNull final File file,
                                    @NotNull final Store pathnames) {
            this.file = file;
            this.txn = txn;
            this.pathnames = pathnames;
        }

        @Override
        public void run() {
            final File modified = new File(file);
            final ArrayByteIterable key = StringBinding.stringToEntry(modified.getPath());
            pathnames.put(txn, key, modified.toByteIterable());
        }
    }
}
