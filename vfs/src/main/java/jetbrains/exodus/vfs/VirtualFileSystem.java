/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class VirtualFileSystem {

    /**
     * Names of stores used by VirtualFileSystem.
     */
    private static final String SETTINGS_STORE_NAME = "jetbrains.exodus.vfs.settings";
    private static final String PATHNAMES_STORE_NAME = "jetbrains.exodus.vfs.pathnames";
    private static final String CONTENTS_STORE_NAME = "jetbrains.exodus.vfs.contents";

    private final Environment env;
    private final VfsConfig config;
    private final VfsSettings settings;
    private final Store pathnames;
    private final Store contents;
    private final AtomicLong fileDescriptorSequence;

    public VirtualFileSystem(@NotNull final Environment env) {
        this(env, VfsConfig.DEFAULT);
    }

    public VirtualFileSystem(@NotNull final Environment env, @NotNull final VfsConfig config) {
        this(env, config, StoreConfig.WITHOUT_DUPLICATES);
    }

    public VirtualFileSystem(@NotNull final Environment env, @NotNull final VfsConfig config, @Nullable final Transaction txn) {
        this(env, config, StoreConfig.WITHOUT_DUPLICATES, txn);
    }

    public VirtualFileSystem(@NotNull final Environment env,
                             @NotNull final VfsConfig config,
                             @NotNull final StoreConfig contentsStoreConfig) {
        this(env, config, contentsStoreConfig, null);
    }

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

    public Environment getEnvironment() {
        return env;
    }

    @NotNull
    public File createFile(@NotNull final Transaction txn, @NotNull String path) {
        return doCreateFile(txn, fileDescriptorSequence.getAndIncrement(), path);
    }

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

    public File createUniqueFile(@NotNull final Transaction txn, @NotNull final String pathPrefix) {
        while (true) {
            try {
                return createFile(txn, pathPrefix + new Object().hashCode());
            } catch (FileExistsException ignored) {
            }
        }
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
     * Renames origin file to a specified new path. File contents is not affected.
     *
     * @param origin  origin file.
     * @param newPath new name of the file.
     * @return true if the file was renamed, otherwise a file with the newPath name already exists.
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
     * Deletes earlier created file.
     *
     * @param txn  mandatory transaction in which the file is deleted.
     * @param path path to the file.
     * @return file been deleted or null if no such file existed.
     */
    @Nullable
    public File deleteFile(@NotNull final Transaction txn, @NotNull final String path) {
        final ArrayByteIterable key = StringBinding.stringToEntry(path);
        try (Cursor cursor = pathnames.openCursor(txn)) {
            final ByteIterable value = cursor.getSearchKey(key);
            if (value != null) {
                final File result = new File(path, value);
                // at first delete contents
                final ClusterIterator iterator = new ClusterIterator(txn, result, contents);
                try {
                    while (iterator.hasCluster()) {
                        iterator.deleteCurrent();
                        iterator.moveToNext();
                    }
                } finally {
                    iterator.close();
                }
                cursor.deleteCurrent();
                return result;
            }
        }
        return null;
    }

    public long getNumberOfFiles(@NotNull final Transaction txn) {
        return pathnames.count(txn);
    }

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

    public long getFileLength(@NotNull final Transaction txn, @NotNull final File file) {
        return getFileLength(txn, file.getDescriptor());
    }

    public long getFileLength(@NotNull final Transaction txn, final long fileDescriptor) {
        long result = 0;
        final ClusterIterator it = new ClusterIterator(txn, fileDescriptor, contents);
        try {
            while (it.hasCluster()) {
                result += it.getCurrent().getSize();
                it.moveToNext();
            }
        } finally {
            it.close();
        }
        return result;
    }

    public VfsInputStream readFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsInputStream(this, txn, file.getDescriptor());
    }

    public VfsInputStream readFile(@NotNull final Transaction txn, final long fileDescriptor) {
        return new VfsInputStream(this, txn, fileDescriptor);
    }

    public VfsInputStream readFile(@NotNull final Transaction txn,
                                   @NotNull final File file,
                                   final long fromPosition) {
        return new VfsInputStream(this, txn, file.getDescriptor(), fromPosition);
    }

    public void touchFile(@NotNull final Transaction txn, @NotNull final File file) {
        new LastModifiedTrigger(txn, file, pathnames).run();
    }

    public OutputStream writeFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsOutputStream(this, txn, file.getDescriptor(), new LastModifiedTrigger(txn, file, pathnames));
    }

    public OutputStream writeFile(@NotNull final Transaction txn,
                                  @NotNull final File file,
                                  final long fromPosition) {
        return new VfsOutputStream(this, txn, file.getDescriptor(), fromPosition, new LastModifiedTrigger(txn, file, pathnames));
    }

    public OutputStream writeFile(@NotNull final Transaction txn, final long fileDescriptor) {
        return new VfsOutputStream(this, txn, fileDescriptor, null);
    }

    public OutputStream writeFile(@NotNull final Transaction txn, final long fileDescriptor, final long fromPosition) {
        return new VfsOutputStream(this, txn, fileDescriptor, fromPosition, null);
    }

    public OutputStream appendFile(@NotNull final Transaction txn, @NotNull final File file) {
        return new VfsAppendingStream(this, txn, file, new LastModifiedTrigger(txn, file, pathnames));
    }

    public void shutdown() {
        saveFileDescriptorSequence(null);
    }

    public VfsConfig getConfig() {
        return config;
    }

    Store getContents() {
        return contents;
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
