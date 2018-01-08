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
package jetbrains.exodus.backup;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;

/**
 * Describes how a backup file should be cooked by {@link BackupBean}. Only listed by
 * {@link #getContents()} and accepted by {@link #acceptFile(VirtualFileDescriptor)} files are put into backup file with defined
 * pre- ({@link #beforeBackup()}) and postprocessing ({@link #afterBackup()}).
 *
 * @see Backupable
 * @see BackupBean
 */
public abstract class BackupStrategy {

    public static final BackupStrategy EMPTY = new BackupStrategy() {

        @Override
        public Iterable<VirtualFileDescriptor> getContents() {
            return Collections.emptyList();
        }
    };

    /**
     * Backup pre-processing procedure. E.g., {@link jetbrains.exodus.env.Environment} turns database GC off before backup.
     *
     * @throws Exception if something went wrong
     */
    public void beforeBackup() throws Exception {
    }

    public Iterable<VirtualFileDescriptor> getContents() {
        final Iterable<FileDescriptor> contents = listFiles();
        return new Iterable<VirtualFileDescriptor>() {
            @NotNull
            @Override
            public Iterator<VirtualFileDescriptor> iterator() {
                final Iterator<FileDescriptor> sourceItr = contents.iterator();
                return new Iterator<VirtualFileDescriptor>() {
                    @Override
                    public boolean hasNext() {
                        return sourceItr.hasNext();
                    }

                    @Override
                    public VirtualFileDescriptor next() {
                        return sourceItr.next();
                    }

                    @Override
                    public void remove() {
                        sourceItr.remove();
                    }
                };
            }
        };
    }

    @Deprecated
    public Iterable<FileDescriptor> listFiles() {
        return Collections.emptyList();
    }

    /**
     * Backup postprocessing procedure. E.g., {@link jetbrains.exodus.env.Environment} turns database GC on after backup.
     *
     * @throws Exception if something went wrong
     */
    public void afterBackup() throws Exception {
    }

    /**
     * Can be used to interrupt backup process. After each processed file, {@link BackupBean}
     * checks if backup procedure should be interrupted.
     *
     * @return true if backup should be interrupted.
     */
    public boolean isInterrupted() {
        return false;
    }

    /**
     * Override this method to define custom exception handling.
     *
     * @param t throwable thrown during backup.
     */
    public void onError(Throwable t) {
    }

    /**
     * @param file file to be backed up.
     * @return the length of specified file that should be put into backup, minimum is taken between the actual
     * file size and the value returned. If the method returns a less than one value the file won't be put into backup.
     */
    public long acceptFile(@NotNull VirtualFileDescriptor file) {
        return Long.MAX_VALUE;
    }

    /**
     * @param file file to be backed up.
     * @return ignored by API
     * @see #acceptFile(VirtualFileDescriptor)
     */
    @Deprecated
    public long acceptFile(@NotNull File file) {
        return Long.MAX_VALUE;
    }

    /**
     * Descriptor of a file to be put into backup file.
     */
    public static class FileDescriptor implements VirtualFileDescriptor {

        @NotNull
        private final File file;
        @NotNull
        private final String path;
        private final long fileSize;
        private final boolean canBeEncrypted;

        public FileDescriptor(@NotNull final File file, @NotNull final String path, final long fileSize, final boolean canBeEncrypted) {
            this.file = file;
            this.path = path;
            this.fileSize = fileSize;
            this.canBeEncrypted = canBeEncrypted;
        }

        public FileDescriptor(@NotNull final File file, @NotNull final String path, final long fileSize) {
            this(file, path, fileSize, true);
        }

        public FileDescriptor(@NotNull final File file, @NotNull final String path) {
            this(file, path, file.length());
        }

        @Override
        @NotNull
        public String getPath() {
            return path;
        }

        @NotNull
        public File getFile() {
            return file;
        }

        @Override
        public long getTimeStamp() {
            return file.lastModified();
        }

        @Override
        @NotNull
        public InputStream getInputStream() throws IOException {
            return new FileInputStream(file);
        }

        @Override
        public boolean shouldCloseStream() {
            return true;
        }

        @Override
        @NotNull
        public String getName() {
            return file.getName();
        }

        @Override
        public boolean hasContent() {
            return file.isFile();
        }

        @Override
        public long getFileSize() {
            return fileSize;
        }

        @Override
        public boolean canBeEncrypted() {
            return canBeEncrypted;
        }

        @Override
        public VirtualFileDescriptor copy(long acceptedSize) {
            return new FileDescriptor(file, path, acceptedSize, canBeEncrypted);
        }
    }
}
