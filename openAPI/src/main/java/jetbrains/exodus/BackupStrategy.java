/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Collections;

public abstract class BackupStrategy {

    public static final BackupStrategy EMPTY = new BackupStrategy() {

        @Override
        public Iterable<FileDescriptor> listFiles() {
            return Collections.emptyList();
        }
    };

    public void beforeBackup() throws Exception {
    }

    public abstract Iterable<FileDescriptor> listFiles();

    public void afterBackup() throws Exception {
    }

    public boolean isInterrupted() {
        return false;
    }

    public void onError(Throwable t) {
    }

    /**
     * @param file file to be backed up.
     * @return the length of specified file that should be put into backup, minimum is taken between the actual
     * file size and the value returned. If the method returns a less than one value the file won't be put into backup.
     */
    public long acceptFile(@NotNull File file) {
        return Long.MAX_VALUE;
    }

    public static class FileDescriptor {

        @NotNull
        private final File file;
        @NotNull
        private final String path;
        private final long fileSize;

        public FileDescriptor(@NotNull final File file, @NotNull final String path, final long fileSize) {
            this.file = file;
            this.path = path;
            this.fileSize = fileSize;
        }

        public FileDescriptor(@NotNull final File file, @NotNull final String path) {
            this(file, path, file.length());
        }

        @NotNull
        public String getPath() {
            return path;
        }

        @NotNull
        public File getFile() {
            return file;
        }

        public long getFileSize() {
            return fileSize;
        }
    }
}
