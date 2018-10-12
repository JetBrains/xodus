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
package jetbrains.exodus.lucene;

import jetbrains.exodus.log.DataCorruptionException;
import jetbrains.exodus.vfs.ClusteringStrategy;
import jetbrains.exodus.vfs.File;
import jetbrains.exodus.vfs.VfsInputStream;
import org.apache.lucene.store.BufferedIndexInput;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

class ExodusIndexInput extends BufferedIndexInput {

    @NotNull
    private final ExodusDirectory directory;
    @NotNull
    private final File file;
    private VfsInputStream input;
    private long currentPosition;

    ExodusIndexInput(@NotNull final ExodusDirectory directory,
                     @NotNull final String name) {
        this(directory, name, 0L);
    }

    private ExodusIndexInput(@NotNull final ExodusDirectory directory,
                             @NotNull final String name,
                             final long currentPosition) {
        super("ExodusIndexInput[" + name + ']');
        this.directory = directory;
        this.file = directory.openExistingFile(name);
        this.currentPosition = currentPosition;
    }

    @Override
    public void close() {
        if (input != null) {
            input.close();
            input = null;
        }
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
        while (true) {
            try {
                currentPosition += getInput().read(b, offset, length);
                return;
            } catch (DataCorruptionException e) {
                handleFalseDataCorruption(e);
            }
        }
    }

    @Override
    protected void seekInternal(long pos) {
        if (pos != currentPosition) {
            if (pos > currentPosition) {
                final ClusteringStrategy clusteringStrategy = directory.getVfs().getConfig().getClusteringStrategy();
                final long bytesToSkip = pos - currentPosition;
                final int clusterSize = clusteringStrategy.getFirstClusterSize();
                if ((!clusteringStrategy.isLinear() ||
                    (currentPosition % clusterSize) + bytesToSkip < clusterSize) // or we are within single cluster
                    && getInput().skip(bytesToSkip) == bytesToSkip) {
                    currentPosition = pos;
                    return;
                }
            }
            close();
            currentPosition = pos;
            getInput();
        }
    }

    @Override
    public long length() {
        return directory.getVfs().getFileLength(directory.getEnvironment().getAndCheckCurrentTransaction(), file);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public final ExodusIndexInput clone() {
        return new ExodusIndexInput(directory, file.getPath(), currentPosition);
    }

    @NotNull
    private VfsInputStream getInput() {
        if (input == null || input.isObsolete()) {
            input = directory.getVfs().readFile(directory.getEnvironment().getAndCheckCurrentTransaction(), file, currentPosition);
        }
        return input;
    }

    private void handleFalseDataCorruption(DataCorruptionException e) {
        // we use this dummy synchronized statement, since we don't want TransactionBase.isFinished to be a volatile field
        synchronized (directory) {
            if (!input.isObsolete()) {
                throw e;
            }
        }
    }
}
