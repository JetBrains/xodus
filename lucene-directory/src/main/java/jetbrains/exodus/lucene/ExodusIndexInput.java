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

import jetbrains.exodus.vfs.ClusteringStrategy;
import jetbrains.exodus.vfs.File;
import jetbrains.exodus.vfs.VfsInputStream;
import org.apache.lucene.store.IndexInput;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class ExodusIndexInput extends IndexInput {

    @NotNull
    private final ExodusDirectory directory;
    @NotNull
    private final File file;
    @NotNull
    private VfsInputStream input;
    private long currentPosition;

    public ExodusIndexInput(@NotNull final ExodusDirectory directory,
                            @NotNull final String name) {
        this(directory, name, 0L);
    }

    private ExodusIndexInput(@NotNull final ExodusDirectory directory,
                             @NotNull final String name,
                             final long currentPosition) {
        super("ExodusDirectory IndexInput for " + name);
        this.directory = directory;
        this.file = directory.openExistingFile(name, true);
        input = directory.getVfs().readFile(directory.getEnvironment().getAndCheckCurrentTransaction(), file, currentPosition);
        this.currentPosition = currentPosition;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public long getFilePointer() {
        return currentPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != currentPosition) {
            if (pos > currentPosition) {
                final ClusteringStrategy clusteringStrategy = directory.getVfs().getConfig().getClusteringStrategy();
                final long bytesToSkip = pos - currentPosition;
                final int clusterSize = clusteringStrategy.getFirstClusterSize();
                if ((!clusteringStrategy.isLinear() ||
                    (currentPosition % clusterSize) + bytesToSkip < clusterSize) // or we are within single cluster
                    && input.skip(bytesToSkip) == bytesToSkip) {
                    currentPosition = pos;
                    return;
                }
            }
            input.close();
            input = directory.getVfs().readFile(directory.getEnvironment().getAndCheckCurrentTransaction(), file, pos);
            currentPosition = pos;
        }
    }

    @Override
    public long length() {
        return directory.getVfs().getFileLength(directory.getEnvironment().getAndCheckCurrentTransaction(), file);
    }

    @Override
    public byte readByte() throws IOException {
        ++currentPosition;
        return (byte) input.read();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 1) {
            b[offset] = readByte();
        } else {
            currentPosition += input.read(b, offset, len);
        }
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public final Object clone() {
        return new ExodusIndexInput(directory, file.getPath(), currentPosition);
    }
}
