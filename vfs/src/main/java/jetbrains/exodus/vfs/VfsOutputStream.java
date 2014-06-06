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
package jetbrains.exodus.vfs;

import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

class VfsOutputStream extends OutputStream {

    @NotNull
    private final VfsConfig config;
    @NotNull
    private final Transaction txn;
    private final long fd;
    private final Store contents;
    private final Runnable clusterFlushTrigger;
    private final ClusterIterator clusterIterator;
    private byte[] outputCluster;
    private int position;
    private int outputClusterSize;
    private boolean isOutputClusterDirty;
    private long currentClusterNumber;

    VfsOutputStream(@NotNull final VirtualFileSystem vfs,
                    @NotNull final Transaction txn,
                    final long fileDescriptor,
                    @Nullable final Runnable clusterFlushTrigger) {
        this(vfs, txn, fileDescriptor, 0, clusterFlushTrigger);
    }

    VfsOutputStream(@NotNull final VirtualFileSystem vfs,
                    @NotNull final Transaction txn,
                    final long fileDescriptor,
                    long position,
                    @Nullable final Runnable clusterFlushTrigger) {
        if (position < 0) {
            throw new IllegalArgumentException("Position in output stream can't be negative");
        }
        this.config = vfs.getConfig();
        this.txn = txn;
        fd = fileDescriptor;
        contents = vfs.getContents();
        final ClusteringStrategy clusteringStrategy = config.getClusteringStrategy();
        this.clusterFlushTrigger = clusterFlushTrigger;
        clusterIterator = new ClusterIterator(txn, fileDescriptor, contents);
        currentClusterNumber = 0;
        loadCurrentCluster(clusteringStrategy.getFirstClusterSize());
        while (clusterIterator.hasCluster()) {
            if (position < outputClusterSize) {
                this.position = (int) position;
                break;
            }
            position -= outputClusterSize;
            clusterIterator.moveToNext();
            loadCurrentCluster(clusteringStrategy.getNextClusterSize(outputCluster.length));
        }
    }

    @Override
    public void write(int b) throws IOException {
        int position = this.position;
        if (position == outputCluster.length) {
            flushCurrentCluster();
            clusterIterator.moveToNext();
            loadCurrentCluster(config.getClusteringStrategy().getNextClusterSize(position));
            position = this.position;
        }

        final byte sourceByte = outputCluster[position];
        final byte destByte = (byte) b;
        if (sourceByte != destByte) {
            outputCluster[position] = destByte;
            isOutputClusterDirty = true;
        }
        if (++position > outputClusterSize) {
            outputClusterSize = position;
            isOutputClusterDirty = true;
        }
        this.position = position;
    }


    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int position = this.position;
        if (position + len > outputCluster.length) {
            super.write(b, off, len);
        } else {
            if (isOutputClusterDirty) {
                System.arraycopy(b, off, outputCluster, position, len);
                position += len;
            } else {
                for (; len > 0; --len, ++off, ++position) {
                    final byte sourceByte = outputCluster[position];
                    final byte destByte = b[off];
                    if (sourceByte != destByte) {
                        outputCluster[position] = destByte;
                        isOutputClusterDirty = true;
                    }
                }
            }
            if ((this.position = position) > outputClusterSize) {
                outputClusterSize = position;
                isOutputClusterDirty = true;
            }
        }
    }

    @Override
    public void close() {
        try {
            flushCurrentCluster();
        } finally {
            clusterIterator.close();
        }
    }

    public boolean isOpen() {
        return !clusterIterator.isClosed();
    }

    private void flushCurrentCluster() {
        if (isOutputClusterDirty) {
            contents.put(txn, ClusterKey.toByteIterable(fd, currentClusterNumber),
                    Cluster.writeCluster(outputCluster, outputClusterSize, config.getAccumulateChangesInRAM()));
            if (clusterFlushTrigger != null) {
                clusterFlushTrigger.run();
            }
        }
    }

    private void loadCurrentCluster(final int clusterSize) {
        final Cluster currentCluster = clusterIterator.getCurrent();
        if (currentCluster == null) {
            outputClusterSize = 0;
            outputCluster = new byte[clusterSize];
            currentClusterNumber = config.getClusteringStrategy().getNextClusterNumber(currentClusterNumber);
        } else {
            outputClusterSize = currentCluster.getSize();
            outputCluster = new byte[clusterSize > outputClusterSize ? clusterSize : outputClusterSize];
            for (int i = 0; i < outputClusterSize; ++i) {
                outputCluster[i] = currentCluster.next();
            }
            currentClusterNumber = currentCluster.getClusterNumber();
        }
        position = 0;
        isOutputClusterDirty = false;
    }
}