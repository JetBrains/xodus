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
package jetbrains.exodus.vfs;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;

class VfsAppendingStream extends OutputStream {

    @NotNull
    private final VirtualFileSystem vfs;
    @NotNull
    private final Transaction txn;
    private final long fd;
    private final Store contents;
    private final Runnable clusterFlushTrigger;
    private byte[] outputCluster;
    private int position;
    private long currentClusterNumber;
    private boolean isOpen;

    VfsAppendingStream(@NotNull final VirtualFileSystem vfs,
                       @NotNull final Transaction txn,
                       @NotNull final File file,
                       @NotNull final Runnable clusterFlushTrigger) {
        this.vfs = vfs;
        this.txn = txn;
        fd = file.getDescriptor();
        contents = vfs.getContents();
        this.clusterFlushTrigger = clusterFlushTrigger;

        outputCluster = null;

        final ClusteringStrategy clusteringStrategy = vfs.getConfig().getClusteringStrategy();

        final Pair<Cluster, Cluster> pairOfClusters = getTwoLastClusters(vfs, txn, file);
        final Cluster prevCluster = pairOfClusters.getFirst();
        final Cluster currentCluster = pairOfClusters.getSecond();
        final int prevClusterSize = prevCluster == null ? 0 : prevCluster.getSize();
        final int currentClusterSize = currentCluster == null ? 0 : currentCluster.getSize();
        final int clusterSize = prevClusterSize == 0 ?
            clusteringStrategy.getFirstClusterSize() : clusteringStrategy.getNextClusterSize(prevClusterSize);

        currentClusterNumber = currentCluster == null ? 0 : currentCluster.getClusterNumber();

        if (clusterSize <= currentClusterSize) {
            // current cluster is full, alloc the next one
            allocNextCluster(clusteringStrategy.getNextClusterSize(currentClusterSize));
        } else {
            outputCluster = new byte[clusterSize];
            if (currentClusterSize > 0) {
                //noinspection ConstantConditions
                currentCluster.copyTo(outputCluster);
            }
            position = currentClusterSize;
        }

        isOpen = true;
    }

    @Override
    public void write(int b) {
        if (position == outputCluster.length) {
            flushCurrentCluster();
            allocNextCluster(vfs.getConfig().getClusteringStrategy().getNextClusterSize(position));
        }
        outputCluster[position++] = (byte) b;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            flushCurrentCluster();
        }
    }

    private void allocNextCluster(final int size) {
        outputCluster = new byte[size];
        position = 0;
        ++currentClusterNumber;
    }

    private void flushCurrentCluster() {
        if (position > 0) {
            contents.put(txn, ClusterKey.toByteIterable(fd, currentClusterNumber),
                Cluster.writeCluster(
                    outputCluster, vfs.getClusterConverter(), position, vfs.getConfig().getAccumulateChangesInRAM()));
            clusterFlushTrigger.run();
        }
    }

    // Seeking to end of the file by walking through all clusters
    private static Pair<Cluster, Cluster> getTwoLastClusters(@NotNull VirtualFileSystem vfs,
                                                             @NotNull final Transaction txn,
                                                             @NotNull final File file) {
        // todo: seek to end of file without loading all clusters
        try (ClusterIterator iterator = new ClusterIterator(vfs, txn, file)) {
            Cluster prevCluster = null;
            Cluster currCluster = null;
            while (iterator.hasCluster()) {
                prevCluster = currCluster;
                currCluster = iterator.getCurrent();
                iterator.moveToNext();
            }
            return new Pair<>(prevCluster, currCluster);
        }
    }
}
