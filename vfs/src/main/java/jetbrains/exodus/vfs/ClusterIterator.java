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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

class ClusterIterator {

    private final long fd;
    private final Cursor cursor;
    private Cluster currentCluster;
    private boolean isClosed;

    ClusterIterator(@NotNull final Transaction txn, @NotNull final File file, @NotNull final Store contents) {
        this(txn, file.getDescriptor(), contents);
    }

    ClusterIterator(@NotNull final Transaction txn, long fileDescriptor, @NotNull final Store contents) {
        fd = fileDescriptor;
        cursor = contents.openCursor(txn);
        final ByteIterable it = cursor.getSearchKeyRange(ClusterKey.toByteIterable(fd, 0));
        if (it == null) {
            currentCluster = null;
        } else {
            currentCluster = new Cluster(it);
            adjustCurrentCluster();
        }
        isClosed = false;
    }

    boolean hasCluster() {
        return currentCluster != null;
    }

    Cluster getCurrent() {
        return currentCluster;
    }

    void moveToNext() {
        if (currentCluster != null) {
            if (!cursor.getNext()) {
                currentCluster = null;
            } else {
                currentCluster = new Cluster(cursor.getValue());
                adjustCurrentCluster();
            }
        }
    }

    void deleteCurrent() {
        if (currentCluster != null) {
            cursor.deleteCurrent();
        }
    }

    void close() {
        if (!isClosed) {
            cursor.close();
            isClosed = true;
        }
    }

    boolean isClosed() {
        return isClosed;
    }

    private void adjustCurrentCluster() {
        final ClusterKey clusterKey = new ClusterKey(cursor.getKey());
        if (clusterKey.getDescriptor() != fd) {
            currentCluster = null;
        } else {
            currentCluster.setClusterNumber(clusterKey.getClusterNumber());
        }
    }
}