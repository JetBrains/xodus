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

import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;

public class VfsInputStream extends InputStream {

    private final ClusterIterator clusterIterator;

    VfsInputStream(@NotNull final VirtualFileSystem vfs, @NotNull final Transaction txn, final long fileDescriptor) {
        this(vfs, txn, fileDescriptor, 0);
    }

    VfsInputStream(@NotNull final VirtualFileSystem vfs,
                   @NotNull final Transaction txn,
                   final long fileDescriptor,
                   long position) {
        clusterIterator = new ClusterIterator(vfs, txn, fileDescriptor, position);
        final Cluster current = clusterIterator.getCurrent();
        if (current != null) {
            position -= current.getStartingPosition();
            final long skipped = current.skip(position);
            if (skipped != position) {
                throw new VfsException();
            }
        }
    }

    @Override
    public int read() {
        while (true) {
            final Cluster current = clusterIterator.getCurrent();
            if (current == null) {
                return -1;
            }
            if (current.hasNext()) {
                return current.next() & 0xff;
            }
            clusterIterator.moveToNext();
        }
    }

    @Override
    public void close() {
        clusterIterator.close();
    }

    public boolean isOpen() {
        return !clusterIterator.isClosed();
    }

    @Override
    public long skip(final long n) {
        long skipped = 0;
        while (clusterIterator.hasCluster()) {
            skipped += clusterIterator.getCurrent().skip(n - skipped);
            if (skipped == n) {
                break;
            }
            clusterIterator.moveToNext();
        }
        return skipped;
    }

    public boolean isObsolete() {
        return clusterIterator.isObsolete();
    }
}
