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

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class Cluster {

    @NotNull
    private final ByteIterable it;
    @Nullable
    private ByteIterator iterator;
    private long startingPosition;
    private long clusterNumber;
    private int size;

    Cluster(@NotNull final ByteIterable it) {
        this.it = it;
    }

    long getStartingPosition() {
        return startingPosition;
    }

    void setStartingPosition(final long startingPosition) {
        this.startingPosition = startingPosition;
    }

    long getClusterNumber() {
        return clusterNumber;
    }

    void setClusterNumber(final long clusterNumber) {
        this.clusterNumber = clusterNumber;
    }

    int getSize() {
        getIterator();
        return size;
    }

    boolean hasNext() {
        return getSize() > 0;
    }

    byte next() {
        final byte result = getIterator().next();
        --size;
        return result;
    }

    long skip(final long length) {
        final int size = getSize();
        final long skipped = length > size ? size : getIterator().skip(length);
        this.size -= (int) skipped;
        return skipped;
    }

    void copyTo(@NotNull final byte[] array) {
        int i = 0;
        while (hasNext()) {
            array[i++] = next();
        }
    }

    static ByteIterable writeCluster(@NotNull final byte[] cluster,
                                     @Nullable final ClusterConverter clusterConverter,
                                     final int size,
                                     final boolean accumulateInRAM) {
        if (accumulateInRAM) {
            final LightOutputStream output = new LightOutputStream(size + 5);
            IntegerBinding.writeCompressed(output, size);
            output.write(cluster, 0, size);
            final ArrayByteIterable result = output.asArrayByteIterable();
            return clusterConverter == null ? result : clusterConverter.onWrite(result);
        }
        final ByteIterable bi;
        try {
            final File file = File.createTempFile("~exodus-vfs-output-cluster", ".tmp");
            try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
                out.write(cluster, 0, size);
            }
            bi = new FileByteIterable(file);
            file.deleteOnExit();
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
        return new CompoundByteIterable(new ByteIterable[]{IntegerBinding.intToCompressedEntry(size), bi});
    }

    @NotNull
    private ByteIterator getIterator() {
        if (iterator == null) {
            iterator = it.iterator();
            size = IntegerBinding.readCompressed(iterator);
        }
        return iterator;
    }
}
