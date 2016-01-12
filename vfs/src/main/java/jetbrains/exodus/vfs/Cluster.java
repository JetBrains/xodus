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

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.log.CompoundByteIterable;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class Cluster {

    private final ByteIterator iterator;
    private long clusterNumber;
    private int size;

    Cluster(@NotNull final ByteIterable it) {
        iterator = it instanceof FixedLengthByteIterable ? ((FixedLengthByteIterable) it).getSource().iterator() : it.iterator();
        clusterNumber = 0;
        size = IntegerBinding.readCompressed(iterator);
    }

    long getClusterNumber() {
        return clusterNumber;
    }

    void setClusterNumber(final long clusterNumber) {
        this.clusterNumber = clusterNumber;
    }

    int getSize() {
        return size;
    }

    boolean hasNext() {
        return size > 0 && iterator.hasNext();
    }

    byte next() {
        final byte result = iterator.next();
        --size;
        return result;
    }

    long skip(final long length) {
        final long skipped = length > size ? size : iterator.skip(length);
        size -= (int) skipped;
        return skipped;
    }

    void copyTo(@NotNull final byte[] array) {
        int i = 0;
        while (hasNext()) {
            array[i++] = next();
        }
    }

    static ByteIterable writeCluster(@NotNull final byte[] cluster, final int size, final boolean accumulateInRAM) {
        final ByteIterable bi;
        if (accumulateInRAM) {
            bi = new ArrayByteIterable(cluster, size);
        } else {
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
        }
        return new CompoundByteIterable(new ByteIterable[]{IntegerBinding.intToCompressedEntry(size), bi});
    }
}
