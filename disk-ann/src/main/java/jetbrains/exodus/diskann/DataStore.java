/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.diskann;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DataStore implements AutoCloseable {
    public static final String DATA_FILE_EXTENSION = ".vectors";
    private final FileChannel channel;
    private final Path dataPath;
    private final ByteBuffer buffer;

    private DataStore(int dimensions, FileChannel channel, Path dataPath) {
        this.channel = channel;
        this.dataPath = dataPath;

        var vectorSize = dimensions * Float.BYTES;
        var bufferSize = Math.min(64 * 1024 * 1024 / vectorSize, 1) * vectorSize;

        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.nativeOrder());
    }

    public static DataStore create(final int dimensions, final String name,
                                   final Path path) throws IOException {
        var dataPath = path.resolve(name + DATA_FILE_EXTENSION);
        var channel = FileChannel.open(dataPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        return new DataStore(dimensions, channel, dataPath);
    }

    public void add(final float[] vector) throws IOException {
        if (buffer.remaining() == 0) {
            buffer.rewind();

            while (buffer.remaining() > 0) {
                channel.write(buffer);
            }

            buffer.rewind();
        }

        for (var value : vector) {
            buffer.putFloat(value);
        }
    }

    public Path dataLocation() {
        return dataPath;
    }

    public void close() throws IOException {
        if (buffer.remaining() < buffer.capacity()) {
            buffer.flip();

            while (buffer.remaining() > 0) {
                //noinspection ResultOfMethodCallIgnored
                channel.write(buffer);
            }
        }

        channel.force(true);
        channel.close();
    }
}
