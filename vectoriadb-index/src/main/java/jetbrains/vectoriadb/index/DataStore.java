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
package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DataStore implements AutoCloseable {
    public static final String DATA_FILE_EXTENSION = ".vectors";
    private final FileChannel channel;
    private final ByteBuffer buffer;

    private final DistanceFunction distanceFunction;

    private final float[] preprocessingResult;

    private DataStore(int dimensions, DistanceFunction distanceFunction, FileChannel channel) {
        this.channel = channel;
        this.distanceFunction = distanceFunction;

        var vectorSize = dimensions * Float.BYTES;
        var bufferSize = Math.min(64 * 1024 * 1024 / vectorSize, 1) * vectorSize;

        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.nativeOrder());
        this.preprocessingResult = new float[dimensions];
    }

    public static DataStore create(final String name, final int dimensions,
                                   final DistanceFunction distanceFunction,
                                   final Path dataDirectoryPath) throws IOException {
        var dataPath = dataLocation(name, dataDirectoryPath);
        var channel = FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        return new DataStore(dimensions, distanceFunction, channel);
    }

    public void add(final float[] vector) throws IOException {
        var vectorToStore = distanceFunction.preProcess(vector, preprocessingResult);

        if (buffer.remaining() == 0) {
            buffer.rewind();

            while (buffer.remaining() > 0) {
                channel.write(buffer);
            }

            buffer.rewind();
        }

        for (var component : vectorToStore) {
            buffer.putFloat(component);
        }
    }

    public static Path dataLocation(@NotNull final String name, final Path dataDirectoryPath) {
        return dataDirectoryPath.resolve(name + DATA_FILE_EXTENSION);
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
