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
package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexBuilder;
import jetbrains.vectoriadb.index.DataStore;
import jetbrains.vectoriadb.index.L2DistanceFunction;
import jetbrains.vectoriadb.index.Slf4jPeriodicProgressTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class PrepareBigANNBench {
    public static final int VECTOR_DIMENSIONS = 128;

    public static final int VECTORS_COUNT = 500_000_000;
    public static final String NAME_SUFFIX = "500m";
    public static final String INDEX_NAME = "bigann_index_" + NAME_SUFFIX;

    public static void main(String[] args) {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        try {
            var baseArchiveName = "bigann_base.bvecs.gz";
            var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);

            var dataFileName = "bigann_base.bvecs";
            var dataFilePath = benchPath.resolve(dataFileName);

            if (!Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
                BenchUtils.extractGzArchive(dataFilePath, baseArchivePath);
            }

            var dbDir = Files.createDirectories(benchPath.resolve(INDEX_NAME));
            System.out.printf("%d data vectors loaded with dimension %d for BigANN index, " +
                            "building index in directory %s...%n",
                    VECTORS_COUNT, VECTOR_DIMENSIONS, dbDir.toAbsolutePath());

            var ts1 = System.nanoTime();
            Path indexDataLocation;

            var recordSize = Integer.BYTES + VECTOR_DIMENSIONS;
            try (var channel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
                var buffer = ByteBuffer.allocate((64 * 1024 * 1024 / recordSize) * recordSize).order(ByteOrder.LITTLE_ENDIAN);

                while (buffer.remaining() > 0) {
                    channel.read(buffer);
                }
                buffer.rewind();

                try (var dataBuilder = DataStore.create(INDEX_NAME, VECTOR_DIMENSIONS,
                        L2DistanceFunction.INSTANCE, dbDir)) {
                    for (long i = 0; i < VECTORS_COUNT; i++) {
                        if (buffer.remaining() == 0) {
                            buffer.rewind();

                            while (buffer.remaining() > 0) {
                                var r = channel.read(buffer);
                                if (r == -1) {
                                    break;
                                }
                            }
                            buffer.clear();
                        }

                        var dimensions = buffer.getInt();
                        if (dimensions != VECTOR_DIMENSIONS) {
                            throw new RuntimeException("Vector dimensions mismatch : " +
                                    dimensions + " vs " + VECTOR_DIMENSIONS);
                        }

                        var vector = new float[VECTOR_DIMENSIONS];
                        for (int j = 0; j < VECTOR_DIMENSIONS; j++) {
                            vector[j] = buffer.get();
                        }

                        dataBuilder.add(vector);
                    }

                    indexDataLocation = dataBuilder.dataLocation();
                }
            }

            IndexBuilder.buildIndex(INDEX_NAME, VECTOR_DIMENSIONS, dbDir, indexDataLocation,
                    60L * 1024 * 1024 * 1024, Distance.L2,
                    new Slf4jPeriodicProgressTracker(5));
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


