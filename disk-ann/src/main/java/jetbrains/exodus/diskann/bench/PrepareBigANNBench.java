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
package jetbrains.exodus.diskann.bench;

import jetbrains.exodus.diskann.IndexBuilder;
import jetbrains.exodus.diskann.DataStore;
import jetbrains.exodus.diskann.L2DistanceFunction;
import jetbrains.exodus.diskann.L2PQQuantizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class PrepareBigANNBench {
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

            var vectorDimensions = 128;
            var dbDir = Files.createDirectories(benchPath.resolve("vectoriadb-bigann_index"));
            System.out.printf("%d data vectors loaded with dimension %d for BigANN index, " +
                            "building index in directory %s...%n",
                    500_000_000, vectorDimensions, dbDir.toAbsolutePath());

            var ts1 = System.nanoTime();
            Path indexDataLocation;
            try (var channel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
                var buffer = ByteBuffer.allocate(vectorDimensions * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                try (var dataBuilder = DataStore.create(vectorDimensions, "bigann_index", dbDir)) {
                    for (int i = 0; i < 500_000_000; i++) {
                        channel.position(i * (vectorDimensions * Float.BYTES + Integer.BYTES));
                        buffer.rewind();
                        while (buffer.remaining() > 0) {
                            channel.read(buffer);
                        }
                        buffer.rewind();

                        var vector = new float[vectorDimensions];
                        for (int j = 0; j < vectorDimensions; j++) {
                            vector[j] = buffer.getFloat();
                        }

                        dataBuilder.add(vector);
                    }

                    indexDataLocation = dataBuilder.dataLocation();
                }
            }

            IndexBuilder.buildIndex("bigann_index", vectorDimensions, dbDir, indexDataLocation,
                    60L * 1024 * 1024 * 1024, L2PQQuantizer.INSTANCE, L2DistanceFunction.INSTANCE);
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


