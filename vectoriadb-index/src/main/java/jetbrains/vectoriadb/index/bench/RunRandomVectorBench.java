/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
import jetbrains.vectoriadb.index.IndexReader;
import jetbrains.vectoriadb.index.diskcache.DiskCache;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class RunRandomVectorBench {
    public static void main(String[] args) throws Exception {
        System.out.println("Reading ground truth...");
        var dbPath = Path.of(PrepareRandomVectorBench.DB_PATH_NAME);
        var vectorDimensions = 768;

        var testDataPath = dbPath.resolve(PrepareRandomVectorBench.TEST_DATA_FILE_NAME);
        var groundTruthPath = dbPath.resolve(PrepareRandomVectorBench.GROUND_TRUTH_FILE_NAME);

        var groundTruthCount = 10_000;
        var groundTruth = new int[groundTruthCount];

        var readBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        try (var channel = FileChannel.open(groundTruthPath, StandardOpenOption.READ)) {
            for (var index = 0; index < groundTruthCount; index++) {
                readBuffer.rewind();

                while (readBuffer.remaining() > 0) {
                    channel.read(readBuffer);
                }

                readBuffer.rewind();
                groundTruth[index] = readBuffer.getInt();
            }
        }

        System.out.println("Running queries...");
        var errors = 0;
        var result = new int[1];

        try (var diskCache = new DiskCache(400 * 1024 * 1024, vectorDimensions,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            try (var indexReader = new IndexReader("random_index", vectorDimensions, dbPath,
                    Distance.DOT, diskCache)) {
                try (var queryVectors = new PrepareRandomVectorBench.MmapVectorReader(vectorDimensions, testDataPath)) {
                    var start = System.nanoTime();
                    for (var index = 0; index < groundTruthCount; index++) {
                        var queryVectorSegment = queryVectors.read(index);

                        var queryVector = new float[vectorDimensions];
                        MemorySegment.copy(queryVectorSegment, ValueLayout.JAVA_FLOAT, 0, queryVector,
                                0, vectorDimensions);

                        indexReader.nearest(queryVector, result, 1);
                        if (result[0] != groundTruth[index]) {
                            errors++;
                        }
                    }
                    var end = System.nanoTime();
                    System.out.printf("Queries done in %d ms.%n", (end - start) / 1000000);
                }

                System.out.printf("Errors: %f%%%n", 100.0 * errors / groundTruthCount);
            }
        }
    }
}
