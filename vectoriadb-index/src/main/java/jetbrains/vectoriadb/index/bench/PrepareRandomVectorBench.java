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

import it.unimi.dsi.fastutil.ints.IntFloatImmutablePair;
import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.DotDistanceFunction;
import jetbrains.vectoriadb.index.IndexBuilder;
import jetbrains.vectoriadb.index.VectorReader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public final class PrepareRandomVectorBench {
    public static final String DATA_FILE_NAME = "data.bin";
    public static final String TEST_DATA_FILE_NAME = "test-data.bin";
    public static final String GROUND_TRUTH_FILE_NAME = "ground-truth.bin";

    public static final String DB_PATH_NAME = "vectoriadb-random_index";

    public static void main(String[] args) throws Exception {
        var dbPath = Path.of(DB_PATH_NAME);
        var vectorDimensions = 768;

        var testDataPath = dbPath.resolve(TEST_DATA_FILE_NAME);
        var dataPath = dbPath.resolve(DATA_FILE_NAME);
        var groundTruthPath = dbPath.resolve(GROUND_TRUTH_FILE_NAME);
        var groundTruthCount = 10_000;
        var vectorsCount = 4_000_000;

        if (!Files.exists(dbPath)) {
            Files.createDirectories(dbPath);
        }

        System.out.printf("Generating %d test vectors with dimension %d...%n", vectorsCount, vectorDimensions);
        generateTestData(vectorDimensions, vectorsCount, dataPath);

        System.out.printf("Generating %d query vectors...%n", groundTruthCount);
        generateTestData(vectorDimensions, groundTruthCount, testDataPath);

        System.out.println("Generating ground truth...");
        generateGroundTruth(vectorDimensions, testDataPath, dataPath, groundTruthPath, groundTruthCount);

        System.out.println("Building index...");

        IndexBuilder.buildIndex("random_index", vectorDimensions, dbPath, dataPath,
                60L * 1024 * 1024 * 1024, Distance.DOT);

        System.out.println("Done.");
    }

    @SuppressWarnings("unused")
    private static void generateGroundTruth(int vectorDimensions, Path testDataPath, Path dataPath,
                                            Path groundTruthPath, int groundTruthCount)
            throws IOException, InterruptedException, ExecutionException {

        var cores = Runtime.getRuntime().availableProcessors();
        var futures = new Future[cores];

        try (final ExecutorService executorService = Executors.newFixedThreadPool(cores)) {
            try (var testDataReader = new MmapVectorReader(vectorDimensions, testDataPath)) {
                try (var dataReader = new MmapVectorReader(vectorDimensions, dataPath)) {
                    var maxVectorsPerCore = dataReader.size() / cores;

                    var buffer = ByteBuffer.allocate(Integer.BYTES);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);

                    try (var channel = FileChannel.open(groundTruthPath,
                            StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                        for (int i = 0; i < groundTruthCount; i++) {

                            for (int n = 0; n < cores; n++) {
                                var testVector = testDataReader.read(i);

                                var start = n * maxVectorsPerCore;
                                var end = Math.min(start + maxVectorsPerCore, dataReader.size());

                                futures[n] = executorService.submit(() -> {
                                    var minDistance = Float.MAX_VALUE;
                                    var minIndex = -1;

                                    for (int j = start; j < end; j++) {
                                        var vector = dataReader.read(j);

                                        var distance = DotDistanceFunction.INSTANCE.computeDistance(testVector, 0, vector,
                                                0, vectorDimensions);
                                        if (distance < minDistance) {
                                            minDistance = distance;
                                            minIndex = j;
                                        }
                                    }

                                    return new IntFloatImmutablePair(minIndex, minDistance);
                                });
                            }

                            var minIndex = -1;
                            var minDistance = Float.MAX_VALUE;

                            for (var future : futures) {
                                var pair = (IntFloatImmutablePair) future.get();

                                if (minDistance > pair.rightFloat()) {
                                    minIndex = pair.leftInt();
                                    minDistance = pair.rightFloat();
                                }
                            }

                            buffer.rewind();
                            buffer.putInt(minIndex);
                            buffer.rewind();

                            while (buffer.remaining() > 0) {
                                //noinspection ResultOfMethodCallIgnored
                                channel.write(buffer);
                            }
                        }

                        channel.force(true);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unused")
    private static void generateTestData(int vectorDimensions, int count, Path filePath) throws IOException {
        var rnd = ThreadLocalRandom.current();
        var buffer = ByteBuffer.allocate(vectorDimensions * Float.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        try (var channel = FileChannel.open(filePath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            for (int i = 0; i < count; i++) {
                buffer.rewind();

                for (int j = 0; j < vectorDimensions; j++) {
                    buffer.putFloat(rnd.nextFloat());
                }
                buffer.rewind();

                while (buffer.remaining() > 0) {
                    //noinspection ResultOfMethodCallIgnored
                    channel.write(buffer);
                }
            }

            channel.force(true);
        }
    }

    public static final class MmapVectorReader implements VectorReader {
        private final int recordSize;
        private final MemorySegment segment;

        private final Arena arena;

        private final int vectorDimensions;

        private final int size;

        public MmapVectorReader(final int vectorDimensions, Path path) throws IOException {
            this.vectorDimensions = vectorDimensions;
            this.recordSize = Float.BYTES * vectorDimensions;


            arena = Arena.openShared();

            try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena.scope());
                this.size = (int) (channel.size() / recordSize);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public MemorySegment read(int index) {
            return segment.asSlice((long) index * recordSize, (long) Float.BYTES * vectorDimensions);
        }

        @Override
        public void close() {
            arena.close();
        }
    }
}
