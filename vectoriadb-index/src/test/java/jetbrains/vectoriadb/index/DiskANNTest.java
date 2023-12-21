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
package jetbrains.vectoriadb.index;

import jetbrains.vectoriadb.index.bench.VectorDatasetInfo;
import jetbrains.vectoriadb.index.bench.VectorFileReader;
import jetbrains.vectoriadb.index.diskcache.DiskCache;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;

import static jetbrains.vectoriadb.index.VectorTestUtilsKt.*;
import static jetbrains.vectoriadb.index.bench.RunBenchKt.toVectorId;

public class DiskANNTest {
    @Test
    public void testFindLoadedVectorsL2Distance() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var recallCount = 5;
        var vectorDimensions = 64;
        var vectorsCount = 10_000;

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

        var vectors = new float[vectorsCount][vectorDimensions];
        var queries = new float[vectorsCount][vectorDimensions];

        generateUniqueVectorSet(vectors, rng);
        generateUniqueVectorSet(queries, rng);

        var groundTruth = calculateGroundTruthVectors(vectors, queries, L2DistanceFunction.INSTANCE, recallCount);

        var dbDir = Files.createTempDirectory(Path.of(buildDir), "testFindLoadedVectorsL2Distance");
        dbDir.toFile().deleteOnExit();

        var indexName = "test_index";
        var ts1 = System.nanoTime();
        try (var dataBuilder = DataStore.create(indexName, vectorDimensions, L2DistanceFunction.INSTANCE, dbDir)) {
            for (int n = 0; n < vectorsCount; n++) {
                var vector = vectors[n];
                var id = ByteBuffer.allocate(IndexBuilder.VECTOR_ID_SIZE).
                        order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
                dataBuilder.add(vector, id);
            }
        }

        IndexBuilder.buildIndex(indexName, vectorDimensions, dbDir,
                DataStore.dataLocation(indexName, dbDir),
                4 * 1024 * 1024, Distance.L2, new ConsolePeriodicProgressTracker(1));
        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        var totalRecall = 0.0;
        try (var diskCache = new DiskCache(256 * 1024 * 1024, vectorDimensions,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            try (var indexReader = new IndexReader(indexName, vectorDimensions, dbDir, Distance.L2, diskCache)) {
                ts1 = System.nanoTime();
                for (var j = 0; j < vectorsCount; j++) {
                    var vector = queries[j];
                    var rawIds = indexReader.nearest(vector, recallCount);
                    var result = new int[recallCount];

                    for (int n = 0; n < recallCount; n++) {
                        result[n] = ByteBuffer.wrap(rawIds[n]).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    }

                    totalRecall += recall(result, groundTruth[j]);

                    if ((j + 1) % 1_000 == 0) {
                        System.out.println("Processed " + (j + 1));
                    }
                }

                ts2 = System.nanoTime();
                var recall = totalRecall / vectorsCount;

                System.out.printf("Avg. query %d time us, R@%d : %f, cache hits %d%% %n",
                        (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall, indexReader.hits());
                Assert.assertTrue("Recall is too low " + recall + " < 0.92",
                        recall >= 0.92);
                indexReader.deleteIndex();
            }
        }
    }

    @Test
    public void testFindLoadedVectorsDotDistance() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var vectorDimensions = 64;
        var vectorsCount = 10_000;
        var recallCount = 5;

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        var vectors = new float[vectorsCount][vectorDimensions];

        generateUniqueVectorSet(vectors, rng);

        var queryVectors = new float[vectorsCount][vectorDimensions];
        generateUniqueVectorSet(queryVectors, rng);

        var groundTruth = calculateGroundTruthVectors(vectors, queryVectors, DotDistanceFunction.INSTANCE,
                recallCount);
        var dbDir = Files.createTempDirectory(Path.of(buildDir), "testFindLoadedVectorsDotDistance");
        dbDir.toFile().deleteOnExit();
        var ts1 = System.nanoTime();

        var indexName = "test_index";
        try (var dataStore = DataStore.create(indexName, vectorDimensions, DotDistanceFunction.INSTANCE, dbDir)) {
            for (var n = 0; n < vectorsCount; n++) {
                var id = ByteBuffer.allocate(IndexBuilder.VECTOR_ID_SIZE).
                        order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
                var vector = vectors[n];
                dataStore.add(vector, id);
            }
        }

        IndexBuilder.buildIndex(indexName, vectorDimensions, dbDir, DataStore.dataLocation(indexName, dbDir),
                4 * 1024 * 1024, Distance.DOT, new ConsolePeriodicProgressTracker(1));

        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        var totalRecall = 0.0;
        try (var diskCache = new DiskCache(256 * 1024 * 1024, vectorDimensions,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            try (var indexReader = new IndexReader(indexName, vectorDimensions, dbDir, Distance.DOT, diskCache)) {
                ts1 = System.nanoTime();
                for (var j = 0; j < vectorsCount; j++) {
                    var vector = queryVectors[j];

                    var rawIds = indexReader.nearest(vector, recallCount);
                    var result = new int[recallCount];

                    for (int n = 0; n < recallCount; n++) {
                        result[n] = ByteBuffer.wrap(rawIds[n]).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    }
                    totalRecall += recall(result, groundTruth[j]);

                    if ((j + 1) % 1_000 == 0) {
                        System.out.println("Processed " + (j + 1));
                    }
                }

                ts2 = System.nanoTime();

                var recall = totalRecall / vectorsCount;
                System.out.printf("Avg. query %d time us, R@%d: %f, cache hits %d%% %n",
                        (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall,
                        indexReader.hits());
                Assert.assertTrue("Recall is too low " + recall + " < 0.85", recall >= 0.85);
                indexReader.deleteIndex();
            }
        }
    }

    @Test
    public void testFindLoadedVectorsCosineDistance() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var vectorDimensions = 64;
        var vectorsCount = 10_000;
        var recallCount = 5;

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        var vectors = new float[vectorsCount][vectorDimensions];

        generateUniqueVectorSet(vectors, rng);

        var queryVectors = new float[vectorsCount][vectorDimensions];
        generateUniqueVectorSet(queryVectors, rng);

        var groundTruth = calculateGroundTruthVectors(vectors, queryVectors,
                CosineDistanceFunction.INSTANCE, recallCount);

        var dbDir = Files.createTempDirectory(Path.of(buildDir), "testFindLoadedVectorsDotDistance");
        dbDir.toFile().deleteOnExit();
        var ts1 = System.nanoTime();

        var indexName = "test_index";
        try (var dataBuilder = DataStore.create(indexName, vectorDimensions,
                CosineDistanceFunction.INSTANCE, dbDir)) {
            for (int n = 0; n < vectorsCount; n++) {
                var vector = vectors[n];
                var id = ByteBuffer.allocate(IndexBuilder.VECTOR_ID_SIZE).
                        order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
                dataBuilder.add(vector, id);
            }
        }

        IndexBuilder.buildIndex(indexName, vectorDimensions, dbDir,
                DataStore.dataLocation("test_index", dbDir),
                4 * 1024 * 1024, Distance.COSINE,
                new ConsolePeriodicProgressTracker(1));
        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        try (var diskCache = new DiskCache(256 * 1024 * 1024, vectorDimensions,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            try (var indexReader = new IndexReader(indexName, vectorDimensions, dbDir,
                    Distance.COSINE, diskCache)) {
                var totalRecall = 0.0;
                ts1 = System.nanoTime();
                for (var j = 0; j < vectorsCount; j++) {
                    var vector = queryVectors[j];

                    var rawIds = indexReader.nearest(vector, recallCount);
                    var result = new int[recallCount];

                    for (var n = 0; n < recallCount; n++) {
                        result[n] = ByteBuffer.wrap(rawIds[n]).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    }
                    totalRecall += recall(result, groundTruth[j]);

                    if ((j + 1) % 1_000 == 0) {
                        System.out.println("Processed " + (j + 1));
                    }
                }

                ts2 = System.nanoTime();

                var recall = totalRecall / vectorsCount;
                System.out.printf("Avg. query %d time us, R@%d: %f, cache hits %d%% %n",
                        (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall,
                        indexReader.hits());

                Assert.assertTrue("Recall is too low " + recall + " < 0.69", recall >= 0.69);
                indexReader.deleteIndex();
            }
        }
    }


    private static void generateUniqueVectorSet(float[][] vectors, RestorableUniformRandomProvider rng) {
        var addedVectors = new HashSet<FloatArrayHolder>();

        for (float[] vector : vectors) {
            var counter = 0;
            do {
                if (counter > 0) {
                    System.out.println("duplicate vector found " + counter + ", retrying...");
                }

                for (var j = 0; j < vector.length; j++) {
                    vector[j] = rng.nextFloat();
                }
                counter++;
            } while (!addedVectors.add(new FloatArrayHolder(vector)));
        }
    }


    @SuppressWarnings("SameParameterValue")
    private static int[][] calculateGroundTruthVectors(float[][] vectors, float[][] queryVectors,
                                                       final DistanceFunction distanceFunction, int itemsPerElement) {
        var queryResult = new float[queryVectors[0].length];
        var vectorResult = new float[queryVectors[0].length];

        var nearestVectors = new BoundedGreedyVertexPriorityQueue(itemsPerElement);
        var groundTruth = new int[vectors.length][itemsPerElement];

        for (int i = 0; i < queryVectors.length; i++) {
            var queryVector = distanceFunction.preProcess(queryVectors[i], queryResult);
            nearestVectors.clear();

            for (int j = 0; j < vectors.length; j++) {
                var vector = distanceFunction.preProcess(vectors[j], vectorResult);
                var distance = distanceFunction.computeDistance(vector, 0, queryVector,
                        0, vector.length);
                nearestVectors.add(j, distance, false, false);
            }

            nearestVectors.vertexIndices(groundTruth[i], itemsPerElement);
        }

        return groundTruth;
    }

    private static double recall(int[] results, int[] groundTruths) {
        assert results.length == groundTruths.length;

        int answers = 0;
        for (var result : results) {
            if (ArrayUtils.contains(groundTruths, result)) {
                answers++;
            }
        }

        return answers * 1.0 / groundTruths.length;
    }

    @Test
    public void testL2SearchSift10KVectors() throws IOException {
        runL2Benchmarks(VectorDatasetInfo.Sift10K.INSTANCE);
    }

    @SuppressWarnings("SameParameterValue")
    private void runL2Benchmarks(VectorDatasetInfo datasetInfo) throws IOException {
        var buildPath = requireBuildPath();

        System.out.println("Reading queries...");
        var queryVectors = readQueryVectors(datasetInfo);

        System.out.printf("%d queries are read%n", queryVectors.length);
        System.out.println("Reading ground truth...");

        var groundTruth = readGroundTruthL2(datasetInfo);
        Assert.assertEquals(queryVectors.length, groundTruth.length);

        System.out.println("Ground truth is read");

        System.out.println("Building index...");

        var dbDir = Files.createTempDirectory(buildPath, "testSearchSift10KVectors");
        dbDir.toFile().deleteOnExit();

        var ts1 = System.nanoTime();

        var distance = Distance.L2;
        var dimensions = datasetInfo.getVectorDimensions();

        var indexName = "test_index";
        var vectorFilePath = requireDatasetPath(datasetInfo).resolve(datasetInfo.getBaseFile());
        try (
                var vectorReader = VectorFileReader.openFileReader(vectorFilePath, dimensions, datasetInfo.getVectorCount());
                var dataBuilder = DataStore.create(indexName, dimensions, distance.buildDistanceFunction(), dbDir)
        ) {
            for (int vectorIdx = 0; vectorIdx < datasetInfo.getVectorCount(); vectorIdx++) {
                var vector = vectorReader.read(vectorIdx);
                var vectorId = vectorReader.readId(vectorIdx);
                dataBuilder.add(vector, vectorId);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        IndexBuilder.buildIndex(indexName, dimensions, dbDir, DataStore.dataLocation(indexName, dbDir),
                4 * 1024 * 1024, distance, new ConsolePeriodicProgressTracker(1));
        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        try (var diskCache = new DiskCache(256 * 1024 * 1024, dimensions, IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            try (var indexReader = new IndexReader(indexName, dimensions, dbDir, distance, diskCache)) {
                System.out.println("Searching...");

                var errorsCount = 0;
                ts1 = System.nanoTime();
                for (var queryIdx = 0; queryIdx < queryVectors.length; queryIdx++) {
                    var query = queryVectors[queryIdx];

                    var rawIds = indexReader.nearest(query, 1);
                    Assert.assertEquals("j = " + queryIdx, 1, rawIds.length);
                    var resultId = toVectorId(rawIds[0]);
                    if (groundTruth[queryIdx][0] != resultId) {
                        errorsCount++;
                    }
                }
                ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / queryVectors.length;

                System.out.printf("Avg. query time : %d us, errors: %f%%, cache hits %d%%%n",
                        (ts2 - ts1) / 1000 / queryVectors.length, errorPercentage, indexReader.hits());
                Assert.assertTrue("Error percentage is too high " + errorPercentage + " > 5",
                        errorPercentage <= 5);
                indexReader.deleteIndex();
            }
        }
    }
}

record FloatArrayHolder(float[] floatArray) {
    @Override
    public int hashCode() {
        return Arrays.hashCode(floatArray);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FloatArrayHolder) {
            return Arrays.equals(floatArray, ((FloatArrayHolder) obj).floatArray);
        }
        return false;
    }
}