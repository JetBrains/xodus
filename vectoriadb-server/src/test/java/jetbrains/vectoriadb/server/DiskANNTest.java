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
package jetbrains.vectoriadb.server;

import com.google.protobuf.Empty;
import io.grpc.internal.testing.StreamRecorder;
import jetbrains.vectoriadb.index.DistanceFunction;
import jetbrains.vectoriadb.index.DotDistanceFunction;
import jetbrains.vectoriadb.index.L2DistanceFunction;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DiskANNTest {
    @Test
    public void testFindLoadedVectorsL2Distance() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var indexName = "testFindLoadedVectorsL2Distance";
        var indexDir = Path.of(buildDir).resolve(indexName);

        if (Files.exists(indexDir)) {
            FileUtils.deleteDirectory(indexDir.toFile());
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

        var environment = new MockEnvironment();
        environment.setProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY, buildDir);
        environment.setProperty(IndexManagerServiceImpl.INDEX_DIMENSIONS_PROPERTY, String.valueOf(vectorDimensions));

        var indexManagerService = new IndexManagerServiceImpl(environment);
        try {
            createIndex(indexName, indexManagerService, IndexManagerOuterClass.Distance.L2);
            uploadVectors(indexManagerService, vectors, indexName);

            var ts1 = System.nanoTime();
            buildIndex(indexName, indexManagerService);
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

            switchToSearchMode(indexManagerService);

            ts1 = System.nanoTime();
            var totalRecall = 0.0;

            for (var j = 0; j < vectorsCount; j++) {
                var vector = queries[j];
                var result = findNearestNeighbours(indexManagerService, vector, indexName, recallCount);
                totalRecall += recall(result, groundTruth[j]);

                if ((j + 1) % 1_000 == 0) {
                    System.out.println("Processed " + (j + 1));
                }
            }

            ts2 = System.nanoTime();
            var recall = totalRecall / vectorsCount;

            System.out.printf("Avg. query %d time us, R@%d : %f%n",
                    (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall);
            Assert.assertTrue("Recall is too low " + recall + " < 0.92",
                    recall >= 0.92);
        } finally {
            indexManagerService.shutdown();
        }

        if (Files.exists(indexDir)) {
            FileUtils.deleteDirectory(indexDir.toFile());
        }
    }

    @Test
    public void testFindLoadedVectorsDotDistance() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var indexName = "testFindLoadedVectorsDotDistance";
        var indexDir = Path.of(buildDir).resolve(indexName);

        if (Files.exists(indexDir)) {
            FileUtils.deleteDirectory(indexDir.toFile());
        }

        var recallCount = 5;
        var vectorDimensions = 64;
        var vectorsCount = 10_000;

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

        var vectors = new float[vectorsCount][vectorDimensions];
        var queries = new float[vectorsCount][vectorDimensions];

        generateUniqueVectorSet(vectors, rng);
        generateUniqueVectorSet(queries, rng);

        var groundTruth = calculateGroundTruthVectors(vectors, queries, DotDistanceFunction.INSTANCE, recallCount);

        var environment = new MockEnvironment();
        environment.setProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY, buildDir);
        environment.setProperty(IndexManagerServiceImpl.INDEX_DIMENSIONS_PROPERTY, String.valueOf(vectorDimensions));

        var indexManagerService = new IndexManagerServiceImpl(environment);
        try {
            createIndex(indexName, indexManagerService, IndexManagerOuterClass.Distance.DOT);
            uploadVectors(indexManagerService, vectors, indexName);

            var ts1 = System.nanoTime();
            buildIndex(indexName, indexManagerService);
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

            switchToSearchMode(indexManagerService);

            ts1 = System.nanoTime();
            var totalRecall = 0.0;

            for (var j = 0; j < vectorsCount; j++) {
                var vector = queries[j];
                var result = findNearestNeighbours(indexManagerService, vector, indexName, recallCount);
                totalRecall += recall(result, groundTruth[j]);

                if ((j + 1) % 1_000 == 0) {
                    System.out.println("Processed " + (j + 1));
                }
            }

            ts2 = System.nanoTime();
            var recall = totalRecall / vectorsCount;

            System.out.printf("Avg. query %d time us, R@%d : %f%n",
                    (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall);
            Assert.assertTrue("Recall is too low " + recall + " < 0.85",
                    recall >= 0.85);
        } finally {
            indexManagerService.shutdown();
        }

        if (Files.exists(indexDir)) {
            FileUtils.deleteDirectory(indexDir.toFile());
        }
    }


    private static void uploadVectors(IndexManagerServiceImpl indexManagerService,
                                      float[][] vectors, String indexName) throws Exception {
        var vectorsUploadRecorder = StreamRecorder.<Empty>create();
        var request = indexManagerService.uploadVectors(vectorsUploadRecorder);
        try {
            for (var vector : vectors) {
                var builder = IndexManagerOuterClass.UploadVectorsRequest.newBuilder();
                builder.setIndexName(indexName);

                for (var component : vector) {
                    builder.addVectorComponents(component);
                }

                request.onNext(builder.build());
                if (vectorsUploadRecorder.getError() != null) {
                    break;
                }
            }

            if (vectorsUploadRecorder.getError() != null) {
                Assert.fail(vectorsUploadRecorder.getError().getMessage());
            }
            request.onCompleted();
        } catch (Exception e) {
            request.onError(e);
            throw e;
        }

        checkCompleteness(vectorsUploadRecorder);
    }

    private static void createIndex(String indexName, IndexManagerServiceImpl indexManagerService,
                                    IndexManagerOuterClass.Distance distance) throws Exception {
        var createIndexRequestBuilder = IndexManagerOuterClass.CreateIndexRequest.newBuilder();
        createIndexRequestBuilder.setIndexName(indexName);
        createIndexRequestBuilder.setDistance(distance);

        var createIndexRecorder = StreamRecorder.<IndexManagerOuterClass.CreateIndexResponse>create();
        indexManagerService.createIndex(createIndexRequestBuilder.build(), createIndexRecorder);

        checkCompleteness(createIndexRecorder);
    }

    private static void buildIndex(String indexName, IndexManagerServiceImpl indexManagerService) throws Exception {
        var indexNameRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        indexNameRequestBuilder.setIndexName(indexName);

        var buildIndexRecorder = StreamRecorder.<Empty>create();
        indexManagerService.buildIndex(indexNameRequestBuilder.build(), buildIndexRecorder);

        checkCompleteness(buildIndexRecorder);
        while (true) {
            var indexStateRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
            indexStateRequestBuilder.setIndexName(indexName);

            var indexStateRecorder = StreamRecorder.<IndexManagerOuterClass.IndexStateResponse>create();
            indexManagerService.indexState(indexStateRequestBuilder.build(), indexStateRecorder);

            checkCompleteness(indexStateRecorder);

            var response = indexStateRecorder.getValues().get(0);
            var indexState = response.getState();
            if (indexState == IndexManagerOuterClass.IndexState.BUILDING || indexState == IndexManagerOuterClass.IndexState.IN_BUILD_QUEUE) {
                //noinspection BusyWait
                Thread.sleep(100);
            } else if (indexState == IndexManagerOuterClass.IndexState.BUILT) {
                break;
            } else {
                Assert.fail("unexpected index state " + indexState);
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

    private static void switchToSearchMode(IndexManagerServiceImpl indexManagerService) throws Exception {
        var switchToSearchModeRecorder = StreamRecorder.<Empty>create();
        indexManagerService.switchToSearchMode(Empty.newBuilder().build(), switchToSearchModeRecorder);

        checkCompleteness(switchToSearchModeRecorder);
    }

    private static int[] findNearestNeighbours(IndexManagerServiceImpl indexManagerService,
                                               float[] queryVector, String indexName, int k) throws Exception {
        var findNearestVectorsRecorder = StreamRecorder.<IndexManagerOuterClass.FindNearestNeighboursResponse>create();
        var builder = IndexManagerOuterClass.FindNearestNeighboursRequest.newBuilder();
        builder.setIndexName(indexName);
        builder.setK(k);

        for (var component : queryVector) {
            builder.addVectorComponents(component);
        }

        indexManagerService.findNearestNeighbours(builder.build(), findNearestVectorsRecorder);

        checkCompleteness(findNearestVectorsRecorder);

        var response = findNearestVectorsRecorder.getValues().get(0);
        var nearestVectors = response.getIdsList();
        var result = new int[nearestVectors.size()];

        for (int i = 0; i < nearestVectors.size(); i++) {
            result[i] = nearestVectors.get(i);
        }

        return result;
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

    private static void checkCompleteness(StreamRecorder<?> recorder) throws Exception {
        var completed = recorder.awaitCompletion(1, TimeUnit.MICROSECONDS);
        Assert.assertTrue(completed);
        if (recorder.getError() != null) {
            Assert.fail(recorder.getError().getMessage());
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
