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
package jetbrains.vectoriadb.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.internal.testing.StreamRecorder;
import jetbrains.vectoriadb.index.CosineDistanceFunction;
import jetbrains.vectoriadb.index.DistanceFunction;
import jetbrains.vectoriadb.index.DotDistanceFunction;
import jetbrains.vectoriadb.index.L2DistanceFunction;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IndexManagerTest {
    @Test
    public void testFindLoadedVectorsL2Distance() throws Exception {
        testIndex("testFindLoadedVectorsL2Distance", L2DistanceFunction.INSTANCE, 0.92);
    }

    @Test
    public void testFindLoadedVectorsDotDistance() throws Exception {
        testIndex("testFindLoadedVectorsDotDistance", DotDistanceFunction.INSTANCE, 0.85);
    }

    @Test
    public void testFindLoadedVectorsCosineDistance() throws Exception {
        testIndex("testFindLoadedVectorsCosineDistance", CosineDistanceFunction.INSTANCE, 0.7);
    }

    @Test
    public void testCreateTwoIndexesSameName() throws Exception {
        var indexName = "testCreateTwoIndexesSameName";
        executeInServiceContext(indexManagerService -> {
            var createIndexRequestBuilder = IndexManagerOuterClass.CreateIndexRequest.newBuilder();
            createIndexRequestBuilder.setIndexName(indexName);
            createIndexRequestBuilder.setDistance(IndexManagerOuterClass.Distance.L2);

            var createIndexRecorder = StreamRecorder.<IndexManagerOuterClass.CreateIndexResponse>create();
            indexManagerService.createIndex(createIndexRequestBuilder.build(), createIndexRecorder);

            checkCompleteness(createIndexRecorder);

            createIndexRecorder = StreamRecorder.create();
            indexManagerService.createIndex(createIndexRequestBuilder.build(), createIndexRecorder);
            var completed = createIndexRecorder.awaitCompletion(1, TimeUnit.MICROSECONDS);
            Assert.assertTrue(completed);

            Assert.assertNotNull(createIndexRecorder.getError());
        });
    }

    @Test
    public void testCreateIndexInSearchMode() throws Exception {
        var indexName = "testCreateIndexInSearchMode";
        executeInServiceContext(indexManagerService -> {
            switchToSearchMode(indexManagerService);

            var createIndexRequestBuilder = IndexManagerOuterClass.CreateIndexRequest.newBuilder();
            createIndexRequestBuilder.setIndexName(indexName);
            createIndexRequestBuilder.setDistance(IndexManagerOuterClass.Distance.L2);

            var createIndexRecorder = StreamRecorder.<IndexManagerOuterClass.CreateIndexResponse>create();
            indexManagerService.createIndex(createIndexRequestBuilder.build(), createIndexRecorder);

            var completed = createIndexRecorder.awaitCompletion(1, TimeUnit.MICROSECONDS);
            Assert.assertTrue(completed);

            Assert.assertNotNull(createIndexRecorder.getError());
        });
    }

    @Test
    public void testSearchInBuildingMode() throws Exception {
        var indexName = "testSearchInBuildingMode";
        executeInServiceContext(indexManagerService -> {
            generateIndex(indexName, L2DistanceFunction.INSTANCE, 64, 10_000, indexManagerService);

            var builder = IndexManagerOuterClass.FindNearestNeighboursRequest.newBuilder();
            builder.setIndexName(indexName);
            builder.setK(1);

            for (var component : new float[64]) {
                builder.addVectorComponents(component);
            }

            var findNearestVectorsRecorder = StreamRecorder.<IndexManagerOuterClass.FindNearestNeighboursResponse>create();
            indexManagerService.findNearestNeighbours(builder.build(), findNearestVectorsRecorder);

            var completed = findNearestVectorsRecorder.awaitCompletion(1, TimeUnit.MICROSECONDS);
            Assert.assertTrue(completed);

            Assert.assertNotNull(findNearestVectorsRecorder.getError());
        });
    }

    @Test
    public void testBuildNotExistingIndex() throws Exception {
        var indexName = "testBuildNotExistingIndex";
        executeInServiceContext(indexManagerService -> {
            var indexNameRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
            indexNameRequestBuilder.setIndexName(indexName);

            var buildIndexRecorder = StreamRecorder.<Empty>create();
            indexManagerService.triggerIndexBuild(indexNameRequestBuilder.build(), buildIndexRecorder);

            var completed = buildIndexRecorder.awaitCompletion(1, TimeUnit.MICROSECONDS);
            Assert.assertTrue(completed);

            Assert.assertNotNull(buildIndexRecorder.getError());
        });
    }

    @Test
    public void testListIndexes() throws Exception {
        var indexName = "testListIndexes";
        executeInServiceContext(indexManagerService -> {
            var indexes = listIndexes(indexManagerService);
            Assert.assertTrue(indexes.isEmpty());

            createIndex(indexName + 1, indexManagerService, IndexManagerOuterClass.Distance.L2);

            indexes = listIndexes(indexManagerService);
            Assert.assertEquals(1, indexes.size());
            Assert.assertEquals(indexName + 1, indexes.get(0));

            createIndex(indexName + 2, indexManagerService, IndexManagerOuterClass.Distance.L2);
            indexes = listIndexes(indexManagerService);
            Assert.assertEquals(2, indexes.size());

            Assert.assertTrue(indexes.contains(indexName + 1));
            Assert.assertTrue(indexes.contains(indexName + 2));

            createIndex(indexName + 3, indexManagerService, IndexManagerOuterClass.Distance.L2);
            indexes = listIndexes(indexManagerService);
            Assert.assertEquals(3, indexes.size());

            Assert.assertTrue(indexes.contains(indexName + 1));
            Assert.assertTrue(indexes.contains(indexName + 2));
            Assert.assertTrue(indexes.contains(indexName + 3));
        });
    }

    @Test
    public void testDropCreatedIndex() throws Exception {
        var indexOne = "testDeleteCreatedIndexOne";
        var indexTwo = "testDeleteCreatedIndexTwo";
        var indexThree = "testDeleteCreatedIndexThree";

        executeInServiceContext(indexManagerService -> {
            createIndex(indexOne, indexManagerService, IndexManagerOuterClass.Distance.L2);
            createIndex(indexTwo, indexManagerService, IndexManagerOuterClass.Distance.L2);
            createIndex(indexThree, indexManagerService, IndexManagerOuterClass.Distance.L2);

            var indexes = listIndexes(indexManagerService);
            Assert.assertEquals(3, indexes.size());

            dropIndex(indexTwo, indexManagerService);

            indexes = listIndexes(indexManagerService);
            Assert.assertEquals(2, indexes.size());

            Assert.assertTrue(indexes.contains(indexOne));
            Assert.assertTrue(indexes.contains(indexThree));
        });
    }


    @Test
    public void testDropBuiltIndex() throws Exception {
        var indexOne = "testDeleteBuildIndexOne";
        var indexTwo = "testDeleteBuiltIndexTwo";
        var indexThree = "testDeleteBuiltIndexThree";

        executeInServiceContext(indexManagerService -> {
            generateIndex(indexOne, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);
            generateIndex(indexTwo, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);
            generateIndex(indexThree, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);

            var indexes = listIndexes(indexManagerService);
            Assert.assertEquals(3, indexes.size());

            dropIndex(indexTwo, indexManagerService);

            indexes = listIndexes(indexManagerService);
            Assert.assertEquals(2, indexes.size());

            Assert.assertTrue(indexes.contains(indexOne));
            Assert.assertTrue(indexes.contains(indexThree));
        });
    }

    @Test
    public void testDropSearchIndex() throws Exception {
        var indexOne = "testDropSearchIndexOne";
        var indexTwo = "testDropSearchIndexTwo";
        var indexThree = "testDropSearchIndexThree";

        executeInServiceContext(indexManagerService -> {
            generateIndex(indexOne, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);
            generateIndex(indexTwo, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);
            generateIndex(indexThree, L2DistanceFunction.INSTANCE, 64, 1, indexManagerService);

            switchToSearchMode(indexManagerService);

            dropIndex(indexTwo, indexManagerService);

            var indexes = listIndexes(indexManagerService);
            Assert.assertEquals(2, indexes.size());

            Assert.assertTrue(indexes.contains(indexOne));
            Assert.assertTrue(indexes.contains(indexThree));
        });
    }

    @Test
    public void testTwoIndexesSimultaniously() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var indexNamePrefix = "testTwoIndexesSimultaniously";
        var dbDir = Path.of(buildDir).resolve("vectoriadb");
        if (Files.exists(dbDir)) {
            FileUtils.deleteDirectory(dbDir.toFile());
        }

        var recallCount = 5;
        var vectorDimensions = 64;
        var vectorsCount = 10_000;

        var environment = new MockEnvironment();

        environment.setProperty(IndexManagerServiceImpl.MAX_CONNECTIONS_PER_VERTEX_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.MAX_CANDIDATES_RETURNED_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.COMPRESSION_RATIO_PROPERTY, String.valueOf(32));
        environment.setProperty(IndexManagerServiceImpl.DISTANCE_MULTIPLIER_PROPERTY, String.valueOf(2.0));

        environment.setProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY, dbDir.toAbsolutePath().toString());
        environment.setProperty(IndexManagerServiceImpl.INDEX_DIMENSIONS_PROPERTY, String.valueOf(vectorDimensions));
        environment.setProperty(IndexManagerServiceImpl.INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY,
                String.valueOf(128 * 1024 * 1024));
        environment.setProperty(IndexManagerServiceImpl.INDEX_SEARCH_DISK_CACHE_MEMORY_CONSUMPTION,
                String.valueOf(128 * 1024 * 1024));

        var indexManagerService = new IndexManagerServiceImpl(environment);
        try {
            var buildFutures = new ArrayList<Future<float[][]>>();

            try (var executor = Executors.newFixedThreadPool(2)) {
                for (int i = 0; i < 2; i++) {
                    var indexName = indexNamePrefix + i;

                    buildFutures.add(executor.submit(() ->
                            generateIndex(indexName, L2DistanceFunction.INSTANCE,
                                    vectorDimensions, vectorsCount, indexManagerService)));
                }

                var vectorsList = new ArrayList<float[][]>();
                for (var future : buildFutures) {
                    vectorsList.add(future.get());
                }

                switchToSearchMode(indexManagerService);

                var searchFutures = new ArrayList<Future<Void>>();

                for (int i = 0; i < 2; i++) {
                    var indexName = indexNamePrefix + i;
                    var vectors = vectorsList.get(i);

                    searchFutures.add(executor.submit(() -> {
                        searchNeighbours(indexName, vectorsCount, vectorDimensions, recallCount, 0.92,
                                vectors, L2DistanceFunction.INSTANCE, indexManagerService);
                        return null;
                    }));
                }

                for (var future : searchFutures) {
                    future.get();
                }
            }
        } finally {
            indexManagerService.shutdown();
        }

        if (Files.exists(dbDir)) {
            FileUtils.deleteDirectory(dbDir.toFile());
        }
    }

    @Test
    public void testShutDownAndReload() throws Exception {
        var createdIndex = "testShutDownAndReloadCreated";
        var uploadedIndex = "testShutDownAndReloadUploaded";
        var builtIndex = "testShutDownAndReloadBuilt";

        executeInServiceContext(true, false,
                indexManagerService -> {
                    createIndex(createdIndex, indexManagerService, IndexManagerOuterClass.Distance.L2);

                    var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
                    var vectors = new float[10][64];
                    generateUniqueVectorSet(vectors, rng);
                    var ids = new byte[10][];

                    for (int i = 0; i < ids.length; i++) {
                        ids[i] = new byte[16];
                        ByteBuffer.wrap(ids[i]).order(ByteOrder.LITTLE_ENDIAN).putInt(i);
                    }

                    createIndex(uploadedIndex, indexManagerService, IndexManagerOuterClass.Distance.L2);
                    uploadVectors(uploadedIndex, vectors, ids, indexManagerService);

                    generateIndex(builtIndex, L2DistanceFunction.INSTANCE, 64, 10,
                            indexManagerService);
                });

        executeInServiceContext(false, true, indexManagerService -> {
            var currentIndexes = listIndexes(indexManagerService);
            Assert.assertEquals(3, currentIndexes.size());

            Assert.assertTrue(currentIndexes.contains(createdIndex));
            Assert.assertTrue(currentIndexes.contains(uploadedIndex));
            Assert.assertTrue(currentIndexes.contains(builtIndex));
        });
    }

    @NotNull
    private static IndexManagerServiceImpl initIndexService(String buildDir) throws IOException {
        var environment = new MockEnvironment();

        environment.setProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY, buildDir);
        environment.setProperty(IndexManagerServiceImpl.INDEX_DIMENSIONS_PROPERTY, String.valueOf(64));

        environment.setProperty(IndexManagerServiceImpl.MAX_CONNECTIONS_PER_VERTEX_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.MAX_CANDIDATES_RETURNED_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.COMPRESSION_RATIO_PROPERTY, String.valueOf(32));
        environment.setProperty(IndexManagerServiceImpl.DISTANCE_MULTIPLIER_PROPERTY, String.valueOf(2.0));

        environment.setProperty(IndexManagerServiceImpl.INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY,
                String.valueOf(64 * 1024 * 1024));
        environment.setProperty(IndexManagerServiceImpl.INDEX_SEARCH_DISK_CACHE_MEMORY_CONSUMPTION,
                String.valueOf(64 * 1024 * 1024));

        return new IndexManagerServiceImpl(environment);
    }

    private static void executeInServiceContext(IndexServiceCode code) throws Exception {
        executeInServiceContext(true, true, code);
    }

    private static void executeInServiceContext(boolean preDeleteDirectories, boolean deleteDirectoriesOnExit,
                                                IndexServiceCode code) throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var dbDir = Path.of(buildDir).resolve("vectoriadb");
        if (preDeleteDirectories) {
            FileUtils.deleteDirectory(dbDir.toFile());
        }

        var indexManagerService = initIndexService(dbDir.toAbsolutePath().toString());
        try {
            code.execute(indexManagerService);
        } finally {
            indexManagerService.shutdown();

            if (deleteDirectoriesOnExit) {
                FileUtils.deleteDirectory(dbDir.toFile());
            }
        }
    }

    private static void uploadVectors(String indexName, float[][] vectors, byte[][] ids,
                                      IndexManagerServiceImpl indexManagerService) throws Exception {
        var vectorsUploadRecorder = StreamRecorder.<Empty>create();
        var request = indexManagerService.uploadVectors(vectorsUploadRecorder);
        try {
            for (var i = 0; i < vectors.length; i++) {
                var vector = vectors[i];
                var id = ids[i];

                var builder = IndexManagerOuterClass.UploadVectorsRequest.newBuilder();
                builder.setIndexName(indexName);

                for (var component : vector) {
                    builder.addVectorComponents(component);
                }
                builder.setId(IndexManagerOuterClass.VectorId.newBuilder().setId(ByteString.copyFrom(id)).build());

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

    private static void dropIndex(String indexName, IndexManagerServiceImpl indexManagerService) throws Exception {
        var dropIndexRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        dropIndexRequestBuilder.setIndexName(indexName);

        var dropIndexRecorder = StreamRecorder.<Empty>create();
        indexManagerService.dropIndex(dropIndexRequestBuilder.build(), dropIndexRecorder);

        checkCompleteness(dropIndexRecorder);
    }

    private static List<String> listIndexes(IndexManagerServiceImpl indexManagerService) throws Exception {
        var listIndexesRecorder = StreamRecorder.<IndexManagerOuterClass.IndexListResponse>create();
        indexManagerService.listIndexes(Empty.newBuilder().build(), listIndexesRecorder);

        checkCompleteness(listIndexesRecorder);
        return listIndexesRecorder.getValues().get(0).getIndexNamesList();
    }


    private static void buildIndex(String indexName, IndexManagerServiceImpl indexManagerService) throws Exception {
        var indexNameRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        indexNameRequestBuilder.setIndexName(indexName);

        var buildIndexRecorder = StreamRecorder.<Empty>create();
        indexManagerService.triggerIndexBuild(indexNameRequestBuilder.build(), buildIndexRecorder);

        checkCompleteness(buildIndexRecorder);
        while (true) {
            var indexStateRequestBuilder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
            indexStateRequestBuilder.setIndexName(indexName);

            var indexStateRecorder = StreamRecorder.<IndexManagerOuterClass.IndexStateResponse>create();
            indexManagerService.retrieveIndexState(indexStateRequestBuilder.build(), indexStateRecorder);

            checkCompleteness(indexStateRecorder);

            var response = indexStateRecorder.getValues().get(0);
            var indexState = response.getState();
            if (indexState == IndexManagerOuterClass.IndexState.BUILDING ||
                    indexState == IndexManagerOuterClass.IndexState.IN_BUILD_QUEUE) {
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

    private static byte[][] findNearestNeighbours(IndexManagerServiceImpl indexManagerService,
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
        var result = new byte[nearestVectors.size()][];

        for (int i = 0; i < nearestVectors.size(); i++) {
            result[i] = nearestVectors.get(i).getId().toByteArray();
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

    private static float[][] generateIndex(String indexName, DistanceFunction distanceFunction, int vectorDimensions, int vectorsCount,
                                           IndexManagerServiceImpl indexManagerService) throws Exception {
        var distance = convertDistanceFunction(distanceFunction);

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

        var vectors = new float[vectorsCount][vectorDimensions];


        generateUniqueVectorSet(vectors, rng);
        var ids = new byte[vectorsCount][];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = new byte[16];
            ByteBuffer.wrap(ids[i]).order(ByteOrder.LITTLE_ENDIAN).putInt(i);
        }

        createIndex(indexName, indexManagerService, distance);
        uploadVectors(indexName, vectors, ids, indexManagerService);

        var ts1 = System.nanoTime();
        buildIndex(indexName, indexManagerService);
        var ts2 = System.nanoTime();

        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
        return vectors;
    }

    private static void searchNeighbours(String indexName, int vectorsCount, int vectorDimensions, int recallCount,
                                         double recallThreshold, float[][] vectors, DistanceFunction distanceFunction,
                                         IndexManagerServiceImpl indexManagerService) throws Exception {
        var ts1 = System.nanoTime();
        var totalRecall = 0.0;

        var queries = new float[vectorsCount][vectorDimensions];

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        generateUniqueVectorSet(queries, rng);

        var groundTruth = calculateGroundTruthVectors(vectors, queries, distanceFunction, recallCount);


        for (var j = 0; j < vectorsCount; j++) {
            var vector = queries[j];
            var rawIds = findNearestNeighbours(indexManagerService, vector, indexName, recallCount);

            var result = new int[rawIds.length];
            for (int i = 0; i < rawIds.length; i++) {
                result[i] = ByteBuffer.wrap(rawIds[i]).order(ByteOrder.LITTLE_ENDIAN).getInt();
            }

            totalRecall += recall(result, groundTruth[j]);

            if ((j + 1) % 1_000 == 0) {
                System.out.println("Processed " + (j + 1));
            }
        }

        var ts2 = System.nanoTime();
        var recall = totalRecall / vectorsCount;

        System.out.printf("Avg. query %d time us, R@%d : %f%n",
                (ts2 - ts1) / 1000 / vectorsCount, recallCount, recall);
        Assert.assertTrue("Recall is too low " + recall + " < " + recallThreshold,
                recall >= recallThreshold);
    }

    @NotNull
    private static IndexManagerOuterClass.Distance convertDistanceFunction(DistanceFunction distanceFunction) {
        return switch (distanceFunction) {
            case L2DistanceFunction l2DistanceFunction -> IndexManagerOuterClass.Distance.L2;
            case DotDistanceFunction dotDistanceFunction -> IndexManagerOuterClass.Distance.DOT;
            case CosineDistanceFunction cosineDistanceFunction -> IndexManagerOuterClass.Distance.COSINE;
            case null, default -> throw new IllegalArgumentException("Unknown distance function " + distanceFunction);
        };
    }

    private static void testIndex(String indexName, DistanceFunction distanceFunction, double recallThreshold) throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }


        var dbDir = Path.of(buildDir).resolve("vectoriadb");
        if (Files.exists(dbDir)) {
            FileUtils.deleteDirectory(dbDir.toFile());
        }

        var recallCount = 5;
        var vectorDimensions = 64;
        var vectorsCount = 10_000;

        var environment = new MockEnvironment();
        environment.setProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY, dbDir.toString());
        environment.setProperty(IndexManagerServiceImpl.INDEX_DIMENSIONS_PROPERTY, String.valueOf(vectorDimensions));
        environment.setProperty(IndexManagerServiceImpl.MAX_CONNECTIONS_PER_VERTEX_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.MAX_CANDIDATES_RETURNED_PROPERTY, String.valueOf(128));
        environment.setProperty(IndexManagerServiceImpl.COMPRESSION_RATIO_PROPERTY, String.valueOf(32));
        environment.setProperty(IndexManagerServiceImpl.DISTANCE_MULTIPLIER_PROPERTY, String.valueOf(2.0));

        environment.setProperty(IndexManagerServiceImpl.INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY,
                String.valueOf(64 * 1024 * 1024));
        environment.setProperty(IndexManagerServiceImpl.INDEX_SEARCH_DISK_CACHE_MEMORY_CONSUMPTION,
                String.valueOf(64 * 1024 * 1024));

        var indexManagerService = new IndexManagerServiceImpl(environment);
        try {
            var vectors = generateIndex(indexName, distanceFunction, vectorDimensions, vectorsCount, indexManagerService);

            switchToSearchMode(indexManagerService);

            searchNeighbours(indexName, vectorsCount, vectorDimensions, recallCount, recallThreshold, vectors,
                    distanceFunction, indexManagerService);
        } finally {
            indexManagerService.shutdown();

            if (Files.exists(dbDir)) {
                FileUtils.deleteDirectory(dbDir.toFile());
            }
        }
    }

    @FunctionalInterface
    private interface IndexServiceCode {
        void execute(IndexManagerServiceImpl indexManagerService) throws Exception;
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
