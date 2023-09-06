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

import jetbrains.exodus.diskann.siftbench.SiftBenchUtils;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Random;

public class PQKMeansTest {
    @Test
    public void distanceTablesTest() {
        var seed = System.nanoTime();
        System.out.println("distanceTablesTest seed = " + seed);
        var rnd = new Random(seed);

        var quantizersCount = 4;
        var centroids = generateDistanceTable(quantizersCount, rnd);
        var expectedDistanceTables = createDistanceTable(quantizersCount, centroids, null);

        var actualDistanceTables = PQKMeans.distanceTables(centroids, new L2DistanceFunction());
        Assert.assertArrayEquals(expectedDistanceTables, actualDistanceTables, 0.0f);
    }


    @Test
    public void symmetricDistanceTest() {
        var seed = System.nanoTime();
        System.out.println("symmetricDistanceTest seed = " + seed);
        var rnd = new Random(seed);

        var quantizersCount = 4;
        var centroids = generateDistanceTable(quantizersCount, rnd);
        var distanceTables = new float[quantizersCount][PQ.PQ_CODE_BASE_SIZE][PQ.PQ_CODE_BASE_SIZE];
        var pqVectors = new byte[100][quantizersCount];
        var flatDistanceTables = createDistanceTable(quantizersCount, centroids, distanceTables);

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < quantizersCount; j++) {
                pqVectors[i][j] = (byte) rnd.nextInt(256);
            }
        }


        try (var arena = Arena.openShared()) {
            var flatDirectPqVectors = arena.allocateArray(ValueLayout.JAVA_BYTE, 100 * quantizersCount);
            var flatHeapVectors = new byte[100 * quantizersCount];

            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < quantizersCount; j++) {
                    flatDirectPqVectors.set(ValueLayout.JAVA_BYTE,
                            MatrixOperations.twoDMatrixIndex(quantizersCount, i, j), pqVectors[i][j]);
                    flatHeapVectors[MatrixOperations.twoDMatrixIndex(quantizersCount, i, j)] = pqVectors[i][j];
                }
            }

            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 100; j++) {
                    var expectedDistance = 0.0f;

                    for (int k = 0; k < quantizersCount; k++) {
                        var firstCode = Byte.toUnsignedInt(pqVectors[i][k]);
                        var secondCode = Byte.toUnsignedInt(pqVectors[j][k]);

                        expectedDistance += distanceTables[k][firstCode][secondCode];
                    }

                    var actualDistance = PQKMeans.symmetricDistance(flatDirectPqVectors, i, flatHeapVectors, j,
                            flatDistanceTables, quantizersCount, PQ.PQ_CODE_BASE_SIZE);
                    Assert.assertEquals(expectedDistance, actualDistance, 0.0f);
                }
            }
        }
    }

    @Test
    public void testPQKmeansQualityVsKMeansL2() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var siftArchive = "siftsmall.tar.gz";
        SiftBenchUtils.downloadSiftBenchmark(siftArchive, buildDir);

        var siftSmallDir = SiftBenchUtils.extractSiftDataSet(siftArchive, buildDir);

        var siftDir = "siftsmall";
        var sifSmallFilesDir = siftSmallDir.toPath().resolve(siftDir);

        var siftBaseName = "siftsmall_base.fvecs";
        var siftSmallBase = sifSmallFilesDir.resolve(siftBaseName);

        System.out.println("Reading data vectors...");

        var vectorDimensions = 128;
        var vectors = SiftBenchUtils.readFVectors(siftSmallBase, vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d%n",
                vectors.length, vectorDimensions);

        var clustersCount = 40;

        var pqParameters = PQ.calculatePQParameters(vectorDimensions, 32);
        var pqQuantizersCount = pqParameters.pqQuantizersCount;
        var pqSubVectorSize = pqParameters.pqSubVectorSize;

        var vectorsByClusters = new ArrayList<ArrayList<float[]>>();
        for (int i = 0; i < clustersCount; i++) {
            vectorsByClusters.add(new ArrayList<>());
        }

        try (var arena = Arena.openShared()) {
            System.out.println("Generating PQ codes...");
            var pqResult = PQ.generatePQCodes(pqQuantizersCount, pqSubVectorSize, new L2DistanceFunction(),
                    new ArrayVectorReader(vectors), arena);
            System.out.println("PQ codes generated. Calculating centroids...");
            var centroids = PQKMeans.calculatePartitions(pqResult.pqCentroids, pqResult.pqVectors,
                    clustersCount, 1_000, new L2DistanceFunction());
            System.out.println("Centroids calculated. Clustering data vectors...");

            for (float[] vector : vectors) {
                var clusterIndex = findClosestCentroid(centroids, pqResult.pqCentroids, vector,
                        pqQuantizersCount, pqSubVectorSize);
                vectorsByClusters.get(clusterIndex).add(vector);
            }

            System.out.println("Data vectors clustered. Calculating silhouette coefficient...");

            var interClusterDistancePQ = interClusterDistancePQ(centroids, pqResult.pqCentroids, pqQuantizersCount
            );
            var intraClusterDistancePQ = 0.0f;

            for (int i = 0; i < clustersCount; i++) {
                var clusterVectors = vectorsByClusters.get(i).toArray(new float[0][]);
                intraClusterDistancePQ += intraClusterDistancePQ(clusterVectors);
            }

            intraClusterDistancePQ /= clustersCount;

            var silhouetteCoefficientPQ = (interClusterDistancePQ - intraClusterDistancePQ) / Math.max(interClusterDistancePQ,
                    intraClusterDistancePQ);

            System.out.printf("interClusterDistancePQ = %f%n", interClusterDistancePQ);
            System.out.printf("intraClusterDistancePQ = %f%n", intraClusterDistancePQ);
            System.out.printf("silhouetteCoefficientPQ = %f%n", silhouetteCoefficientPQ);


            System.out.println("Creation of clusterable vectors for Apache Commons ...");
            var clusterableVectors = new ArrayList<Clusterable>();
            for (float[] vector : vectors) {
                var doubleVector = new double[vector.length];
                for (int j = 0; j < vector.length; j++) {
                    doubleVector[j] = vector[j];
                }
                clusterableVectors.add(new DoublePoint(doubleVector));
            }

            var kMeansPlusPlus = new KMeansPlusPlusClusterer<>(clustersCount, 1_000);
            System.out.println("Clustering data vectors using Apache Commons ...");
            var kMeansPlusPlusClusters = kMeansPlusPlus.cluster(clusterableVectors);

            System.out.println("Data vectors clustered. " +
                    "Calculating silhouette coefficient for result created by Apache Commons...");

            var interClusterDistanceApache = 0.0;
            var count = 0;
            for (int i = 0; i < clustersCount; i++) {
                var firstPoint = kMeansPlusPlusClusters.get(i).getCenter().getPoint();
                for (int j = i + 1; j < clustersCount; j++) {
                    var secondPoint = kMeansPlusPlusClusters.get(j).getCenter().getPoint();

                    for (int n = 0; n < vectorDimensions; n++) {
                        interClusterDistanceApache += Math.pow(firstPoint[n] - secondPoint[n], 2);
                    }
                    count++;
                }
            }
            interClusterDistanceApache /= count;

            var intraClusterDistanceApache = 0.0;
            for (int i = 0; i < clustersCount; i++) {

                var singleClusterDistance = 0.0;
                count = 0;
                var points = kMeansPlusPlusClusters.get(i).getPoints();
                for (int j = 0; j < points.size(); j++) {
                    var firstPoint = points.get(j).getPoint();
                    for (int k = j + 1; k < points.size(); k++) {
                        var secondPoint = points.get(k).getPoint();

                        for (int n = 0; n < vectorDimensions; n++) {
                            singleClusterDistance += Math.pow(firstPoint[n] - secondPoint[n], 2);

                        }
                        count++;
                    }
                }
                singleClusterDistance /= count;

                intraClusterDistanceApache += singleClusterDistance;
            }
            intraClusterDistanceApache /= clustersCount;

            var silhouetteCoefficientApache = (interClusterDistanceApache - intraClusterDistanceApache) / Math.max(interClusterDistanceApache,
                    intraClusterDistanceApache);

            System.out.printf("Apache Commons interClusterDistanceApache = %f%n", interClusterDistanceApache);
            System.out.printf("Apache Commons intraClusterDistanceApache = %f%n", intraClusterDistanceApache);
            System.out.printf("Apache Commons  silhouetteCoefficientApache = %f%n", silhouetteCoefficientApache);

            Assert.assertTrue(silhouetteCoefficientPQ >= silhouetteCoefficientApache ||
                    silhouetteCoefficientApache - silhouetteCoefficientPQ < 0.1);
        }
    }

    private static float[][][] generateDistanceTable(int quantizersCount, Random rnd) {
        float[][][] centroids = new float[quantizersCount][PQ.PQ_CODE_BASE_SIZE][PQ.PQ_CODE_BASE_SIZE];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < PQ.PQ_CODE_BASE_SIZE; j++) {
                var vector = new float[PQ.PQ_CODE_BASE_SIZE];
                for (int k = 0; k < PQ.PQ_CODE_BASE_SIZE; k++) {
                    vector[k] = rnd.nextFloat();
                }

                centroids[i][j] = vector;
            }
        }

        return centroids;
    }

    @NotNull
    private static float[] createDistanceTable(int quantizersCount, float[][][] centroids, float[][][] distanceTable) {
        var expectedDistanceTables = new float[4 * PQ.PQ_CODE_BASE_SIZE * PQ.PQ_CODE_BASE_SIZE];
        var index = 0;

        var l2Distance = new L2DistanceFunction();
        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < PQ.PQ_CODE_BASE_SIZE; j++) {
                for (int k = 0; k < PQ.PQ_CODE_BASE_SIZE; k++) {
                    expectedDistanceTables[index] = l2Distance.computeDistance(centroids[i][j], 0,
                            centroids[i][k], 0, PQ.PQ_CODE_BASE_SIZE);
                    if (distanceTable != null) {
                        distanceTable[i][j][k] = expectedDistanceTables[index];
                    }

                    index++;
                }
            }
        }
        return expectedDistanceTables;
    }

    private static float interClusterDistancePQ(byte[] centroids, float[][][] pqCentroids, int pqQuantizersCount) {
        var distance = 0.0f;
        var count = 0;

        var l2Distance = new L2DistanceFunction();
        var numVectors = centroids.length / pqQuantizersCount;
        for (int i = 0; i < numVectors; i++) {
            for (int j = i + 1; j < numVectors; j++) {
                var firstIndex = MatrixOperations.twoDMatrixIndex(pqQuantizersCount, i, 0);
                var secondIndex = MatrixOperations.twoDMatrixIndex(pqQuantizersCount, j, 0);

                for (int k = 0; k < pqQuantizersCount; k++) {
                    var firstCode = Byte.toUnsignedInt(centroids[firstIndex + k]);
                    var secondCode = Byte.toUnsignedInt(centroids[secondIndex + k]);

                    var firstVector = pqCentroids[k][firstCode];
                    var secondVector = pqCentroids[k][secondCode];

                    distance += l2Distance.computeDistance(firstVector, 0, secondVector, 0,
                            firstVector.length);
                }

                count++;
            }
        }

        return distance / count;
    }

    private static float intraClusterDistancePQ(float[][] vectors) {
        var distance = 0.0f;
        var count = 0;
        var l2Distance = new L2DistanceFunction();
        for (var i = 0; i < vectors.length; i++) {
            for (int j = i + 1; j < vectors.length; j++) {
                distance += l2Distance.computeDistance(vectors[i], 0, vectors[j], 0,
                        vectors[i].length);
                count++;
            }
        }

        return distance / count;
    }

    private static int findClosestCentroid(byte[] centroids, float[][][] pqCentroids, float[] vector,
                                           int pqQuantizersCount, int pqSubVectorSize) {
        var minDistance = Float.MAX_VALUE;
        var minIndex = -1;

        var lookupTable = PQ.blankLookupTable(pqQuantizersCount);
        PQ.buildPQDistanceLookupTable(vector, lookupTable, pqCentroids, pqQuantizersCount, pqSubVectorSize,
                new L2DistanceFunction());

        for (int centroidIndex = 0, index = 0; centroidIndex < centroids.length; centroidIndex += pqQuantizersCount, index++) {
            var distance = PQ.computePQDistance(centroids, lookupTable, index, pqQuantizersCount);

            if (distance < minDistance) {
                minDistance = distance;
                minIndex = index;
            }
        }

        return minIndex;
    }

    record ArrayVectorReader(float[][] vectors) implements VectorReader {
        public int size() {
            return vectors.length;
        }

        public MemorySegment read(int index) {
            var vectorSegment = MemorySegment.ofArray(new byte[vectors[index].length * Float.BYTES]);

            MemorySegment.copy(MemorySegment.ofArray(vectors[index]), 0, vectorSegment, 0,
                    (long) vectors[index].length * Float.BYTES);

            return vectorSegment;
        }

        @Override
        public void close() {
        }
    }
}
