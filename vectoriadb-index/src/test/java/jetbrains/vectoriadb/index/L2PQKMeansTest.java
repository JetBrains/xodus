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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

public class L2PQKMeansTest extends AbstractVectorsTest {
    @Test
    public void distanceTablesTest() {
        var seed = System.nanoTime();
        System.out.println("distanceTablesTest seed = " + seed);
        var rnd = new Random(seed);

        var quantizersCount = 4;
        var centroids = generateDistanceTable(quantizersCount, rnd);
        var expectedDistanceTables = createDistanceTable(quantizersCount, centroids, null);

        var actualDistanceTables = L2PQQuantizer.buildDistanceTables(centroids, quantizersCount, centroids[0][0].length,
                L2DistanceFunction.INSTANCE);
        Assert.assertArrayEquals(expectedDistanceTables, actualDistanceTables, 0.0f);
    }


    @Test
    public void symmetricDistanceTest() {
        var seed = System.nanoTime();
        System.out.println("symmetricDistanceTest seed = " + seed);
        var rnd = new Random(seed);

        var quantizersCount = 4;
        var centroids = generateDistanceTable(quantizersCount, rnd);
        var distanceTables = new float[quantizersCount][Quantizer.CODE_BASE_SIZE][Quantizer.CODE_BASE_SIZE];
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
                    flatDirectPqVectors.set(ValueLayout.JAVA_BYTE, MatrixOperations.twoDMatrixIndex(quantizersCount, i, j), pqVectors[i][j]);
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

                    var actualDistance = AbstractQuantizer.symmetricDistance(flatDirectPqVectors, i, flatHeapVectors, j,
                            flatDistanceTables, quantizersCount, Quantizer.CODE_BASE_SIZE);
                    Assert.assertEquals(expectedDistance, actualDistance, 0.0f);
                }
            }
        }
    }

    @Test
    public void testPQKMeansQuality() throws Exception {
        var vectors = loadSift10KVectors();
        var clustersCount = 40;

        try (var pqQuantizer = new L2PQQuantizer()) {
            System.out.println("Generating PQ codes...");
            pqQuantizer.generatePQCodes(SIFT_VECTOR_DIMENSIONS, 32, new ArrayVectorReader(vectors), new NoOpProgressTracker());

            System.out.println("PQ codes generated. Calculating centroids...");
            var centroids = pqQuantizer.calculateCentroids(clustersCount, 50, L2DistanceFunction.INSTANCE, new NoOpProgressTracker());

            System.out.println("Centroids calculated. Clustering data vectors...");

            var silhouetteCoefficient = new SilhouetteCoefficient(centroids, vectors, L2DistanceFunction.INSTANCE);

            System.out.println("Data vectors clustered. Calculating silhouette coefficient...");

            var coefficientValue = silhouetteCoefficient.calculate();
            System.out.printf("silhouetteCoefficient = %f%n", coefficientValue);
            Assert.assertTrue("silhouetteCoefficient < 0.17:" + silhouetteCoefficient, coefficientValue >= 0.08);
        }
    }

    @Test
    //@Ignore
    public void clusterInitializersBenchmark() throws Exception {
        var vectors = loadSift1MVectors();
        var clustersCount = 40;

        var progressTracker = new NoOpProgressTracker();
        try (var pqQuantizer = new L2PQQuantizer()) {
            System.out.println("Generating PQ codes...");
            pqQuantizer.generatePQCodes(SIFT_VECTOR_DIMENSIONS, 32, new ArrayVectorReader(vectors), progressTracker);

            var count = 0;
            var meanPpRandomDiff = 0f;
            var meanMaxDistanceRandomDiff = 0f;
            var meanSilhCoeffRandom = 0f;
            var meanSilhCoeffPP = 0f;
            var meanSilhCoeffMaxDistance = 0f;
            for (var i = 0; i < 10; i += 1) {
                var silhCoeffRandom = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new RandomClusterInitializer(), clustersCount, vectors, progressTracker);
                var silhCoeffPP = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new KMeansPlusPlusClusterInitializer(), clustersCount, vectors, progressTracker);
                var silhCoeffMaxDistance = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new MaxDistanceClusterInitializer(), clustersCount, vectors, progressTracker);

                System.out.printf("silhouetteCoefficient random = %f%n", silhCoeffRandom);
                System.out.printf("silhouetteCoefficient k-means++ = %f%n", silhCoeffPP);
                System.out.printf("silhouetteCoefficient max distance = %f%n", silhCoeffMaxDistance);

                meanPpRandomDiff += silhCoeffPP - silhCoeffRandom;
                meanMaxDistanceRandomDiff += silhCoeffMaxDistance - silhCoeffRandom;
                meanSilhCoeffRandom += silhCoeffRandom;
                meanSilhCoeffPP += silhCoeffPP;
                meanSilhCoeffMaxDistance += silhCoeffMaxDistance;
                count++;
            }
            meanPpRandomDiff = meanPpRandomDiff / count;
            meanMaxDistanceRandomDiff = meanMaxDistanceRandomDiff / count;
            meanSilhCoeffRandom = meanSilhCoeffRandom / count;
            meanSilhCoeffPP = meanSilhCoeffPP / count;
            meanSilhCoeffMaxDistance = meanSilhCoeffMaxDistance / count;
            System.out.printf("k-means++ - random: %f, max distance - random: %f, meanSilhCoeffRandom: %f, meanSilhCoeffPP: %f, meanSilhCoeffMaxDistance: %f", meanPpRandomDiff, meanMaxDistanceRandomDiff, meanSilhCoeffRandom, meanSilhCoeffPP, meanSilhCoeffMaxDistance);
        }
    }

    private static Float doPQKMeansAndGetSilhouetteCoefficient(L2PQQuantizer pqQuantizer, ClusterInitializer clusterInitializer, int clustersCount, float[][] vectors, ProgressTracker progressTracker) {
        System.out.print("PQ codes generated. Calculating centroids, initialization...\n");
        pqQuantizer.setClusterInitializer(clusterInitializer);
        var centroids = pqQuantizer.calculateCentroids(clustersCount, 50, L2DistanceFunction.INSTANCE, progressTracker);

        System.out.println("Centroids calculated. Calculating silhouette coefficient...");
        return new SilhouetteCoefficientMedoid(centroids, vectors, L2DistanceFunction.INSTANCE).calculate();
    }

    private static float[][][] generateDistanceTable(int quantizersCount, Random rnd) {
        float[][][] centroids = new float[quantizersCount][Quantizer.CODE_BASE_SIZE][Quantizer.CODE_BASE_SIZE];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < Quantizer.CODE_BASE_SIZE; j++) {
                var vector = new float[Quantizer.CODE_BASE_SIZE];
                for (int k = 0; k < Quantizer.CODE_BASE_SIZE; k++) {
                    vector[k] = rnd.nextFloat();
                }

                centroids[i][j] = vector;
            }
        }

        return centroids;
    }

    @NotNull
    private static float[] createDistanceTable(int quantizersCount, float[][][] centroids, float[][][] distanceTable) {
        var expectedDistanceTables = new float[4 * Quantizer.CODE_BASE_SIZE * Quantizer.CODE_BASE_SIZE];
        var index = 0;

        var l2Distance = new L2DistanceFunction();
        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < Quantizer.CODE_BASE_SIZE; j++) {
                for (int k = 0; k < Quantizer.CODE_BASE_SIZE; k++) {
                    expectedDistanceTables[index] = l2Distance.computeDistance(centroids[i][j], 0,
                            centroids[i][k], 0, Quantizer.CODE_BASE_SIZE);
                    if (distanceTable != null) {
                        distanceTable[i][j][k] = expectedDistanceTables[index];
                    }

                    index++;
                }
            }
        }
        return expectedDistanceTables;
    }


    record ArrayVectorReader(float[][] vectors) implements VectorReader {
        public int size() {
            return vectors.length;
        }

        public MemorySegment read(int index) {
            var vectorSegment = MemorySegment.ofArray(new byte[vectors[index].length * Float.BYTES]);

            MemorySegment.copy(MemorySegment.ofArray(vectors[index]), 0,
                    vectorSegment, 0, (long) vectors[index].length * Float.BYTES);

            return vectorSegment;
        }

        @Override
        public void close() {
        }
    }
}
