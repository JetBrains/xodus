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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Random;

public class PQKMeansTest extends AbstractVectorsTest {
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

                    var actualDistance = PQKMeans.symmetricDistance(flatDirectPqVectors, i, flatHeapVectors, j,
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

        var pqParameters = L2PQQuantizer.INSTANCE.calculatePQParameters(SIFT_VECTOR_DIMENSIONS, 32);
        var pqQuantizersCount = pqParameters.pqQuantizersCount;
        var pqSubVectorSize = pqParameters.pqSubVectorSize;

        try (var arena = Arena.openShared()) {
            System.out.println("Generating PQ codes...");
            var pqResult = L2PQQuantizer.INSTANCE.generatePQCodes(pqQuantizersCount, pqSubVectorSize,
                    new ArrayVectorReader(vectors), arena);

            System.out.println("PQ codes generated. Calculating centroids...");
            var pqCentroids = PQKMeans.calculatePartitions(pqResult.pqCodesVectors, pqResult.pqVectors, clustersCount,
                    1_000, L2DistanceFunction.INSTANCE);

            System.out.println("Centroids calculated. Clustering data vectors...");
            var centroids = convertPqVectorsIntoFloatVectors(pqCentroids, pqResult.pqCodesVectors);

            var vectorsByClusters = new ArrayList<IntArrayList>();
            for (int i = 0; i < clustersCount; i++) {
                vectorsByClusters.add(new IntArrayList());
            }

            var secondClosestClusterIndexes = new int[vectors.length];
            for (int i = 0; i < vectors.length; i++) {
                float[] vector = vectors[i];
                var clusters = findClosestAndSecondClosestCluster(centroids, vector,
                        L2DistanceFunction.INSTANCE);
                vectorsByClusters.get(clusters[0]).add(i);
                secondClosestClusterIndexes[i] = clusters[1];
            }

            System.out.println("Data vectors clustered. Calculating silhouette coefficient...");
            var silhouetteCoefficient = silhouetteCoefficient(vectorsByClusters, secondClosestClusterIndexes,
                    vectors, L2DistanceFunction.INSTANCE);
            System.out.printf("silhouetteCoefficient = %f%n", silhouetteCoefficient);
            Assert.assertTrue(silhouetteCoefficient >= 0.18);
        }
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

    private static float[][] convertPqVectorsIntoFloatVectors(byte[] pqVectors, float[][][] pqCodes) {
        var quantizersCount = pqCodes.length;
        var vectorsCount = pqVectors.length / quantizersCount;

        var subVectorSize = pqCodes[0][0].length;
        var vectorDimension = quantizersCount * subVectorSize;

        var result = new float[vectorsCount][vectorDimension];
        for (int i = 0, pqIndex = 0; i < vectorsCount; i++) {
            var vector = result[i];

            for (int j = 0; j < vectorDimension / subVectorSize; j++) {
                var code = Byte.toUnsignedInt(pqVectors[pqIndex]);
                var subVector = pqCodes[j][code];

                System.arraycopy(subVector, 0, vector, j * subVectorSize, subVectorSize);
                pqIndex++;
            }
        }

        return result;
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
