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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public final class PQKMeans {
    public static byte[] calculatePartitions(float[][][] centroids,
                                             MemorySegment pqVectors,
                                             int numClusters,
                                             int iterations,
                                             byte distanceFunction) {
        var quantizersCount = centroids.length;
        var codeBaseSize = centroids[0].length;

        var numVectors = (int) (pqVectors.byteSize() / quantizersCount);
        var centroidIndexes = new int[numVectors];

        var distanceTables = distanceTables(centroids, distanceFunction);

        var pqCentroids = new byte[numClusters * quantizersCount];


        var rnd = ThreadLocalRandom.current();

        for (int i = 0; i < numClusters; i++) {
            var vecIndex = rnd.nextInt(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE,
                    (long) vecIndex * quantizersCount, pqCentroids,
                    i * quantizersCount, quantizersCount);
        }


        var histogram = new float[numClusters * quantizersCount * codeBaseSize];
        var v = new float[codeBaseSize];
        var mulBuffer = new float[4];

        var histogramStep = MatrixOperations.threeDMatrixIndex(quantizersCount,
                codeBaseSize, 0, 0, 1);

        for (int n = 0; n < iterations; n++) {
            boolean assignedDifferently = false;
            for (int i = 0; i < numVectors; i++) {
                var prevIndex = centroidIndexes[i];
                var centroidIndex = findClosestCentroid(pqVectors, i, pqCentroids, distanceTables,
                        quantizersCount, codeBaseSize);
                centroidIndexes[i] = centroidIndex;
                assignedDifferently = assignedDifferently || prevIndex != centroidIndex;
            }

            if (!assignedDifferently) {
                break;
            }

            generateHistogram(pqVectors, centroidIndexes, quantizersCount, codeBaseSize, histogram);

            assignedDifferently = false;
            for (int k = 0, histogramOffset = 0, centroidIndex = 0; k < numClusters; k++,
                    histogramOffset += histogramStep) {
                for (int q = 0; q < quantizersCount; q++) {
                    var clusterDistanceTableOffset = MatrixOperations.threeDMatrixIndex(codeBaseSize,
                            codeBaseSize, q, 0, 0);
                    MatrixOperations.multiply(distanceTables, clusterDistanceTableOffset,
                            codeBaseSize, codeBaseSize, histogram, histogramOffset,
                            v, mulBuffer);
                    var minIndex = MatrixOperations.minIndex(v, 0, quantizersCount);
                    assert minIndex < codeBaseSize;

                    var prevIndex = pqCentroids[centroidIndex];
                    pqCentroids[centroidIndex] = (byte) minIndex;
                    assignedDifferently = assignedDifferently || prevIndex != minIndex;
                }
            }

            if (!assignedDifferently) {
                break;
            }
        }

        return pqCentroids;
    }


    static void generateHistogram(final MemorySegment pqVectors,
                                  final int[] clusters,
                                  final int quantizersCount,
                                  final int codeBaseSize,
                                  final float[] histogram) {
        Arrays.fill(histogram, 0.0f);
        var numCodes = pqVectors.byteSize();

        for (var codeIndex = 0; codeIndex < numCodes; ) {
            var clusterIndex = clusters[codeIndex / quantizersCount];

            for (int i = 0; i < quantizersCount; i++) {
                var code = Byte.toUnsignedInt(pqVectors.get(ValueLayout.JAVA_BYTE, codeIndex));
                var histogramIndex = MatrixOperations.threeDMatrixIndex(quantizersCount,
                        codeBaseSize, clusterIndex, i, code);
                histogram[histogramIndex]++;
                codeIndex++;
            }
        }
    }

    static float[] distanceTables(float[][][] centroids, byte distanceFunction) {
        var quantizers = centroids.length;
        var codeSpaceSize = centroids[0].length;
        var vecSize = centroids[0][0].length;

        var result = new float[quantizers * codeSpaceSize * codeSpaceSize];


        var batchResult = new float[4];
        for (int n = 0; n < quantizers; n++) {
            for (int i = 0; i < codeSpaceSize; i++) {
                var batchBoundary = i & -4;

                var baseOffset = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, n, i, 0);
                var j = 0;
                for (; j < batchBoundary; j += 4) {
                    var origin = centroids[n][i];

                    var vector1 = centroids[n][j];
                    var vector2 = centroids[n][j + 1];
                    var vector3 = centroids[n][j + 2];
                    var vector4 = centroids[n][j + 3];

                    Distance.computeDistance(origin, 0, vector1, 0, vector2, 0, vector3, 0, vector4, 0, batchResult, vecSize, distanceFunction
                    );

                    var offset = baseOffset + j;
                    result[offset] = batchResult[0];
                    result[offset + 1] = batchResult[1];
                    result[offset + 2] = batchResult[2];
                    result[offset + 3] = batchResult[3];
                }


                for (; j <= i; j++) {
                    var origin = centroids[n][i];
                    var vector = centroids[n][j];

                    result[baseOffset + j] = Distance.computeDistance(origin, vector, 0, vecSize, distanceFunction
                    );
                }
            }

            for (int i = 0; i < codeSpaceSize; i++) {
                for (int j = i + 1; j < codeSpaceSize; j++) {
                    var firstIndex = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, n, i, j);
                    var secondIndex = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, n, j, i);

                    result[firstIndex] = result[secondIndex];
                }
            }
        }

        return result;
    }

    static float symmetricDistance(MemorySegment dmPqVectors, int dmVectorIndex, byte[] heapPqCentroids,
                                   int heapVectorIndex, float[] distanceTables, int quantizersCount, int codeBaseSize) {
        float result = 0.0f;

        var firstPqBase = MatrixOperations.twoDMatrixIndex(quantizersCount, dmVectorIndex, 0);
        var secondPqBase = MatrixOperations.twoDMatrixIndex(quantizersCount, heapVectorIndex, 0);

        for (int i = 0; i < quantizersCount; i++) {
            var firstPqCode = Byte.toUnsignedInt(dmPqVectors.get(ValueLayout.JAVA_BYTE, firstPqBase + i));
            var secondPwCode = Byte.toUnsignedInt(heapPqCentroids[secondPqBase + i]);

            var distanceIndex = MatrixOperations.threeDMatrixIndex(codeBaseSize, codeBaseSize,
                    i, firstPqCode, secondPwCode);
            result += distanceTables[distanceIndex];
        }

        return result;
    }

    static int findClosestCentroid(final MemorySegment pqVectors, final int vectorIndex, final byte[] centroids,
                                   final float[] distanceTable, final int quantizersCount, int codeBaseSize) {
        int minIndex = 0;
        float minDistance = Float.MAX_VALUE;

        for (int centroidsIndex = 0, index = 0; centroidsIndex < centroids.length;
             centroidsIndex += quantizersCount, index++) {
            var distance = symmetricDistance(pqVectors, vectorIndex, centroids, index, distanceTable,
                    quantizersCount, codeBaseSize);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = index;
            }
        }

        return minIndex;
    }
}
