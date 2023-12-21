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

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;

import static jetbrains.vectoriadb.index.CodebookInitializer.getCodebookCount;
import static jetbrains.vectoriadb.index.SilhouetteCoefficientKt.l2SilhouetteCoefficient;

public class L2PQKMeansTest {
    @Test
    public void distanceTablesTest() {
        var seed = System.nanoTime();
        System.out.println("distanceTablesTest seed = " + seed);
        var rnd = new Random(seed);

        var codebookCount = 4;
        var codeBaseSize = CodebookInitializer.CODE_BASE_SIZE;
        var dimensions = CodebookInitializer.CODE_BASE_SIZE;
        var codebooks = generateCodebooks(codebookCount, codeBaseSize, dimensions, rnd);
        var codebookDimensions = new int[codebookCount];
        Arrays.fill(codebookDimensions, dimensions);

        var expectedDistanceTables = buildDistanceTable(codebooks, null);

        var actualDistanceTables = L2PQQuantizer.buildDistanceTables(codebooks, codebookDimensions, L2DistanceFunction.INSTANCE);
        Assert.assertArrayEquals(expectedDistanceTables, actualDistanceTables, 1e-5f);
    }


    @Test
    public void symmetricDistanceTest() {
        var seed = System.nanoTime();
        System.out.println("symmetricDistanceTest seed = " + seed);
        var rnd = new Random(seed);

        var codebookCount = 4;
        var codeBaseSize = CodebookInitializer.CODE_BASE_SIZE;
        var dimensions = CodebookInitializer.CODE_BASE_SIZE;
        var codebooks = generateCodebooks(codebookCount, codeBaseSize, dimensions, rnd);

        var distanceTables = new float[codebookCount][codeBaseSize][codeBaseSize];

        var numVectors = 100;
        var pqVectors = new byte[numVectors][codebookCount];
        var flatDistanceTables = buildDistanceTable(codebooks, distanceTables);

        for (int vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
            for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
                pqVectors[vectorIdx][codebookIdx] = (byte) rnd.nextInt(256);
            }
        }

        try (var arena = Arena.ofShared()) {
            var flatDirectPqVectors = arena.allocateArray(ValueLayout.JAVA_BYTE, numVectors * codebookCount);
            var flatHeapVectors = new byte[numVectors * codebookCount];

            for (int vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
                for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
                    flatDirectPqVectors.set(ValueLayout.JAVA_BYTE, MatrixOperations.twoDMatrixIndex(codebookCount, vectorIdx, codebookIdx), pqVectors[vectorIdx][codebookIdx]);
                    flatHeapVectors[MatrixOperations.twoDMatrixIndex(codebookCount, vectorIdx, codebookIdx)] = pqVectors[vectorIdx][codebookIdx];
                }
            }

            for (int vectorIdx1 = 0; vectorIdx1 < numVectors; vectorIdx1++) {
                for (int vectorIdx2 = 0; vectorIdx2 < 100; vectorIdx2++) {
                    var expectedDistance = 0.0f;

                    for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
                        var firstCode = Byte.toUnsignedInt(pqVectors[vectorIdx1][codebookIdx]);
                        var secondCode = Byte.toUnsignedInt(pqVectors[vectorIdx2][codebookIdx]);

                        expectedDistance += distanceTables[codebookIdx][firstCode][secondCode];
                    }

                    var actualDistance = AbstractQuantizer.symmetricDistance(flatDirectPqVectors, vectorIdx1, flatHeapVectors, vectorIdx2, flatDistanceTables, codebookCount, dimensions);
                    Assert.assertEquals(expectedDistance, actualDistance, 0.0f);
                }
            }
        }
    }



    @Test
    public void PQKMeansQuality() {
        var dataset = VectorDataset.Sift10K.INSTANCE.build();
        var vectors = dataset.getVectors();
        var clustersCount = 40;

        try (var pqQuantizer = new L2PQQuantizer()) {
            System.out.println("Generating PQ codes...");
            var vectorReader = new FloatArrayToByteArrayVectorReader(vectors);
            var codebookCount = getCodebookCount(dataset.getDimensions(), 32);
            pqQuantizer.generatePQCodes(vectorReader, codebookCount, new NoOpProgressTracker());

            System.out.println("PQ codes generated. Calculating centroids...");
            var centroids = pqQuantizer.calculateCentroids(vectorReader, clustersCount, 50, L2DistanceFunction.INSTANCE, new NoOpProgressTracker());

            System.out.println("Centroids calculated. Clustering data vectors...");
            System.out.println("Data vectors clustered. Calculating silhouette coefficient...");

            var silhouetteCoeff = l2SilhouetteCoefficient(L2DistanceFunction.INSTANCE, centroids, vectors).calculate();

            System.out.printf("silhouetteCoefficient = %f%n", silhouetteCoeff);
            Assert.assertTrue("silhouetteCoefficient < 0.08:" + silhouetteCoeff, silhouetteCoeff >= 0.08);
        }
    }

    private static float[][][] generateCodebooks(int codebookCount, int codeBaseSize, int codebookDimensions, Random rnd) {
        float[][][] centroids = new float[codebookCount][codeBaseSize][codebookDimensions];
        for (int codebookIdx = 0; codebookIdx < 4; codebookIdx++) {
            for (int code = 0; code < codeBaseSize; code++) {
                var vector = new float[codebookDimensions];
                for (int dimensionIdx = 0; dimensionIdx < codebookDimensions; dimensionIdx++) {
                    vector[dimensionIdx] = rnd.nextFloat();
                }

                centroids[codebookIdx][code] = vector;
            }
        }

        return centroids;
    }

    @NotNull
    private static float[] buildDistanceTable(float[][][] codebooks, float[][][] distanceTable) {
        var codebookCount = codebooks.length;
        var codeBaseSize = codebooks[0].length;
        var dimensions = codebooks[0][0].length;
        var distanceTables = new float[codebookCount * codeBaseSize * codeBaseSize];
        var index = 0;

        var l2Distance = new L2DistanceFunction();
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            for (int code1 = 0; code1 < codeBaseSize; code1++) {
                for (int code2 = 0; code2 < codeBaseSize; code2++) {
                    distanceTables[index] = l2Distance.computeDistance(codebooks[codebookIdx][code1], 0, codebooks[codebookIdx][code2], 0, dimensions);
                    if (distanceTable != null) {
                        distanceTable[codebookIdx][code1][code2] = distanceTables[index];
                    }

                    index++;
                }
            }
        }
        return distanceTables;
    }
}