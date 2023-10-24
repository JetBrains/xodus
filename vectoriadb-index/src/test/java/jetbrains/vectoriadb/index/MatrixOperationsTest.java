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

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class MatrixOperationsTest {
    @Test
    public void testMatrixMultiplication() {
        var matrix = new float[]{42.0f, 24.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f,
                9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 14.0f, 15.0f};

        var vector = new float[]{13.0f, 1.0f, 2.0f, 3.0f};
        var result = new float[5];
        var expectedResult = new float[]{14.0f, 32.0f, 50.0f, 68.0f, 86.0f};

        MatrixOperations.multiply(matrix, 2, 3, 5, vector,
                1, result, new float[4]);
        Assert.assertArrayEquals(expectedResult, result, 0.0f);
    }

    @Test
    public void test3DMatrixIndexCalculation() {
        var seed = System.nanoTime();
        var matrix = new int[13][42][24];
        System.out.println("test3DMatrixIndexCalculation seed = " + seed);
        var rnd = new Random(seed);

        var flatMatrix = new int[13 * 42 * 24];
        var index = 0;
        for (var i = 0; i < 13; i++) {
            for (var j = 0; j < 42; j++) {
                for (var k = 0; k < 24; k++) {
                    matrix[i][j][k] = rnd.nextInt();
                    flatMatrix[index] = matrix[i][j][k];
                    index++;
                }
            }
        }

        for (var i = 0; i < 13; i++) {
            for (var j = 0; j < 42; j++) {
                for (var k = 0; k < 24; k++) {
                    Assert.assertEquals(matrix[i][j][k],
                            flatMatrix[MatrixOperations.threeDMatrixIndex(42, 24, i, j, k)]);
                }
            }
        }
    }

    @Test
    public void test2DMatrixIndexCalculation() {
        var seed = System.nanoTime();
        var matrix = new int[13][42];
        System.out.println("test2DMatrixIndexCalculation seed = " + seed);
        var rnd = new Random(seed);

        var flatMatrix = new int[13 * 42];
        var index = 0;
        for (var i = 0; i < 13; i++) {
            for (var j = 0; j < 42; j++) {
                matrix[i][j] = rnd.nextInt();
                flatMatrix[index] = matrix[i][j];
                index++;
            }
        }

        for (var i = 0; i < 13; i++) {
            for (var j = 0; j < 42; j++) {
                Assert.assertEquals(matrix[i][j],
                        flatMatrix[MatrixOperations.twoDMatrixIndex(42, i, j)]);
            }
        }
    }
}
