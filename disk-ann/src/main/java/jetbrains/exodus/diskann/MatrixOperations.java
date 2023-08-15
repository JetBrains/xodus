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

import java.util.Arrays;

public final class MatrixOperations {
    public static void multiply(float[] matrix, int matrixOffset,
                                int matrixColumns, int matrixRows,
                                float[] vector, int vectorOffset,
                                float[] result, float[] mulBuffer) {
        Arrays.fill(result, 0.0f);

        var index = 0;
        var boundary = matrixRows & -4;

        var matrixStep = matrixColumns << 2;

        var matrixOffset1 = matrixOffset;
        var matrixOffset2 = matrixOffset1 + matrixColumns;
        var matrixOffset3 = matrixOffset2 + matrixColumns;
        var matrixOffset4 = matrixOffset3 + matrixColumns;

        for (index = 0; index < boundary; index += 4, matrixOffset1 += matrixStep,
                matrixOffset2 += matrixStep, matrixOffset3 += matrixStep, matrixOffset4 += matrixStep) {
            DotDistance.computeDotDistance(vector, vectorOffset, matrix, matrixOffset1, matrix,
                    matrixOffset2, matrix, matrixOffset3, matrix, matrixOffset4, mulBuffer, matrixColumns);

            result[index] += mulBuffer[0];
            result[index + 1] += mulBuffer[1];
            result[index + 2] += mulBuffer[2];
            result[index + 3] += mulBuffer[3];
        }

        for (; index < matrixRows; index++, matrixOffset1 += matrixRows) {
            result[index] +=
                    DotDistance.computeDotDistance(vector, vectorOffset, matrix, matrixOffset1, matrixColumns);
        }
    }

    public static int threeDMatrixIndex(final int secondDimension, final int thirdDimension,
                                 final int firstIndex, final int secondIndex, final int thirdIndex) {
        return secondDimension * thirdDimension * firstIndex + thirdDimension * secondIndex + thirdIndex;
    }

    public static int twoDMatrixIndex(final int secondDimension, final int firstIndex, final int secondIndex) {
        return secondDimension * firstIndex + secondIndex;
    }

    public static long twoDMatrixIndex(final long secondDimension, final long firstIndex, final long secondIndex) {
        return secondDimension * firstIndex + secondIndex;
    }

    public static int minIndex(float[] vector, int from, int to) {
        var min = vector[0];
        var minIndex = 0;

        for (var i = from + 1; i < to; i++) {
            if (vector[i] < min) {
                min = vector[i];
                minIndex = i;
            }
        }
        return minIndex;
    }
}
