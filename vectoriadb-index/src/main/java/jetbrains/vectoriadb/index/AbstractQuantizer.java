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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract class AbstractQuantizer implements Quantizer {
    public static float symmetricDistance(MemorySegment pqVectors, long storedEncodedVectorIndex, byte[] encodedVectors, int encodedVectorIndex,
                                          float[] distanceTables, int quantizersCount, int codeBaseSize) {
        float result = 0.0f;

        var firstPqBase = MatrixOperations.twoDMatrixIndex(quantizersCount, storedEncodedVectorIndex, 0);
        var secondPqBase = MatrixOperations.twoDMatrixIndex(quantizersCount, encodedVectorIndex, 0);

        for (int i = 0; i < quantizersCount; i++) {
            var firstPqCode = Byte.toUnsignedInt(pqVectors.get(ValueLayout.JAVA_BYTE, firstPqBase + i));
            var secondPwCode = Byte.toUnsignedInt(encodedVectors[secondPqBase + i]);

            var distanceIndex = MatrixOperations.threeDMatrixIndex(codeBaseSize, codeBaseSize,
                    i, firstPqCode, secondPwCode);
            result += distanceTables[distanceIndex];
        }

        return result;
    }
}
