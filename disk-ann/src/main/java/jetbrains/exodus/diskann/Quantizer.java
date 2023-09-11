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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public interface Quantizer {
    int CODE_BASE_SIZE = 256;

    default Parameters calculatePQParameters(int vectorDim, int compression) {
        var pqSubVectorSize = compression / Float.BYTES;
        var quantizersCount = vectorDim / pqSubVectorSize;

        if (compression % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        if (vectorDim % pqSubVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        return new Parameters(pqSubVectorSize, quantizersCount);
    }


    Codes generatePQCodes(int quantizersCount, int subVectorSize, VectorReader vectorReader, Arena arena);

    void buildDistanceLookupTable(float[] vector, float[] lookupTable, float[][][] centroids,
                                  int quantizersCount, int subVectorSize, DistanceFunction distanceFunction);

    float computeDistance(MemorySegment vectors, float[] lookupTable, int vectorIndex,
                          int quantizersCount);

    final class Codes {
        public final MemorySegment pqVectors;
        public final float[][][] pqCodesVectors;

        public Codes(MemorySegment pqVectors, float[][][] lookupTable) {
            this.pqVectors = pqVectors;
            this.pqCodesVectors = lookupTable;
        }
    }

    final class Parameters {
        public final int pqSubVectorSize;
        public final int pqQuantizersCount;

        public Parameters(int pqSubVectorSize, int pqQuantizersCount) {
            this.pqSubVectorSize = pqSubVectorSize;
            this.pqQuantizersCount = pqQuantizersCount;
        }
    }
}
