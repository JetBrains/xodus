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
import java.lang.foreign.ValueLayout;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class PQ {
    public static final int PQ_CODE_BASE_SIZE = 256;

    public static PQParameters calculatePQParameters(int vectorDim, int pqCompression) {
        var pqSubVectorSize = pqCompression / Float.BYTES;
        var pqQuantizersCount = vectorDim / pqSubVectorSize;

        if (pqCompression % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        if (vectorDim % pqSubVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        return new PQParameters(pqSubVectorSize, pqQuantizersCount);
    }

    public static PQCodes generatePQCodes(int pqQuantizersCount,
                                          int pqSubVectorSize, byte distanceFunction,
                                          VectorReader vectorReader, Arena arena) {

        var kMeans = new KMeansMiniBatchGD[pqQuantizersCount];

        for (int i = 0; i < pqQuantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchGD(PQ_CODE_BASE_SIZE,
                    50, i * pqSubVectorSize, pqSubVectorSize,
                    vectorReader);
        }

        var codeBaseSize = Math.min(PQ_CODE_BASE_SIZE, vectorReader.size());
        float[][][] pqCentroids = new float[pqQuantizersCount][codeBaseSize][pqSubVectorSize];

        var minBatchSize = 16;

        var cores = Math.min(Runtime.getRuntime().availableProcessors(), pqQuantizersCount);
        var batchSize = Math.max(minBatchSize, 2 * 1024 * 1024 / (Float.BYTES * pqSubVectorSize)) / cores;

        var executors = Executors.newFixedThreadPool(cores);
        var futures = new Future[pqQuantizersCount];

        for (int i = 0; i < pqQuantizersCount; i++) {
            var km = kMeans[i];
            futures[i] = executors.submit(() -> km.calculate(minBatchSize, batchSize, distanceFunction));
        }

        for (var future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException("Error during KMeans clustering of indexed data.", e);
            }
        }
        executors.shutdown();

        for (int i = 0; i < pqQuantizersCount; i++) {
            var centroids = kMeans[i].centroids;

            var index = 0;
            for (int j = 0; j < codeBaseSize; j++) {
                System.arraycopy(centroids, index, pqCentroids[i][j], 0, pqSubVectorSize);
                index += pqSubVectorSize;
            }
        }

        var size = vectorReader.size();
        MemorySegment pqVectors = arena.allocate((long) size * pqQuantizersCount);

        for (int n = 0; n < size; n++) {
            var vector = vectorReader.read(n);

            for (int i = 0; i < pqQuantizersCount; i++) {
                var centroidIndex = Distance.findClosestVector(kMeans[i].centroids, vector, i * pqSubVectorSize,
                        pqSubVectorSize, distanceFunction);
                pqVectors.set(ValueLayout.JAVA_BYTE, (long) n * pqQuantizersCount + i, (byte) centroidIndex);
            }
        }

        return new PQCodes(pqVectors, pqCentroids);
    }

    public static float[] blankLookupTable(int pqQuantizersCount) {
        return new float[pqQuantizersCount * PQ_CODE_BASE_SIZE];
    }


    public static void buildPQDistanceLookupTable(float[] vector, float[] lookupTable, float[][][] pqCentroids,
                                                  int pqQuantizersCount, int pqSubVectorSize,
                                                  byte distanceFunction) {
        for (int i = 0; i < pqQuantizersCount; i++) {
            var centroids = pqCentroids[i];

            for (int j = 0; j < centroids.length; j++) {
                var centroid = centroids[j];
                var distance = Distance.computeDistance(centroid, vector, i * pqSubVectorSize, centroid.length, distanceFunction
                );
                lookupTable[i * (1 << Byte.SIZE) + j] = distance;
            }
        }
    }

    public static float computePQDistance(MemorySegment pqVectors, float[] lookupTable, int vectorIndex,
                                          int pqQuantizersCount) {
        var distance = 0f;

        var pqIndex = pqQuantizersCount * vectorIndex;
        for (int i = pqIndex; i < pqIndex + pqQuantizersCount; i++) {
            var code = pqVectors.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            distance += lookupTable[(i - pqIndex) * (1 << Byte.SIZE) + code];
        }

        return distance;
    }

    public static float computePQDistance(byte[] pqVectors, float[] lookupTable, int vectorIndex,
                                          int pqQuantizersCount) {
        var distance = 0f;

        var pqIndex = pqQuantizersCount * vectorIndex;
        for (int i = pqIndex; i < pqIndex + pqQuantizersCount; i++) {
            var code = Byte.toUnsignedInt(pqVectors[i]);
            distance += lookupTable[(i - pqIndex) * (1 << Byte.SIZE) + code];
        }

        return distance;
    }


    public static final class PQCodes {
        public final MemorySegment pqVectors;
        public final float[][][] pqCentroids;

        public PQCodes(MemorySegment pqVectors, float[][][] lookupTable) {
            this.pqVectors = pqVectors;
            this.pqCentroids = lookupTable;
        }
    }

    public static final class PQParameters {
        public final int pqSubVectorSize;
        public final int pqQuantizersCount;

        public PQParameters(int pqSubVectorSize, int pqQuantizersCount) {
            this.pqSubVectorSize = pqSubVectorSize;
            this.pqQuantizersCount = pqQuantizersCount;
        }
    }
}
