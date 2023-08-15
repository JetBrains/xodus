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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class PQ {
    private static final Logger logger = LoggerFactory.getLogger(PQ.class);
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

        logger.info("Start generation of pq codes for {} quantizers.", pqQuantizersCount);

        for (int i = 0; i < pqQuantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchGD(i, PQ_CODE_BASE_SIZE,
                    50, i * pqSubVectorSize, pqSubVectorSize,
                    vectorReader);
        }

        var codeBaseSize = Math.min(PQ_CODE_BASE_SIZE, vectorReader.size());
        float[][][] pqCentroids = new float[pqQuantizersCount][codeBaseSize][pqSubVectorSize];

        var minBatchSize = 16;

        var cores = Math.min(Runtime.getRuntime().availableProcessors(), pqQuantizersCount);
        var batchSize = Math.max(minBatchSize, 2 * 1024 * 1024 / (Float.BYTES * pqSubVectorSize)) / cores;

        logger.info("{} cores will be used, batch size is {}, min batch size is {}.", cores, batchSize, minBatchSize);


        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-kmeans-thread-" + thread.threadId());
            return thread;
        })) {
            var futures = new Future[pqQuantizersCount];

            for (int i = 0; i < pqQuantizersCount; i++) {
                var km = kMeans[i];
                futures[i] = executors.submit(() -> {
                    try {
                        km.calculate(minBatchSize, batchSize, distanceFunction);
                    } catch (Exception e) {
                        logger.error("Error during KMeans clustering of indexed data.", e);
                        throw e;
                    }
                });
            }

            for (var future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException("Error during KMeans clustering of indexed data.", e);
                }
            }
        }


        var size = vectorReader.size();
        cores = Math.min(Runtime.getRuntime().availableProcessors(), size);
        logger.info("KMeans clustering finished. Creation of PQ codes started. {} cores will be used.", cores);

        for (int i = 0; i < pqQuantizersCount; i++) {
            var centroids = kMeans[i].centroids;

            var index = 0;
            for (int j = 0; j < codeBaseSize; j++) {
                System.arraycopy(centroids, index, pqCentroids[i][j], 0, pqSubVectorSize);
                index += pqSubVectorSize;
            }
        }

        var pqVectors = arena.allocate((long) size * pqQuantizersCount);

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-code-assignment-thread-" + thread.threadId());
            return thread;
        })) {
            var assignmentSize = (size + cores - 1) / cores;
            var futures = new Future[cores];

            for (var n = 0; n < cores; n++) {
                var start = n * assignmentSize;
                var end = Math.min(start + assignmentSize, size);

                var id = n;
                futures[n] = executors.submit(() -> {
                    var localSize = end - start;
                    for (var k = 0; k < localSize; k++) {
                        var vectorIndex = start + k;
                        var vector = vectorReader.read(vectorIndex);

                        for (int i = 0; i < pqQuantizersCount; i++) {
                            var centroidIndex = Distance.findClosestVector(kMeans[i].centroids, vector,
                                    i * pqSubVectorSize, pqSubVectorSize, distanceFunction);
                            pqVectors.set(ValueLayout.JAVA_BYTE,
                                    (long) vectorIndex * pqQuantizersCount + i, (byte) centroidIndex);
                        }

                        if ((k & (1024 * 1024 - 1)) == 0) {
                            logger.info("Thread # {} - {} vectors out of {} are processed ({}%). ", id, k,
                                    localSize, k * 100.0 / localSize);
                        }
                    }

                    logger.info("Thread # {} - All {} vectors are processed. ", id, localSize);
                });
            }

            for (var future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException("Error during assigning of PQ codes.", e);
                }
            }
        }

        logger.info("PQ codes created.");

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
                var distance = Distance.computeDistance(centroid, vector,
                        i * pqSubVectorSize, centroid.length, distanceFunction
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
