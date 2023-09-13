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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class L2PQQuantizer implements Quantizer {
    public static final L2PQQuantizer INSTANCE = new L2PQQuantizer();

    private static final Logger logger = LoggerFactory.getLogger(L2PQQuantizer.class);

    @Override
    public Codes generatePQCodes(int quantizersCount, int subVectorSize, VectorReader vectorReader, Arena arena) {
        var kMeans = new KMeansMiniBatchGD[quantizersCount];

        logger.info("Start generation of pq codes for {} quantizers.", quantizersCount);

        for (int i = 0; i < quantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchGD(i, CODE_BASE_SIZE,
                    50, i * subVectorSize, subVectorSize,
                    vectorReader);
        }

        var codeBaseSize = Math.min(CODE_BASE_SIZE, vectorReader.size());
        float[][][] pqCentroids = new float[quantizersCount][codeBaseSize][subVectorSize];

        var minBatchSize = 16;

        var cores = Math.min(Runtime.getRuntime().availableProcessors(), quantizersCount);
        var batchSize = Math.max(minBatchSize, 2 * 1024 * 1024 / (Float.BYTES * subVectorSize)) / cores;

        logger.info("{} cores will be used, batch size is {}, min batch size is {}.", cores, batchSize, minBatchSize);


        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-kmeans-thread-" + thread.threadId());
            return thread;
        })) {
            var futures = new Future[quantizersCount];

            for (int i = 0; i < quantizersCount; i++) {
                var km = kMeans[i];
                futures[i] = executors.submit(() -> {
                    try {
                        km.calculate(minBatchSize, batchSize);
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

        for (int i = 0; i < quantizersCount; i++) {
            var centroids = kMeans[i].centroids;

            var index = 0;
            for (int j = 0; j < codeBaseSize; j++) {
                System.arraycopy(centroids, index, pqCentroids[i][j], 0, subVectorSize);
                index += subVectorSize;
            }
        }

        var pqVectors = arena.allocate((long) size * quantizersCount);

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

                        for (int i = 0; i < quantizersCount; i++) {
                            var centroidIndex = L2DistanceFunction.INSTANCE.findClosestVector(kMeans[i].centroids,
                                    vector, i * subVectorSize, subVectorSize);
                            pqVectors.set(ValueLayout.JAVA_BYTE,
                                    (long) vectorIndex * quantizersCount + i, (byte) centroidIndex);
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

        return new Codes(pqVectors, pqCentroids);

    }

    @Override
    public void buildDistanceLookupTable(float[] vector, float[] lookupTable, float[][][] centroids,
                                         int quantizersCount, int subVectorSize, DistanceFunction distanceFunction) {
        for (int i = 0; i < quantizersCount; i++) {
            var quantizerCentroids = centroids[i];

            for (int j = 0; j < quantizerCentroids.length; j++) {
                var centroid = quantizerCentroids[j];
                var distance = distanceFunction.computeDistance(centroid, 0, vector,
                        i * subVectorSize, centroid.length);
                lookupTable[i * (1 << Byte.SIZE) + j] = distance;
            }
        }
    }

    @Override
    public float computeDistance(MemorySegment vectors, float[] lookupTable, int vectorIndex, int quantizersCount) {
        var distance = 0f;

        var pqIndex = quantizersCount * vectorIndex;
        for (int i = pqIndex; i < pqIndex + quantizersCount; i++) {
            var code = vectors.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            distance += lookupTable[(i - pqIndex) * (1 << Byte.SIZE) + code];
        }

        return distance;
    }
}
