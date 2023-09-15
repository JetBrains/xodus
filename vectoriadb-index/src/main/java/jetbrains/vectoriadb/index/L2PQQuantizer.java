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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class L2PQQuantizer implements Quantizer {
    private static final Logger logger = LoggerFactory.getLogger(L2PQQuantizer.class);

    private int quantizersCount;
    private int subVectorSize;

    private final Arena arena;

    private MemorySegment pqVectors;

    //1st dimension quantizer index
    //2nd index of code inside code book
    //3d dimension centroid vector

    private float[][][] pqCentroids;

    L2PQQuantizer() {
        this.arena = Arena.openShared();
    }

    @Override
    public int quantizersCount() {
        return quantizersCount;
    }

    @Override
    public MemorySegment encodedVectors() {
        return pqVectors;
    }

    @Override
    public float[][][] centroids() {
        return pqCentroids;
    }

    @Override
    public void generatePQCodes(int vectorsDimension, int compressionRatio, VectorReader vectorReader) {
        if (compressionRatio % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }
        subVectorSize = compressionRatio / Float.BYTES;

        if (vectorsDimension % subVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        quantizersCount = vectorsDimension / subVectorSize;
        logger.info("PQ quantizers count is " + quantizersCount + ", sub vector size is " + subVectorSize +
                " elements , compression ratio is " + compressionRatio);

        var kMeans = new KMeansMiniBatchGD[quantizersCount];

        logger.info("Start generation of pq codes for {} quantizers.", quantizersCount);

        for (int i = 0; i < quantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchGD(i, CODE_BASE_SIZE,
                    50, i * subVectorSize, subVectorSize,
                    vectorReader);
        }

        var codeBaseSize = Math.min(CODE_BASE_SIZE, vectorReader.size());
        pqCentroids = new float[quantizersCount][codeBaseSize][subVectorSize];

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

        pqVectors = arena.allocate((long) size * quantizersCount);

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
    }

    @Override
    public float[] decodeVector(byte[] vectors, int index) {
        var result = new float[quantizersCount * subVectorSize];
        var offset = index * quantizersCount;

        for (int i = 0, pqCentroidVectorOffset = 0; i < quantizersCount; i++,
                pqCentroidVectorOffset += subVectorSize) {
            var pqCentroidVector = Byte.toUnsignedInt(vectors[i + offset]);
            System.arraycopy(pqCentroids[i][pqCentroidVector], 0, result,
                    pqCentroidVectorOffset, subVectorSize);
        }

        return result;
    }

    //    @Override
//    public MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
//        return arena.allocate((long) vectorsCount * quantizersCount);
//    }


    public void buildDistanceLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction) {
        for (int i = 0; i < quantizersCount; i++) {
            var quantizerCentroids = pqCentroids[i];

            for (int j = 0; j < quantizerCentroids.length; j++) {
                var centroid = quantizerCentroids[j];
                var distance = distanceFunction.computeDistance(centroid, 0, vector,
                        i * subVectorSize, centroid.length);
                lookupTable[i * (1 << Byte.SIZE) + j] = distance;
            }
        }
    }

    @Override
    public float computeDistance(float[] lookupTable, int vectorIndex) {
        var distance = 0f;

        var pqIndex = quantizersCount * vectorIndex;
        for (int i = pqIndex; i < pqIndex + quantizersCount; i++) {
            var code = pqVectors.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            distance += lookupTable[(i - pqIndex) * (1 << Byte.SIZE) + code];
        }

        return distance;
    }

    @Override
    public void computeDistance4Batch(float[] lookupTable, int vectorIndex1, int vectorIndex2,
                                      int vectorIndex3, int vectorIndex4, float[] result) {
        assert result.length == 4;

        var pqIndex1 = quantizersCount * vectorIndex1;
        var pqIndex2 = quantizersCount * vectorIndex2;
        var pqIndex3 = quantizersCount * vectorIndex3;
        var pqIndex4 = quantizersCount * vectorIndex4;

        var result1 = 0.0f;
        var result2 = 0.0f;
        var result3 = 0.0f;
        var result4 = 0.0f;

        for (int i = 0; i < quantizersCount; i++) {
            var rowOffset = i * (1 << Byte.SIZE);

            var code1 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex1 + i) & 0xFF;
            result1 += lookupTable[rowOffset + code1];

            var code2 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex2 + i) & 0xFF;
            result2 += lookupTable[rowOffset + code2];

            var code3 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex3 + i) & 0xFF;
            result3 += lookupTable[rowOffset + code3];

            var code4 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex4 + i) & 0xFF;
            result4 += lookupTable[rowOffset + code4];
        }

        result[0] = result1;
        result[1] = result2;
        result[2] = result3;
        result[3] = result4;
    }

    @Override
    public void store(DataOutputStream dataOutputStream) throws IOException {
        var codeBaseSize = pqCentroids[0].length;

        dataOutputStream.writeInt(quantizersCount);
        dataOutputStream.writeInt(codeBaseSize);
        dataOutputStream.writeInt(subVectorSize);

        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < codeBaseSize; j++) {
                for (int k = 0; k < subVectorSize; k++) {
                    dataOutputStream.writeFloat(pqCentroids[i][j][k]);
                }
            }
        }

        var pqVectorsSize = pqVectors.byteSize();
        dataOutputStream.writeLong(pqVectorsSize);

        for (long i = 0; i < pqVectorsSize; i++) {
            dataOutputStream.writeByte(pqVectors.get(ValueLayout.JAVA_BYTE, i));
        }
    }

    @Override
    public void load(DataInputStream dataInputStream) throws IOException {
        quantizersCount = dataInputStream.readInt();
        int pqCodeBaseSize = dataInputStream.readInt();
        subVectorSize = dataInputStream.readInt();

        pqCentroids = new float[quantizersCount][pqCodeBaseSize][subVectorSize];
        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < pqCodeBaseSize; j++) {
                for (int k = 0; k < subVectorSize; k++) {
                    pqCentroids[i][j][k] = dataInputStream.readFloat();
                }
            }
        }

        var pqVectorsSize = dataInputStream.readLong();
        pqVectors = arena.allocate(pqVectorsSize);
        for (int i = 0; i < pqVectorsSize; i++) {
            pqVectors.set(ValueLayout.JAVA_BYTE, i, dataInputStream.readByte());
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
