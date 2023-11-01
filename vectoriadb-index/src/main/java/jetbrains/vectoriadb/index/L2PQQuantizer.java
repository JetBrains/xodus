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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class L2PQQuantizer extends AbstractQuantizer {
    private static final VarHandle ATOMIC_HISTOGRAM_VARHANDLE = MethodHandles.arrayElementVarHandle(int[].class);
    private int quantizersCount;
    private int subVectorSize;
    private int codeBaseSize;
    private int vectorsCount;
    private final Arena arena;
    private MemorySegment pqVectors;

    //1st dimension quantizer index
    //2nd index of code inside code book
    //3d dimension centroid vector
    private float[][][] centroids;

    L2PQQuantizer() {
        this.arena = Arena.openShared();
    }


    @Override
    public void generatePQCodes(int vectorsDimension, int compressionRatio, VectorReader vectorReader,
                                @NotNull ProgressTracker progressTracker) {
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
        codeBaseSize = Math.min(CODE_BASE_SIZE, vectorReader.size());
        centroids = new float[quantizersCount][codeBaseSize][subVectorSize];

        var minBatchSize = 16;

        var cores = Math.min(Runtime.getRuntime().availableProcessors(), quantizersCount);
        var batchSize = Math.max(minBatchSize, 2 * 1024 * 1024 / (Float.BYTES * subVectorSize)) / cores;

        progressTracker.pushPhase("PQ codes creation",
                "quantizers count", String.valueOf(quantizersCount),
                "sub vector size", String.valueOf(subVectorSize),
                "code base size", String.valueOf(codeBaseSize));

        progressTracker.pushPhase("KMeans clustering", "batch size", String.valueOf(batchSize),
                "min batch size", String.valueOf(minBatchSize),
                "cores", String.valueOf(cores));

        var kMeans = new KMeansMiniBatchGD[quantizersCount];
        for (int i = 0; i < quantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchGD(CODE_BASE_SIZE,
                    50, i * subVectorSize, subVectorSize,
                    vectorReader);
        }

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-kmeans-thread-" + thread.threadId());
            return thread;
        })) {
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(quantizersCount, progressTracker);

            var futures = new Future[quantizersCount];
            for (int i = 0; i < quantizersCount; i++) {
                var km = kMeans[i];
                var id = i;
                futures[i] = executors.submit(() -> {
                    try (var localProgressTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                        km.calculate(minBatchSize, batchSize, localProgressTracker);
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
        progressTracker.pullPhase();

        progressTracker.progress(50);

        vectorsCount = vectorReader.size();
        cores = Math.min(Runtime.getRuntime().availableProcessors(), vectorsCount);

        progressTracker.pushPhase("PQ codes creation", "cores", String.valueOf(cores));
        for (int i = 0; i < quantizersCount; i++) {
            var centroids = kMeans[i].centroids;

            var index = 0;
            for (int j = 0; j < codeBaseSize; j++) {
                System.arraycopy(centroids, index, this.centroids[i][j], 0, subVectorSize);
                index += subVectorSize;
            }
        }

        pqVectors = allocateMemoryForPqVectors(quantizersCount, vectorsCount, arena);

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-code-assignment-thread-" + thread.threadId());
            return thread;
        })) {
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
            var assignmentSize = (vectorsCount + cores - 1) / cores;
            var futures = new Future[cores];

            for (var n = 0; n < cores; n++) {
                var start = n * assignmentSize;
                var end = Math.min(start + assignmentSize, vectorsCount);

                var id = n;
                futures[n] = executors.submit(() -> {
                    try (var localProgressTracker = mtProgressTracker.createThreadLocalTracker(id)) {
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

                            localProgressTracker.progress(k * 100.0 / localSize);
                        }
                    }
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
        progressTracker.pullPhase();

        progressTracker.progress(100);
        progressTracker.pullPhase();
    }

    @Override
    public float[] decodeVector(byte[] vectors, int vectorIndex) {
        var result = new float[quantizersCount * subVectorSize];

        var offset = vectorIndex * quantizersCount;
        for (int i = 0, pqCentroidVectorOffset = 0; i < quantizersCount; i++,
                pqCentroidVectorOffset += subVectorSize) {
            var pqCentroidVector = Byte.toUnsignedInt(vectors[i + offset]);
            System.arraycopy(centroids[i][pqCentroidVector], 0, result,
                    pqCentroidVectorOffset, subVectorSize);
        }

        return result;
    }

    @Override
    public IntArrayList[] splitVectorsByPartitions(int numClusters, int iterations, DistanceFunction distanceFunction,
                                                   boolean initializeCentroidsWithKMeansPlusPlus, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("split vectors by partitions",
                "partitions count", String.valueOf(numClusters));
        try {
            var distanceTables = buildDistanceTables(centroids, quantizersCount, subVectorSize, distanceFunction);
            var pqCentroids = new byte[numClusters * quantizersCount];

            calculateClusters(numClusters, iterations, vectorsCount, pqCentroids, distanceTables, initializeCentroidsWithKMeansPlusPlus, progressTracker);

            var vectorsByPartitions = new IntArrayList[numClusters];
            for (int i = 0; i < numClusters; i++) {
                vectorsByPartitions[i] = new IntArrayList(vectorsCount / numClusters);
            }

            for (int i = 0; i < vectorsCount; i++) {
                var twoClosestClusters = findTwoClosestClusters(i, pqCentroids, distanceTables);

                var firstPartition = (int) (twoClosestClusters >>> 32);
                var secondPartition = (int) twoClosestClusters;

                vectorsByPartitions[firstPartition].add(i);

                if (firstPartition != secondPartition) {
                    vectorsByPartitions[secondPartition].add(i);
                }

                if (progressTracker.isProgressUpdatedRequired()) {
                    progressTracker.progress(i * 100.0 / vectorsCount);
                }
            }

            return vectorsByPartitions;
        } finally {
            progressTracker.pullPhase();
        }
    }

    @Override
    public float[][] calculateCentroids(int clustersCount, int iterations, DistanceFunction distanceFunction, boolean initializeCentroidsWithKMeansPlusPlus, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("calculate centroids");
        try {
            var numVectors = (int) (pqVectors.byteSize() / quantizersCount);
            var distanceTables = buildDistanceTables(centroids, quantizersCount, subVectorSize, distanceFunction);
            var pqCentroids = new byte[clustersCount * quantizersCount];

            calculateClusters(clustersCount, iterations, numVectors, pqCentroids, distanceTables, initializeCentroidsWithKMeansPlusPlus, progressTracker);

            var result = new float[clustersCount][];
            for (int i = 0; i < clustersCount; i++) {
                result[i] = decodeVector(pqCentroids, i);
            }

            return result;
        } finally {
            progressTracker.pullPhase();
        }
    }

    private void calculateClusters(int numClusters, int iterations, long numVectors,
                                   byte[] pqCentroids, float[] distanceTables,
                                   boolean initializeCentroidsWithKMeansPlusPlus, @NotNull ProgressTracker progressTracker) {
        var cores = (int) Math.min(Runtime.getRuntime().availableProcessors(), numVectors);
        progressTracker.pushPhase("PQ k-means clustering",
                "max iterations", String.valueOf(iterations),
                "clusters count", String.valueOf(numClusters),
                "vectors count", String.valueOf(numVectors),
                "cores", String.valueOf(cores));
        try (var arena = Arena.openShared()) {
            var centroidIndexes = arena.allocate(numVectors * Integer.BYTES, ValueLayout.JAVA_INT.byteAlignment());

            if (initializeCentroidsWithKMeansPlusPlus) {
                initializeCentroidsKMeansPlusPlus(pqVectors, numVectors, quantizersCount, distanceTables, numClusters, pqCentroids, progressTracker);
            } else {
                initializeCentroidsRandomly(pqVectors, numVectors, quantizersCount, numClusters, pqCentroids);
            }

            var histogram = new float[numClusters * quantizersCount * codeBaseSize];
            var atomicIntegerHistogram = new int[numClusters * quantizersCount * codeBaseSize];

            var v = new float[codeBaseSize];
            var mulBuffer = new float[4];

            try (var executors = Executors.newFixedThreadPool(cores, r -> {
                var thread = new Thread(r);
                thread.setName("pq-kmeans-cluster-lookup-" + thread.threadId());
                return thread;
            })) {
                var assignmentSize = (numVectors + cores - 1) / cores;
                var futures = new Future[cores];

                for (int iteration = 0; iteration < iterations; iteration++) {
                    progressTracker.pushPhase("Iteration " + iteration);
                    try {
                        progressTracker.pushPhase("clusters assignment");
                        try {
                            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores,
                                    progressTracker);
                            for (int i = 0; i < cores; i++) {
                                var start = i * assignmentSize;
                                var end = Math.min(start + assignmentSize, numVectors);
                                var id = i;

                                futures[i] = executors.submit(() -> {
                                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                                        boolean assignedDifferently = false;
                                        var localSize = end - start;

                                        for (long k = 0; k < localSize; k++) {
                                            var vectorIndex = start + k;

                                            var prevIndex = centroidIndexes.getAtIndex(ValueLayout.JAVA_INT, vectorIndex);
                                            var centroidIndex = findClosestCluster(vectorIndex,
                                                    pqCentroids, distanceTables);
                                            centroidIndexes.setAtIndex(ValueLayout.JAVA_INT, vectorIndex, centroidIndex);
                                            assignedDifferently = assignedDifferently || prevIndex != centroidIndex;

                                            localTracker.progress(k * 100.0 / localSize);
                                        }

                                        return assignedDifferently;
                                    }
                                });
                            }

                            var assignedDifferently = false;
                            for (var future : futures) {
                                try {
                                    //noinspection unchecked
                                    assignedDifferently = assignedDifferently || ((Future<Boolean>) future).get();
                                } catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException("Error during cluster assigment phase in PQ kmeans clustering.", e);
                                }
                            }

                            if (!assignedDifferently) {
                                break;
                            }
                        } finally {
                            progressTracker.pullPhase();
                        }

                        generateHistogram(pqVectors, centroidIndexes, quantizersCount, codeBaseSize,
                                atomicIntegerHistogram, progressTracker);

                        for (int i = 0; i < histogram.length; i++) {
                            histogram[i] = (float) atomicIntegerHistogram[i];
                        }

                        progressTracker.pushPhase("centroids generation");
                        try {
                            var assignedDifferently = false;

                            for (int k = 0, histogramOffset = 0, centroidIndex = 0; k < numClusters; k++) {
                                for (int q = 0; q < quantizersCount; q++, histogramOffset += codeBaseSize) {
                                    var clusterDistanceTableOffset = MatrixOperations.threeDMatrixIndex(codeBaseSize,
                                            codeBaseSize, q, 0, 0);
                                    MatrixOperations.multiply(distanceTables, clusterDistanceTableOffset,
                                            codeBaseSize, codeBaseSize, histogram, histogramOffset,
                                            v, mulBuffer);
                                    var minIndex = MatrixOperations.minIndex(v, 0, codeBaseSize);
                                    assert minIndex < codeBaseSize;

                                    var prevIndex = Byte.toUnsignedInt(pqCentroids[centroidIndex]);

                                    pqCentroids[centroidIndex] = (byte) minIndex;
                                    assignedDifferently = assignedDifferently || prevIndex != minIndex;
                                    centroidIndex++;
                                }

                                if (progressTracker.isProgressUpdatedRequired()) {
                                    progressTracker.progress(k * 100.0 / numClusters);
                                }
                            }

                            if (!assignedDifferently) {
                                break;
                            }
                        } finally {
                            progressTracker.pullPhase();
                        }
                    } finally {
                        progressTracker.pullPhase();
                    }
                }
            }
        } finally {
            progressTracker.pullPhase();
        }
    }

    private void initializeCentroidsRandomly(MemorySegment pqVectors, long numVectors, int quantizersCount, int numClusters, byte[] pqCentroids) {
        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        for (int i = 0; i < numClusters; i++) {
            var vecIndex = rng.nextLong(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, vecIndex * quantizersCount, pqCentroids, i * quantizersCount, quantizersCount);
        }
    }

    /**
     * A smarter than random centroid initialization technique.
     * 1. Initialize the first centroid randomly.
     * 2. Calculate the distance to the closest already chosen centroid `distanceToClosestCentroid` for all the vectors.
     * 3. Choose a vector to become a new centroid based on the weighted probability proportional to `distanceToClosestCentroid^2`.
     * 4. Repeat from 2 until all the centroid initialized.
     * */
    public void initializeCentroidsKMeansPlusPlus(MemorySegment pqVectors, long numVectors, int quantizersCount, float[] distanceTable, int numClusters, byte[] pqCentroids, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("Centroids initialization");
        try (var arena = Arena.openShared()) {
            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

            // initialize the first centroid randomly
            var firstCentroidIndex = rng.nextLong(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, firstCentroidIndex * quantizersCount, pqCentroids, 0, quantizersCount);

            // here we keep distance to the closest centroid for every vector
            var distancesToClosestCentroid = arena.allocate(numVectors * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
            // we do not know vectors' distances to any centroids, so make them infinite
            for (long vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
                distancesToClosestCentroid.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, Float.MAX_VALUE);
            }

            var cores = (int) Math.min(Runtime.getRuntime().availableProcessors(), numVectors);
            try (var executors = Executors.newFixedThreadPool(cores, r -> {
                var thread = new Thread(r);
                thread.setName("pq-kmeans-cluster-initialization-" + thread.threadId());
                return thread;
            })) {
                var assignmentSize = (numVectors + cores - 1) / cores;
                var futures = new Future[cores];
                var sumSquaredDistancesToClosestCentroid = new double[cores];

                for (int newCentroidIdx = 1; newCentroidIdx < numClusters; newCentroidIdx++) {
                    progressTracker.pushPhase("Initialize centroid " + newCentroidIdx);
                    try {
                        var lastCentroidIdx = newCentroidIdx - 1;
                        var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
                        // map
                        for (int i = 0; i < cores; i++) {
                            var start = i * assignmentSize;
                            var end = Math.min(start + assignmentSize, numVectors);
                            var id = i;

                            futures[i] = executors.submit(() -> {
                                try (var localTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                                    var localSize = end - start;
                                    double sumSquaredDistanceToClosestCentroid = 0f;
                                    for (long k = 0; k < localSize; k++) {
                                        long vectorIdx = start + k;
                                        float distanceToClosestCentroid = distancesToClosestCentroid.getAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx);
                                        // process the distance to the last initialized centroid
                                        float distanceToLastCentroid = symmetricDistance(pqVectors, vectorIdx, pqCentroids, lastCentroidIdx, distanceTable, quantizersCount, codeBaseSize);
                                        if (distanceToLastCentroid < distanceToClosestCentroid) {
                                            distanceToClosestCentroid = distanceToLastCentroid;
                                            distancesToClosestCentroid.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, distanceToClosestCentroid);
                                        }
                                        sumSquaredDistanceToClosestCentroid += (distanceToClosestCentroid * distanceToClosestCentroid);
                                        localTracker.progress(k * 100.0 / localSize);
                                    }
                                    sumSquaredDistancesToClosestCentroid[id] = sumSquaredDistanceToClosestCentroid;
                                }
                            });
                        }

                        for (var future : futures) {
                            try {
                                future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException("Error during cluster initialization phase in PQ kmeans clustering.", e);
                            }
                        }

                        // reduce
                        double sumSquaredDistanceToClosestCentroid = 0;
                        for (var i = 0; i < cores; i++) {
                            sumSquaredDistanceToClosestCentroid += sumSquaredDistancesToClosestCentroid[i];
                        }

                        // choose a vector to become a new centroid
                        var randomSum = rng.nextDouble(sumSquaredDistanceToClosestCentroid);
                        double runningSum = 0;
                        long vectorToBecomeCentroidIdx = 0;
                        while (vectorToBecomeCentroidIdx < numVectors - 1) {
                            var distance = distancesToClosestCentroid.getAtIndex(ValueLayout.JAVA_FLOAT, vectorToBecomeCentroidIdx);
                            runningSum += (distance * distance);
                            if (runningSum >= randomSum) {
                                break;
                            }
                            vectorToBecomeCentroidIdx++;
                        }
                        MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, vectorToBecomeCentroidIdx * quantizersCount, pqCentroids, newCentroidIdx * quantizersCount, quantizersCount);
                    } finally {
                        progressTracker.pullPhase(); // Another centroid initialized
                    }
                }
            }
        } finally {
            progressTracker.pullPhase(); // Initialization completed
        }
    }

    static void generateHistogram(final MemorySegment pqVectors,
                                  final MemorySegment clusters,
                                  final int quantizersCount,
                                  final int codeBaseSize,
                                  final int[] histogram, @NotNull ProgressTracker progressTracker) {
        Arrays.fill(histogram, 0);

        var numCodes = pqVectors.byteSize();
        var numVectors = numCodes / quantizersCount;

        var cores = (int) Math.min(Runtime.getRuntime().availableProcessors(), numVectors);
        progressTracker.pushPhase("PQ k-means histogram generation",
                "cores", String.valueOf(cores));

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-kmeans-histogram-generator-" + thread.threadId());
            return thread;
        })) {
            var assignmentSize = (numVectors + cores - 1) / cores;
            var futures = new Future[cores];

            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
            for (int i = 0; i < cores; i++) {
                var start = i * assignmentSize;
                var end = Math.min(start + assignmentSize, numVectors);
                var id = i;

                futures[i] = executors.submit(() -> {
                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                        var localSize = end - start;

                        for (long k = 0, codeIndex = start * quantizersCount; k < localSize; k++) {
                            var vectorIndex = start + k;
                            var clusterIndex = clusters.getAtIndex(ValueLayout.JAVA_INT, vectorIndex);

                            for (int n = 0; n < quantizersCount; n++) {
                                var code = Byte.toUnsignedInt(pqVectors.get(ValueLayout.JAVA_BYTE, codeIndex));
                                var histogramIndex = MatrixOperations.threeDMatrixIndex(quantizersCount,
                                        codeBaseSize, clusterIndex, n, code);
                                ATOMIC_HISTOGRAM_VARHANDLE.getAndAdd(histogram, histogramIndex, 1);
                                codeIndex++;
                            }

                            localTracker.progress(k * 100.0 / localSize);
                        }
                    }
                });
            }

            for (var future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Error during histogram generation phase in PQ kmeans clustering.", e);
                }
            }
        } finally {
            progressTracker.pullPhase();
        }
    }


    private int findClosestCluster(final long vectorIndex, final byte[] centroids,
                                   final float[] distanceTable) {
        int minIndex = -1;
        float minDistance = Float.MAX_VALUE;
        var vectorsCount = centroids.length / quantizersCount;

        for (int index = 0; index < vectorsCount; index++) {
            var distance = symmetricDistance(pqVectors, vectorIndex, centroids, index, distanceTable,
                    quantizersCount, codeBaseSize);

            if (distance < minDistance) {
                minDistance = distance;
                minIndex = index;
            }
        }

        return minIndex;
    }

    private long findTwoClosestClusters(final long vectorIndex, final byte[] centroids,
                                        final float[] distanceTable) {
        int firstMinIndex = 0;
        float firstMinDistance = Float.MAX_VALUE;

        int secondMindIndex = 0;
        float secondMinDistance = Float.MAX_VALUE;

        var vectorsCount = centroids.length / quantizersCount;

        for (int index = 0; index < vectorsCount; index++) {
            var distance = symmetricDistance(pqVectors, vectorIndex, centroids, index, distanceTable,
                    quantizersCount, codeBaseSize);

            if (distance < firstMinDistance) {
                firstMinDistance = distance;
                firstMinIndex = index;
            }

            if (distance < secondMinDistance && distance > firstMinDistance) {
                secondMinDistance = distance;
                secondMindIndex = index;
            }
        }

        return (((long) firstMinIndex) << 32) | secondMindIndex;
    }

    @Override
    MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
        return arena.allocate((long) vectorsCount * quantizersCount);
    }

    @Override
    public float[] blankLookupTable() {
        return new float[quantizersCount * (1 << Byte.SIZE)];
    }

    public void buildLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction) {
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
    public float computeDistanceUsingLookupTable(float[] lookupTable, int vectorIndex) {
        var distance = 0f;

        var pqIndex = (long) quantizersCount * vectorIndex;
        for (long i = pqIndex; i < pqIndex + quantizersCount; i++) {
            var code = pqVectors.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            distance += lookupTable[(int) ((i - pqIndex) * (1 << Byte.SIZE) + code)];
        }

        return distance;
    }

    @Override
    public void computeDistance4BatchUsingLookupTable(float[] lookupTable, int vectorIndex1, int vectorIndex2,
                                                      int vectorIndex3, int vectorIndex4, float[] result) {
        assert result.length == 4;

        var pqIndex1 = (long) quantizersCount * vectorIndex1;
        var pqIndex2 = (long) quantizersCount * vectorIndex2;
        var pqIndex3 = (long) quantizersCount * vectorIndex3;
        var pqIndex4 = (long) quantizersCount * vectorIndex4;

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
        dataOutputStream.writeInt(quantizersCount);
        dataOutputStream.writeInt(codeBaseSize);
        dataOutputStream.writeInt(subVectorSize);

        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < codeBaseSize; j++) {
                for (int k = 0; k < subVectorSize; k++) {
                    dataOutputStream.writeFloat(centroids[i][j][k]);
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
        codeBaseSize = dataInputStream.readInt();
        subVectorSize = dataInputStream.readInt();

        centroids = new float[quantizersCount][codeBaseSize][subVectorSize];
        for (int i = 0; i < quantizersCount; i++) {
            for (int j = 0; j < codeBaseSize; j++) {
                for (int k = 0; k < subVectorSize; k++) {
                    centroids[i][j][k] = dataInputStream.readFloat();
                }
            }
        }

        var pqVectorsSize = dataInputStream.readLong();
        pqVectors = arena.allocate(pqVectorsSize);
        for (long i = 0; i < pqVectorsSize; i++) {
            pqVectors.set(ValueLayout.JAVA_BYTE, i, dataInputStream.readByte());
        }
    }

    static float[] buildDistanceTables(float[][][] centroids, int quantizersCount, int subVectorSize,
                                       DistanceFunction distanceFunction) {
        var codeSpaceSize = centroids[0].length;
        var result = new float[quantizersCount * codeSpaceSize * codeSpaceSize];


        var batchResult = new float[4];
        for (int n = 0; n < quantizersCount; n++) {
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

                    distanceFunction.computeDistance(origin, 0, vector1, 0, vector2,
                            0, vector3, 0, vector4, 0,
                            batchResult, subVectorSize);

                    var offset = baseOffset + j;
                    result[offset] = batchResult[0];
                    result[offset + 1] = batchResult[1];
                    result[offset + 2] = batchResult[2];
                    result[offset + 3] = batchResult[3];
                }


                for (; j <= i; j++) {
                    var origin = centroids[n][i];
                    var vector = centroids[n][j];

                    result[baseOffset + j] = distanceFunction.computeDistance(origin, 0, vector,
                            0, subVectorSize);
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


    static float symmetricDistance(MemorySegment pqVectors, long storedEncodedVectorIndex, byte[] encodedVectors, int encodedVectorIndex,
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

    @Override
    public void close() {
        arena.close();
    }
}
