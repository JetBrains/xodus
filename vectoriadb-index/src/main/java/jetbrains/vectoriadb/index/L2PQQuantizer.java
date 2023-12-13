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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class L2PQQuantizer extends AbstractQuantizer {
    private static final VarHandle ATOMIC_HISTOGRAM_VARHANDLE = MethodHandles.arrayElementVarHandle(int[].class);

    // how many codebooks we use to encode a vector
    private int codebookCount;
    // how many vector features we encode with a single codebook
    private int[] codebookDimensions;
    private int maxCodebookDimensions;
    private int[] codebookDimensionOffset;
    // how many values a single codebook should encode
    private int codeBaseSize;
    // 1st dimension quantizer/codebook index
    // 2nd index of code inside code book
    // 3d dimension centroid vector
    private float[][][] codebooks;

    private int vectorCount;
    private int vectorDimensions;
    // encoded vectors
    private MemorySegment pqVectors;

    private final Arena arena;

    L2PQQuantizer() {
        this.arena = Arena.ofShared();
    }

    public float[] getVectorApproximation(int vectorIdx) {
        var codedVector = pqVectors.asSlice((long) vectorIdx * codebookCount, codebookCount);
        return decodeVector(codedVector);
    }

    private float[] decodeVector(MemorySegment codedVector) {
        var result = new float[vectorDimensions];

        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            var code = Byte.toUnsignedInt(codedVector.getAtIndex(ValueLayout.JAVA_BYTE, codebookIdx));
            System.arraycopy(codebooks[codebookIdx][code], 0, result, codebookDimensionOffset[codebookIdx], codebookDimensions[codebookIdx]);
        }

        return result;
    }

    private void initializeCodebooks(int codebookCount, int vectorCount, int vectorDimensions) {
        var init = new CodebookInitializer(codebookCount, vectorCount, vectorDimensions);
        this.codebookCount = codebookCount;
        codebookDimensions = init.getCodebookDimensions();
        maxCodebookDimensions = init.getMaxCodebookDimensions();
        codebookDimensionOffset = init.getCodebookDimensionOffset();
        codeBaseSize = init.getCodeBaseSize();
        codebooks = init.getCodebooks();
        this.vectorCount = vectorCount;
        this.vectorDimensions = vectorDimensions;
    }

    // Initialize, make PQ code for the vectors

    @Override
    public void generatePQCodes(VectorReader vectorReader, int codebookCount, @NotNull ProgressTracker progressTracker) {
        initializeCodebooks(codebookCount, vectorReader.size(), vectorReader.dimensions());

        var minBatchSize = 16;

        var cores = Math.min(Runtime.getRuntime().availableProcessors(), codebookCount);
        var batchSize = Math.max(minBatchSize, 2 * 1024 * 1024 / (Float.BYTES * maxCodebookDimensions)) / cores;

        progressTracker.pushPhase("PQ codes creation",
                "codebook count", String.valueOf(codebookCount),
                "max codebook dimension", String.valueOf(maxCodebookDimensions),
                "code base size", String.valueOf(codeBaseSize));

        progressTracker.pushPhase("KMeans clustering", "batch size", String.valueOf(batchSize),
                "min batch size", String.valueOf(minBatchSize),
                "cores", String.valueOf(cores));

        var kMeans = new KMeansMiniBatchGD[codebookCount];
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            kMeans[codebookIdx] = new KMeansMiniBatchGD(CodebookInitializer.CODE_BASE_SIZE, 50, codebookDimensionOffset[codebookIdx], codebookDimensions[codebookIdx], vectorReader);
        }

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-kmeans-thread-" + thread.threadId());
            return thread;
        })) {
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(codebookCount, progressTracker);

            var futures = new Future[codebookCount];
            for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
                var km = kMeans[codebookIdx];
                var id = codebookIdx;
                futures[codebookIdx] = executors.submit(() -> {
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

        cores = Math.min(Runtime.getRuntime().availableProcessors(), vectorCount);

        progressTracker.pushPhase("PQ codes creation", "cores", String.valueOf(cores));
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            var centroids = kMeans[codebookIdx].centroids;

            var index = 0;
            for (int code = 0; code < codeBaseSize; code++) {
                System.arraycopy(centroids, index, codebooks[codebookIdx][code], 0, codebookDimensions[codebookIdx]);
                index += codebookDimensions[codebookIdx];
            }
        }

        pqVectors = allocateMemoryForPqVectors(codebookCount, vectorCount, arena);

        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName("pq-code-assignment-thread-" + thread.threadId());
            return thread;
        })) {
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
            var assignmentSize = (vectorCount + cores - 1) / cores;
            var futures = new Future[cores];

            for (var n = 0; n < cores; n++) {
                var start = n * assignmentSize;
                var end = Math.min(start + assignmentSize, vectorCount);

                var id = n;
                futures[n] = executors.submit(() -> {
                    try (var localProgressTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                        var localSize = end - start;
                        for (var k = 0; k < localSize; k++) {
                            var vectorIndex = start + k;
                            var vector = vectorReader.read(vectorIndex);

                            for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
                                var centroidIndex = L2DistanceFunction.INSTANCE.findClosestVector(kMeans[codebookIdx].centroids, vector, codebookDimensionOffset[codebookIdx], codebookDimensions[codebookIdx]);
                                pqVectors.set(ValueLayout.JAVA_BYTE, (long) vectorIndex * codebookCount + codebookIdx, (byte) centroidIndex);
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

    private MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
        return arena.allocate((long) vectorsCount * quantizersCount);
    }

    // Calculate distances using the lookup table

    @Override
    public float[] blankLookupTable() {
        /*
         * Every quantizer can get 256 values (one byte).
         * Every value corresponds to a single vector (256 vectors per a single quantizer).
         * So we want to have a precalculated distance from a query vector `q` to all the quantizersCount * 256 vectors.
         * Having these precalculated distances we can easily calculate an approximate distance from
         * the query vector `q` to any of the encoded vectors `pqVectors` in the database.
         */
        // todo why not to use CODE_BASE_SIZE=256 instead of 1 << Byte.SIZE, it may make the code easier to read
        return new float[codebookCount * (1 << Byte.SIZE)];
    }

    @Override
    public void buildLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction) {
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            for (int code = 0; code < codeBaseSize; code++) {
                var codebookVector = codebooks[codebookIdx][code];
                var distance = distanceFunction.computeDistance(codebookVector, 0, vector, codebookDimensionOffset[codebookIdx], codebookDimensions[codebookIdx]);
                lookupTable[codebookIdx * (1 << Byte.SIZE) + code] = distance;
            }
        }
    }

    @Override
    public float computeDistanceUsingLookupTable(float[] lookupTable, int vectorIndex) {
        var distance = 0f;

        var pqIndex = (long) codebookCount * vectorIndex;
        for (long i = pqIndex; i < pqIndex + codebookCount; i++) {
            var code = pqVectors.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            distance += lookupTable[(int) ((i - pqIndex) * (1 << Byte.SIZE) + code)];
        }

        return distance;
    }

    @Override
    public void computeDistance4BatchUsingLookupTable(float[] lookupTable, int vectorIndex1, int vectorIndex2,
                                                      int vectorIndex3, int vectorIndex4, float[] result) {
        assert result.length == 4;

        var pqIndex1 = (long) codebookCount * vectorIndex1;
        var pqIndex2 = (long) codebookCount * vectorIndex2;
        var pqIndex3 = (long) codebookCount * vectorIndex3;
        var pqIndex4 = (long) codebookCount * vectorIndex4;

        var result1 = 0.0f;
        var result2 = 0.0f;
        var result3 = 0.0f;
        var result4 = 0.0f;

        for (int i = 0; i < codebookCount; i++) {
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


    // PQ k-means clustering

    @Override
    public VectorsByPartitions splitVectorsByPartitions(VectorReader vectorReader, int numClusters, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("split vectors by partitions",
                "partitions count", String.valueOf(numClusters));
        try {
            var distanceTables = buildDistanceTables(codebooks, codebookDimensions, distanceFunction);
            var pqCentroids = new byte[numClusters * codebookCount];

            calculateClusters(numClusters, iterations, vectorCount, pqCentroids, distanceTables, progressTracker);

            var vectorsByPartitions = new IntArrayList[numClusters];
            for (int i = 0; i < numClusters; i++) {
                vectorsByPartitions[i] = new IntArrayList(vectorCount / numClusters);
            }

            for (int i = 0; i < vectorCount; i++) {
                var twoClosestClusters = findTwoClosestClusters(i, pqCentroids, distanceTables);

                var firstPartition = (int) (twoClosestClusters >>> 32);
                var secondPartition = (int) twoClosestClusters;

                vectorsByPartitions[firstPartition].add(i);

                if (firstPartition != secondPartition) {
                    vectorsByPartitions[secondPartition].add(i);
                }

                if (progressTracker.isProgressUpdatedRequired()) {
                    progressTracker.progress(i * 100.0 / vectorCount);
                }
            }

            var centroids = decodeVectors(pqCentroids);
            return new VectorsByPartitions(centroids, vectorsByPartitions);
        } finally {
            progressTracker.pullPhase();
        }
    }

    @Override
    public float[][] calculateCentroids(VectorReader vectorReader, int numClusters, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("calculate centroids");
        try {
            var numVectors = (int) (pqVectors.byteSize() / codebookCount);
            var distanceTables = buildDistanceTables(codebooks, codebookDimensions, distanceFunction);
            var pqCentroids = new byte[numClusters * codebookCount];

            calculateClusters(numClusters, iterations, numVectors, pqCentroids, distanceTables, progressTracker);

            return decodeVectors(pqCentroids);
        } finally {
            progressTracker.pullPhase();
        }
    }

    private float[][] decodeVectors(byte[] vectors) {
        var count = vectors.length / codebookCount;
        var result = new float[count][];
        for (int i = 0; i < count; i++) {
            result[i] = decodeVector(vectors, i);
        }
        return result;
    }

    private float[] decodeVector(byte[] vectors, int vectorIndex) {
        var result = new float[vectorDimensions];

        var offset = vectorIndex * codebookCount;
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            var code = Byte.toUnsignedInt(vectors[codebookIdx + offset]);
            System.arraycopy(codebooks[codebookIdx][code], 0, result, codebookDimensionOffset[codebookIdx], codebookDimensions[codebookIdx]);
        }

        return result;
    }

    private void calculateClusters(int numClusters, int iterations, long numVectors, byte[] pqCentroids, float[] distanceTables, @NotNull ProgressTracker progressTracker) {
        var cores = (int) Math.min(Runtime.getRuntime().availableProcessors(), numVectors);
        progressTracker.pushPhase("PQ k-means clustering",
                "max iterations", String.valueOf(iterations),
                "clusters count", String.valueOf(numClusters),
                "vectors count", String.valueOf(numVectors),
                "cores", String.valueOf(cores));
        try (var memorySession = Arena.ofShared()) {
            var centroidIndexByVectorIndex = memorySession.allocate(numVectors * Integer.BYTES, ValueLayout.JAVA_INT.byteAlignment());
            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

            for (int i = 0; i < numClusters; i++) {
                var vecIndex = rng.nextLong(numVectors);
                MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, vecIndex * codebookCount, pqCentroids, i * codebookCount, codebookCount);
            }

            var histogram = new float[numClusters * codebookCount * codeBaseSize];
            var atomicIntegerHistogram = new int[numClusters * codebookCount * codeBaseSize];

            var v = new float[codeBaseSize];
            var mulBuffer = new float[4];

            try (
                    // todo why not to use a single pool
                    // if we need different names for the threads, we can override Runnable.toString() and use it as the thread name
                    var assignVectorsExecutors = Executors.newFixedThreadPool(cores, r -> {
                        var thread = new Thread(r);
                        thread.setName("pq-kmeans-cluster-lookup-" + thread.threadId());
                        return thread;
                    });
                    var generateHistogramExecutors = Executors.newFixedThreadPool(cores, r -> {
                        var thread = new Thread(r);
                        thread.setName("pq-kmeans-histogram-generator-" + thread.threadId());
                        return thread;
                    })
            ) {
                var assignmentSize = (numVectors + cores - 1) / cores;
                var futures = new Future[cores];

                for (int iteration = 0; iteration < iterations; iteration++) {
                    progressTracker.pushPhase("Iteration " + iteration);
                    try {
                        var assignedDifferently = assignVectorsToClosestClusters(numVectors, assignmentSize, centroidIndexByVectorIndex, pqCentroids, distanceTables, assignVectorsExecutors, futures, progressTracker);
                        if (!assignedDifferently) {
                            break;
                        }

                        generateHistogram(assignmentSize, pqVectors, centroidIndexByVectorIndex, codebookCount, codeBaseSize, atomicIntegerHistogram, generateHistogramExecutors, futures, progressTracker);
                        for (int i = 0; i < histogram.length; i++) {
                            histogram[i] = (float) atomicIntegerHistogram[i];
                        }

                        assignedDifferently = calculateClusterCentroids(numClusters, pqCentroids, distanceTables, histogram, v, mulBuffer, progressTracker);
                        if (!assignedDifferently) {
                            break;
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

    /**
     * Assigns vectors to closest clusters.
     *
     * @return true if the closest cluster is changed for any vector
     * */
    private boolean assignVectorsToClosestClusters(
            long numVectors,
            long assignmentSize,
            // byteSize: numVectors * Integer.BYTES
            MemorySegment centroidIndexByVectorIndex,
            // size: numClusters * quantizersCount
            byte[] pqCentroids,
            // size: quantizersCount * codeSpaceSize * codeSpaceSize
            float[] distanceTables,
            ExecutorService executors,
            // size: cores
            @SuppressWarnings("rawtypes")
            @NotNull Future[] futures,
            @NotNull ProgressTracker progressTracker
    ) {
        progressTracker.pushPhase("clusters assignment");
        try {
            var cores = futures.length;
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
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

                            var prevIndex = centroidIndexByVectorIndex.getAtIndex(ValueLayout.JAVA_INT, vectorIndex);
                            var centroidIndex = findClosestCluster(vectorIndex, pqCentroids, distanceTables);
                            centroidIndexByVectorIndex.setAtIndex(ValueLayout.JAVA_INT, vectorIndex, centroidIndex);
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

            return assignedDifferently;
        } finally {
            progressTracker.pullPhase(); // clusters assignment
        }
    }

    /**
     * <p>The histogram helps to calculate new centroids faster.
     *
     * <p>Here we have vector codes and the index of the nearest cluster for every vector.
     *
     * <p>By the end of the procedure, we have in histogram[cluster1 * quantizer1 * code1] - the number of
     * vectors that are assigned to cluster1 and have for quantizer1 value code1.
     *
     * <p>See {@link L2PQQuantizer#assignVectorsToClosestClusters} to get an idea of how new centroids are calculated
     * using the histogram.
     * */
    static void generateHistogram(
            long assignmentSize,
            // byteSize: vectorsCount * quantizersCount
            final MemorySegment pqVectors,
            // byteSize: numVectors * Integer.BYTES
            final MemorySegment clusterIdxByVectorIdx,
            final int quantizersCount,
            final int codeBaseSize,
            // numClusters * quantizersCount * codeBaseSize
            final int[] histogram,
            ExecutorService executors,
            // size: cores
            @SuppressWarnings("rawtypes")
            @NotNull Future[] futures,
            @NotNull ProgressTracker progressTracker
    ) {
        Arrays.fill(histogram, 0);

        var numCodes = pqVectors.byteSize();
        var numVectors = numCodes / quantizersCount;

        var cores = futures.length;
        progressTracker.pushPhase("PQ k-means histogram generation", "cores", String.valueOf(cores));

        try {
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);
            for (int i = 0; i < cores; i++) {
                var start = i * assignmentSize;
                var end = Math.min(start + assignmentSize, numVectors);
                var id = i;

                futures[i] = executors.submit(() -> {
                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                        var localSize = end - start;

                        for (long localVectorIndex = 0, codeIndex = start * quantizersCount; localVectorIndex < localSize; localVectorIndex++) {
                            var vectorIndex = start + localVectorIndex;
                            var clusterIndex = clusterIdxByVectorIdx.getAtIndex(ValueLayout.JAVA_INT, vectorIndex);

                            for (int quantizerIndex = 0; quantizerIndex < quantizersCount; quantizerIndex++) {
                                var code = Byte.toUnsignedInt(pqVectors.get(ValueLayout.JAVA_BYTE, codeIndex));
                                var histogramIndex = MatrixOperations.threeDMatrixIndex(quantizersCount, codeBaseSize, clusterIndex, quantizerIndex, code);
                                ATOMIC_HISTOGRAM_VARHANDLE.getAndAdd(histogram, histogramIndex, 1);
                                codeIndex++;
                            }

                            localTracker.progress(localVectorIndex * 100.0 / localSize);
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

    /**
     * Calculates new cluster centroids as means of current cluster vectors.
     *
     * @return true if any cluster centroid is changed
     */
    private boolean calculateClusterCentroids(
            int numClusters,
            // size: numClusters * quantizersCount
            byte[] pqCentroids,
            // size: quantizersCount * codeSpaceSize * codeSpaceSize
            float[] distanceTables,
            // size: numClusters * quantizersCount * codeBaseSize
            float[] histogram,
            // size: codeBaseSize
            float[] v,
            // size: 4
            float[] mulBuffer,
            @NotNull ProgressTracker progressTracker
    ) {
        /*
         * The idea of how to calculate new centroids using distanceTable and histogram is the following:
         *
         * 1. Given:
         *  - quantizer1
         *  - For quantizer1, distanceTable1[codeSpaceSize * codeSpaceSize].
         *    distanceTable1[codeI * codeJ] contains the distance between quantizer1.centroid[codeI] and quantizer1.centroid[codeJ].
         *  - cluster1 and respectful clusterCentroid1
         *  - For clusterCentroid1[quantizer1], histogram1[codeSpaceSize].
         *    histogram1[code1] contains the number of vectors in cluster1 that have subVector[quantizer1] = code1.
         * 2. Find codeX. quantizer1.centroid[codeX] should have the min sum distance to all the subVectors[quantizer1] in `cluster1`.
         * 3. Matrix multiplication of distanceTable1 (2D matrix) and histogram1 (1D matrix) gives us sumDistances1[codeSpaceSize] (1D matrix).
         *    sumDistances1[code1] contains the sum distance from quantizer1.centroid[code1] to all the subVectors[quantizer1] in cluster1.
         *    Finding the min in `sumDistances1` gives us codeX.
         * */
        progressTracker.pushPhase("centroids generation");
        try {
            var assignedDifferently = false;

            for (int k = 0, histogramOffset = 0, centroidIndex = 0; k < numClusters; k++) {
                for (int q = 0; q < codebookCount; q++, histogramOffset += codeBaseSize) {
                    var clusterDistanceTableOffset = MatrixOperations.threeDMatrixIndex(codeBaseSize, codeBaseSize, q, 0, 0);
                    MatrixOperations.multiply(distanceTables, clusterDistanceTableOffset, codeBaseSize, codeBaseSize, histogram, histogramOffset, v, mulBuffer);
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

            return assignedDifferently;
        } finally {
            progressTracker.pullPhase();
        }
    }

    private int findClosestCluster(final long vectorIndex, final byte[] centroids,
                                   final float[] distanceTable) {
        int minIndex = -1;
        float minDistance = Float.MAX_VALUE;
        var vectorsCount = centroids.length / codebookCount;

        for (int index = 0; index < vectorsCount; index++) {
            var distance = symmetricDistance(pqVectors, vectorIndex, centroids, index, distanceTable, codebookCount, codeBaseSize);

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

        var vectorsCount = centroids.length / codebookCount;

        for (int index = 0; index < vectorsCount; index++) {
            var distance = symmetricDistance(pqVectors, vectorIndex, centroids, index, distanceTable, codebookCount, codeBaseSize);

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

    static float[] buildDistanceTables(float[][][] codebooks, int[] codebookDimensions, DistanceFunction distanceFunction) {
        var codebookCount = codebooks.length;
        var codeSpaceSize = codebooks[0].length;
        var result = new float[codebookCount * codeSpaceSize * codeSpaceSize];

        var batchResult = new float[4];
        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            for (int code = 0; code < codeSpaceSize; code++) {
                var batchBoundary = code & -4;

                var baseOffset = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, codebookIdx, code, 0);
                var j = 0;
                for (; j < batchBoundary; j += 4) {
                    var origin = codebooks[codebookIdx][code];

                    var vector1 = codebooks[codebookIdx][j];
                    var vector2 = codebooks[codebookIdx][j + 1];
                    var vector3 = codebooks[codebookIdx][j + 2];
                    var vector4 = codebooks[codebookIdx][j + 3];

                    distanceFunction.computeDistance(origin, 0,
                            vector1, 0, vector2, 0,
                            vector3, 0, vector4, 0,
                            batchResult, codebookDimensions[codebookIdx]
                    );

                    var offset = baseOffset + j;
                    result[offset] = batchResult[0];
                    result[offset + 1] = batchResult[1];
                    result[offset + 2] = batchResult[2];
                    result[offset + 3] = batchResult[3];
                }


                for (; j <= code; j++) {
                    var origin = codebooks[codebookIdx][code];
                    var vector = codebooks[codebookIdx][j];

                    result[baseOffset + j] = distanceFunction.computeDistance(origin, 0, vector, 0, codebookDimensions[codebookIdx]);
                }
            }

            for (int i = 0; i < codeSpaceSize; i++) {
                for (int j = i + 1; j < codeSpaceSize; j++) {
                    var firstIndex = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, codebookIdx, i, j);
                    var secondIndex = MatrixOperations.threeDMatrixIndex(codeSpaceSize, codeSpaceSize, codebookIdx, j, i);

                    result[firstIndex] = result[secondIndex];
                }
            }
        }

        return result;
    }

    // Other

    @Override
    public void store(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(codebookCount);
        dataOutputStream.writeInt(vectorCount);
        dataOutputStream.writeInt(vectorDimensions);

        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            for (int code = 0; code < codeBaseSize; code++) {
                for (int dimensionIdx = 0; dimensionIdx < maxCodebookDimensions; dimensionIdx++) {
                    dataOutputStream.writeFloat(codebooks[codebookIdx][code][dimensionIdx]);
                }
            }
        }

        var pqVectorsSize = (long) vectorCount * codebookCount;

        for (long i = 0; i < pqVectorsSize; i++) {
            dataOutputStream.writeByte(pqVectors.get(ValueLayout.JAVA_BYTE, i));
        }
    }

    @Override
    public void load(DataInputStream dataInputStream) throws IOException {
        codebookCount = dataInputStream.readInt();
        vectorCount = dataInputStream.readInt();
        vectorDimensions = dataInputStream.readInt();
        initializeCodebooks(codebookCount, vectorCount, vectorDimensions);

        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            for (int code = 0; code < codeBaseSize; code++) {
                for (int dimensionIdx = 0; dimensionIdx < maxCodebookDimensions; dimensionIdx++) {
                    codebooks[codebookIdx][code][dimensionIdx] = dataInputStream.readFloat();
                }
            }
        }

        var pqVectorsSize = vectorCount * codebookCount;
        pqVectors = arena.allocate(pqVectorsSize);
        for (long i = 0; i < pqVectorsSize; i++) {
            pqVectors.set(ValueLayout.JAVA_BYTE, i, dataInputStream.readByte());
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
