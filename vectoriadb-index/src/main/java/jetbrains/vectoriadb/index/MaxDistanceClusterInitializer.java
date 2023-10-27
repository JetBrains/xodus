package jetbrains.vectoriadb.index;

import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A smarter than random centroid initialization technique.
 * 1. Initialize the first centroid randomly.
 * 2. Take the vector which has the largest distance to its closest centroid.
 * 3. Make the vector new centroid.
 * 4. Repeat from 2 until all the centroid initialized.
 * */
class MaxDistanceClusterInitializer implements ClusterInitializer {
    public void initializeCentroids(
            MemorySegment pqVectors,
            long numVectors,
            float[] distanceTable,
            int quantizersCount,
            int codeBaseSize,
            byte[] pqCentroids,
            int numClusters,
            @NotNull ProgressTracker progressTracker
    ) {
        progressTracker.pushPhase("Centroids initialization");
        try (var arena = Arena.ofShared()) {
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
                var bestVectorIndices = new long[cores];
                var maxDistancesToClosestCentroid = new float[cores];

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
                                    long bestVectorIndex = -1;
                                    float maxDistanceToClosestCentroid = 0;
                                    for (long k = 0; k < localSize; k++) {
                                        long vectorIdx = start + k;
                                        float distanceToClosestCentroid = distancesToClosestCentroid.getAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx);
                                        // process the distance to the last initialized centroid
                                        float distanceToLastCentroid = AbstractQuantizer.symmetricDistance(pqVectors, vectorIdx, pqCentroids, lastCentroidIdx, distanceTable, quantizersCount, codeBaseSize);
                                        if (distanceToLastCentroid < distanceToClosestCentroid) {
                                            distanceToClosestCentroid = distanceToLastCentroid;
                                            distancesToClosestCentroid.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, distanceToClosestCentroid);
                                        }
                                        if (distanceToClosestCentroid > maxDistanceToClosestCentroid) {
                                            maxDistanceToClosestCentroid = distanceToClosestCentroid;
                                            bestVectorIndex = vectorIdx;
                                        }
                                        localTracker.progress(k * 100.0 / localSize);
                                    }
                                    bestVectorIndices[id] = bestVectorIndex;
                                    maxDistancesToClosestCentroid[id] = maxDistanceToClosestCentroid;
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
                        var bestVectorIndex = bestVectorIndices[0];
                        var maxDistanceToClosestCentroid = maxDistancesToClosestCentroid[0];
                        for (var i = 1; i < cores; i++) {
                            if (maxDistancesToClosestCentroid[i] > maxDistanceToClosestCentroid) {
                                maxDistanceToClosestCentroid = maxDistancesToClosestCentroid[i];
                                bestVectorIndex = bestVectorIndices[i];
                            }
                        }
                        MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, bestVectorIndex * quantizersCount, pqCentroids, newCentroidIdx * quantizersCount, quantizersCount);
                    } finally {
                        progressTracker.pullPhase(); // Another centroid initialized
                    }
                }
            }
        } finally {
            progressTracker.pullPhase(); // Initialization completed
        }
    }
}