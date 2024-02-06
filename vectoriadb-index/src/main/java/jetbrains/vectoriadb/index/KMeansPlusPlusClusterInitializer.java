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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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
 * 2. Calculate the distance to the closest already chosen centroid `distanceToClosestCentroid` for all the vectors.
 * 3. Choose a vector to become a new centroid based on the weighted probability proportional to `distanceToClosestCentroid^2`.
 * 4. Repeat from 2 until all the centroid initialized.
 * It is possible to adjust the behaviour by setting a random source and the power to calculate weighted probability.
 * */
class KMeansPlusPlusClusterInitializer implements ClusterInitializer {
    @NotNull
    private final RandomSource randomSource;
    private final int power;

    KMeansPlusPlusClusterInitializer() {
        randomSource = RandomSource.XO_RO_SHI_RO_128_PP;
        power = 2;
    }

    KMeansPlusPlusClusterInitializer(@NotNull RandomSource randomSource, int power) {
        this.randomSource = randomSource;
        this.power = power;
    }

    private double weightedDistance(double distance) {
        return Math.pow(distance, power);
    }

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
            var rng = randomSource.create();

            // initialize the first centroid randomly
            var firstCentroidIndex = rng.nextLong(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, firstCentroidIndex * quantizersCount, pqCentroids, 0, quantizersCount);

            // here we keep distance to the closest centroid for every vector
            var distancesToClosestCentroid = arena.allocate(numVectors * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
            // we do not know vectors' distances to any centroids, so make them infinite
            for (long vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
                distancesToClosestCentroid.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, Float.MAX_VALUE);
            }

            var vectorsBecameCentroids = new LongOpenHashSet(numClusters);

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
                                        float distanceToLastCentroid = AbstractQuantizer.symmetricDistance(pqVectors, vectorIdx, pqCentroids, lastCentroidIdx, distanceTable, quantizersCount, codeBaseSize);
                                        if (distanceToLastCentroid < distanceToClosestCentroid) {
                                            distanceToClosestCentroid = distanceToLastCentroid;
                                            distancesToClosestCentroid.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, distanceToClosestCentroid);
                                        }

                                        sumSquaredDistanceToClosestCentroid += weightedDistance(distanceToClosestCentroid);
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
                            runningSum += weightedDistance(distance);
                            if (runningSum >= randomSum) {
                                break;
                            }
                            vectorToBecomeCentroidIdx++;
                        }
                        // make sure we do not make a single vector centroid twice
                        while (vectorsBecameCentroids.contains(vectorToBecomeCentroidIdx)) {
                            vectorToBecomeCentroidIdx++;
                            vectorToBecomeCentroidIdx = vectorToBecomeCentroidIdx % numVectors;
                        }
                        vectorsBecameCentroids.add(vectorToBecomeCentroidIdx);
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
}