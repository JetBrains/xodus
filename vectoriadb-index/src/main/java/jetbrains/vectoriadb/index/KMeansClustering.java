package jetbrains.vectoriadb.index;

import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;

public final class KMeansClustering {
    @NotNull
    private final DistanceFunction distanceFun;

    @NotNull
    private final VectorReader vectorReader;
    private final int numVectors;
    private final int vectorDimensions;

    @NotNull
    private final FloatVectorSegment centroids;
    private final int numClusters;

    private final int maxIterations;

    /**
     * centroidIdx by vectorIdx
     * */
    @NotNull
    private final CodeSegment codes;

    @NotNull
    private final ParallelBuddy pBuddy;

    KMeansClustering(
            @NotNull DistanceFunction distanceFun,
            @NotNull VectorReader vectorReader,
            @NotNull FloatVectorSegment centroids,
            @NotNull CodeSegment codes,
            int maxIterations,
            @NotNull ParallelBuddy pBuddy
    ) {
        numVectors = vectorReader.size();
        this.numClusters = centroids.count();

        if (numVectors < numClusters) throw new IllegalArgumentException();
        if (numClusters > codes.maxNumberOfCodes()) throw new IllegalArgumentException();
        if (numVectors != codes.count()) throw new IllegalArgumentException();
        if (vectorReader.dimensions() != centroids.dimensions()) throw new IllegalArgumentException();

        this.vectorDimensions = centroids.dimensions();
        this.centroids = centroids;
        this.codes = codes;
        this.distanceFun = distanceFun;
        this.vectorReader = vectorReader;

        this.maxIterations = maxIterations;
        this.pBuddy = pBuddy;
    }

    public void calculateCentroids(@NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("K-means clustering",
                "max iterations", String.valueOf(maxIterations),
                "clusters count", String.valueOf(numClusters),
                "vectors count", String.valueOf(numVectors)
        );

        try (
                var sessionArena = Arena.ofShared()
        ) {
            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            var vectorCountForCentroid = IntSegment.makeNativeSegment(sessionArena, numClusters);
            // use them to parallelize centroids calculation
            var centroidsPerCore = FloatVectorSegment.makeNativeSegments(sessionArena, pBuddy.numWorkers(), numClusters, vectorDimensions);
            var vectorCountForCentroidPerCore = IntSegment.makeNativeSegments(sessionArena, pBuddy.numWorkers(), numClusters);

            // initialize centroids
            // todo use centroid initializers from the other branch
            for (int i = 0; i < numClusters; i++) {
                var vectorIndex = rng.nextInt(numVectors);
                centroids.set(i, vectorReader.read(vectorIndex));
            }

            for (int iteration = 0; iteration < maxIterations; iteration++) {
                progressTracker.pushPhase("Iteration " + iteration);
                try {
                    var assignedDifferently = assignVectorsToClosestClusters(progressTracker);
                    if (!assignedDifferently) {
                        break;
                    }
                    calculateCentroids(centroidsPerCore, vectorCountForCentroidPerCore, vectorCountForCentroid, progressTracker);
                } finally {
                    progressTracker.pullPhase();
                }
            }
        } finally {
            progressTracker.pullPhase();
        }

    }

    private boolean assignVectorsToClosestClusters(@NotNull final ProgressTracker progressTracker) {
        final var assignedDifferently = new boolean[pBuddy.numWorkers()];
        pBuddy.run(
                "clusters assigment",
                numVectors,
                progressTracker,
                (workerIdx, vectorIdx) -> {
                    var prevIndex = codes.get(vectorIdx);
                    var centroidIndex = findClosestCluster(vectorIdx);
                    codes.set(vectorIdx, centroidIndex);
                    assignedDifferently[workerIdx] = assignedDifferently[workerIdx] || prevIndex != centroidIndex;
                }
        );
        for (var v : assignedDifferently) {
            if (v) return true;
        }
        return false;
    }

    private void calculateCentroids(
            final FloatVectorSegment[] centroidsPerCore,
            final IntSegment[] vectorCountForCentroidPerCore,
            final IntSegment vectorCountForCentroid,
            @NotNull final ProgressTracker progressTracker
    ) {
        pBuddy.run(
                "centroids calculation",
                numVectors,
                progressTracker,
                // init worker
                (workerIdx) -> {
                    centroidsPerCore[workerIdx].fill((byte) 0);
                    vectorCountForCentroidPerCore[workerIdx].fill((byte) 0);
                },
                // process an item
                (workerIdx, vectorIdx) -> {
                    var vector = vectorReader.read(vectorIdx);
                    var centroidIdx = codes.get(vectorIdx);
                    centroidsPerCore[workerIdx].add(centroidIdx, vector, 0);
                    vectorCountForCentroidPerCore[workerIdx].add(centroidIdx, 1);
                }
        );

        centroids.fill((byte) 0);
        vectorCountForCentroid.fill((byte) 0);
        for (int workerId = 0; workerId < pBuddy.numWorkers(); workerId++) {
            for (int centroidIdx = 0; centroidIdx < numClusters; centroidIdx++) {
                centroids.add(centroidIdx, centroidsPerCore[workerId], centroidIdx);
                vectorCountForCentroid.add(centroidIdx, vectorCountForCentroidPerCore[workerId].get(centroidIdx));
            }
        }

        for (int centroidIdx = 0; centroidIdx < numClusters; centroidIdx++) {
            var vectorCount = vectorCountForCentroid.get(centroidIdx);
            if (vectorCount > 0) {
                centroids.div(centroidIdx, vectorCount);
            }
        }
    }

    private int findClosestCluster(int vectorIdx) {
        int minIndex = -1;
        float minDistance = Float.MAX_VALUE;
        var vector = vectorReader.read(vectorIdx);

        for (int i = 0; i < numClusters; i += 4) {
            var centroid = centroids.get(i);

            var distance = distanceFun.computeDistance(vector, 0, centroid, 0, vectorDimensions);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }

        return minIndex;
    }
}
