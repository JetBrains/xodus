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
    private final CodeSegment centroidIdxByVectorIdx;

    @NotNull
    private final ParallelBuddy pBuddy;

    KMeansClustering(
            @NotNull DistanceFunction distanceFun,
            @NotNull VectorReader vectorReader,
            @NotNull FloatVectorSegment centroids,
            @NotNull CodeSegment centroidIdxByVectorIdx,
            int maxIterations,
            @NotNull ParallelBuddy pBuddy
    ) {
        numVectors = vectorReader.size();
        this.numClusters = centroids.count();

        if (numVectors < numClusters) throw new IllegalArgumentException();
        if (numClusters > centroidIdxByVectorIdx.maxNumberOfCodes()) throw new IllegalArgumentException();
        if (numVectors != centroidIdxByVectorIdx.count()) throw new IllegalArgumentException();
        if (vectorReader.dimensions() != centroids.dimensions()) throw new IllegalArgumentException();

        this.vectorDimensions = centroids.dimensions();
        this.centroids = centroids;
        this.centroidIdxByVectorIdx = centroidIdxByVectorIdx;
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
                    /*
                     * iteration > 0 is required for the case when we calculate a single centroid.
                     * A single centroid can not be assigned differently obviously, and if we break here
                     * the centroid will never be actually calculated, and we just return whatever the centroid
                     * was initialized with.
                     * So, we make sure centroids are calculated at least once.
                     * */
                    if (!assignedDifferently && iteration > 0) {
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
        pBuddy.runSplitEvenly(
                "clusters assigment",
                numVectors,
                progressTracker,
                (workerIdx, vectorIdx) -> {
                    var prevCentroidIdx = centroidIdxByVectorIdx.get(vectorIdx);
                    var centroidIdx = findClosestCluster(vectorIdx);
                    centroidIdxByVectorIdx.set(vectorIdx, centroidIdx);
                    assignedDifferently[workerIdx] = assignedDifferently[workerIdx] || prevCentroidIdx != centroidIdx;
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
        pBuddy.runSplitEvenly(
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
                    var centroidIdx = centroidIdxByVectorIdx.get(vectorIdx);
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

        for (int i = 0; i < numClusters; i++) {
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
