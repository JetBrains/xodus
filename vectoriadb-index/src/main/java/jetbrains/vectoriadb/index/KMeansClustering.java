package jetbrains.vectoriadb.index;

import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

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

        try {
            var vectorCountForCentroid = new int[numClusters];
            // use them to parallelize centroids calculation
            var centroidsPerCore = FloatVectorSegment.makeSegments(pBuddy.numWorkers(), numClusters, vectorDimensions);
            var vectorCountForCentroidPerCore = new int[pBuddy.numWorkers()][];
            for (int i = 0; i < pBuddy.numWorkers(); i++) {
                vectorCountForCentroidPerCore[i] = new int[numClusters];
            }

            if (numClusters == 1) {
                progressTracker.pushPhase("Iteration " + 1);
                try {
                    calculateCentroids(centroidsPerCore, vectorCountForCentroidPerCore, vectorCountForCentroid, progressTracker);
                } finally {
                    progressTracker.pullPhase();
                }
            } else {
                var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
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
            final int[][] vectorCountForCentroidPerCore,
            final int[] vectorCountForCentroid,
            @NotNull final ProgressTracker progressTracker
    ) {
        pBuddy.runSplitEvenly(
                "centroids calculation",
                numVectors,
                progressTracker,
                // init worker
                (workerIdx) -> {
                    centroidsPerCore[workerIdx].fill(0);
                    Arrays.fill(vectorCountForCentroidPerCore[workerIdx], 0);
                },
                // process an item
                (workerIdx, vectorIdx) -> {
                    var vector = vectorReader.read(vectorIdx);
                    var centroidIdx = centroidIdxByVectorIdx.get(vectorIdx);
                    centroidsPerCore[workerIdx].add(centroidIdx, vector, 0);
                    vectorCountForCentroidPerCore[workerIdx][centroidIdx] += 1;
                }
        );

        centroids.fill(0);
        Arrays.fill(vectorCountForCentroid, 0);
        for (int workerId = 0; workerId < pBuddy.numWorkers(); workerId++) {
            for (int centroidIdx = 0; centroidIdx < numClusters; centroidIdx++) {
                centroids.add(centroidIdx, centroidsPerCore[workerId], centroidIdx);
                vectorCountForCentroid[centroidIdx] += vectorCountForCentroidPerCore[workerId][centroidIdx];
            }
        }

        for (int centroidIdx = 0; centroidIdx < numClusters; centroidIdx++) {
            var vectorCount = vectorCountForCentroid[centroidIdx];
            if (vectorCount > 0) {
                centroids.div(centroidIdx, vectorCount);
            }
        }
    }

    private int findClosestCluster(int vectorIdx) {
        if (vectorDimensions == 1) {
            return findClosestCluster1Dimension(vectorIdx);
        }
        int minIndex = -1;
        float minDistance = Float.MAX_VALUE;
        // todo slicing here causes a lot of heap allocations
        var vector = vectorReader.read(vectorIdx);

        for (int i = 0; i < numClusters; i++) {
            var distance = distanceFun.computeDistance(vector, 0, centroids.getInternalArray(), centroids.offset(i), vectorDimensions);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }

        return minIndex;
    }

    private int findClosestCluster1Dimension(int vectorIdx) {
        int minIndex = -1;
        float minDistance = Float.MAX_VALUE;

        var vector = vectorReader.read(vectorIdx, 0);

        for (int i = 0; i < numClusters; i++) {
            var centroid = centroids.get(i, 0);

            var distance = distanceFun.computeDistance(vector, centroid);
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return minIndex;
    }
}
