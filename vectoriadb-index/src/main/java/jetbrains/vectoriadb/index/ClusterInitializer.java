package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;

interface ClusterInitializer {
    void initializeCentroids(
            MemorySegment pqVectors,
            long numVectors,
            float[] distanceTable,
            int quantizersCount,
            int codeBaseSize,
            byte[] pqCentroids,
            int numClusters,
            @NotNull ProgressTracker progressTracker
    );
}