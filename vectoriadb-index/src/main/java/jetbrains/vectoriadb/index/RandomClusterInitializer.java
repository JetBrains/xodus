package jetbrains.vectoriadb.index;

import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class RandomClusterInitializer implements ClusterInitializer {
    @Override
    public void initializeCentroids(MemorySegment pqVectors, long numVectors, float[] distanceTable, int quantizersCount, int codeBaseSize, byte[] pqCentroids, int numClusters, @NotNull ProgressTracker progressTracker) {
        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        for (int i = 0; i < numClusters; i++) {
            var vecIndex = rng.nextLong(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, vecIndex * quantizersCount, pqCentroids, i * quantizersCount, quantizersCount);
        }
    }
}