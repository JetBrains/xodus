package jetbrains.vectoriadb.index;

import org.apache.commons.rng.simple.RandomSource;
import org.junit.Ignore;
import org.junit.Test;

import static jetbrains.vectoriadb.index.LoadVectorsUtil.loadGist1MVectors;
import static jetbrains.vectoriadb.index.SilhouetteCoefficientKt.l2SilhouetteCoefficientMedoid;

public class ClusterInitializerTest {
    @Test
    @Ignore
    public void clusterInitializersBenchmark() throws Exception {
        var vectors = loadGist1MVectors();//loadSift1MVectors();
        var clustersCount = 100;

        var progressTracker = new ConsolePeriodicProgressTracker(1);
        progressTracker.start("Index");
        try (var pqQuantizer = new L2PQQuantizer()) {
            System.out.println("Generating PQ codes...");
            pqQuantizer.generatePQCodes(32, new FloatArrayToByteArrayVectorReader(vectors), progressTracker);

            var count = 0;
            var meanPpRandomDiff = 0f;
            var meanMaxDistanceRandomDiff = 0f;
            var meanSilhCoeffRandom = 0f;
            var meanSilhCoeffPP = 0f;
            var meanSilhCoeffMaxDistance = 0f;
            for (var i = 0; i < 1; i += 1) {
                var silhCoeffRandom = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new RandomClusterInitializer(), clustersCount, vectors, progressTracker);
                var silhCoeffPP = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new KMeansPlusPlusClusterInitializer(RandomSource.PCG_XSH_RS_32, 2), clustersCount, vectors, progressTracker);
                var silhCoeffMaxDistance = doPQKMeansAndGetSilhouetteCoefficient(pqQuantizer, new MaxDistanceClusterInitializer(), clustersCount, vectors, progressTracker);

                System.out.printf("silhouetteCoefficient random = %f%n", silhCoeffRandom);
                System.out.printf("silhouetteCoefficient k-means++ = %f%n", silhCoeffPP);
                System.out.printf("silhouetteCoefficient max distance = %f%n", silhCoeffMaxDistance);

                meanPpRandomDiff += silhCoeffPP - silhCoeffRandom;
                meanMaxDistanceRandomDiff += silhCoeffMaxDistance - silhCoeffRandom;
                meanSilhCoeffRandom += silhCoeffRandom;
                meanSilhCoeffPP += silhCoeffPP;
                meanSilhCoeffMaxDistance += silhCoeffMaxDistance;
                count++;
            }
            meanPpRandomDiff = meanPpRandomDiff / count;
            meanMaxDistanceRandomDiff = meanMaxDistanceRandomDiff / count;
            meanSilhCoeffRandom = meanSilhCoeffRandom / count;
            meanSilhCoeffPP = meanSilhCoeffPP / count;
            meanSilhCoeffMaxDistance = meanSilhCoeffMaxDistance / count;
            System.out.printf("k-means++ - random: %f, max distance - random: %f, meanSilhCoeffRandom: %f, meanSilhCoeffPP: %f, meanSilhCoeffMaxDistance: %f", meanPpRandomDiff, meanMaxDistanceRandomDiff, meanSilhCoeffRandom, meanSilhCoeffPP, meanSilhCoeffMaxDistance);
        }
    }

    private static Float doPQKMeansAndGetSilhouetteCoefficient(L2PQQuantizer pqQuantizer, ClusterInitializer clusterInitializer, int clustersCount, float[][] vectors, ProgressTracker progressTracker) {
        System.out.print("PQ codes generated. Calculating centroids, initialization...\n");
        // todo fix
        //pqQuantizer.setClusterInitializer(clusterInitializer);
        var centroids = pqQuantizer.calculateCentroids(clustersCount, 50, L2DistanceFunction.INSTANCE, progressTracker);

        System.out.println("Centroids calculated. Calculating silhouette coefficient...");
        return l2SilhouetteCoefficientMedoid(centroids, vectors).calculate();
    }
}
