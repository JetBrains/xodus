package jetbrains.vectoriadb.index

import org.junit.Test

class KMeansClusteringTest {

    @Test
    fun kMeansClustering() = parallelVectorTest(VectorDataset.Sift10K) {
        val distance = L2DistanceFunctionNew()

        val maxIteration = 50
        val numClusters = 33
        val centroids = FloatVectorSegment.makeSegment(numClusters, dimensions)
        val centroidIdxByVectorIdx = ByteCodeSegment.makeArraySegment(numVectors)

        val kmeans = KMeansClustering(
            "test clustering",
            distance,
            vectorReader,
            centroids,
            centroidIdxByVectorIdx,
            maxIteration,
            pBuddy
        )

        kmeans.calculateCentroids(progressTracker)

        val randomCentroids = makeRandomCentroids(vectors, numClusters)

        val coef = silhouetteCoefficient(distance, centroids.toArray(), vectors)
        val randomCoef = silhouetteCoefficient(distance, randomCentroids, vectors)

        println("coef: $coef, randomCoef: $randomCoef")
        assert(coef > randomCoef)
    }
}