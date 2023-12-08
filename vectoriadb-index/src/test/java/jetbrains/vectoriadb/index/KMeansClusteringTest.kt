package jetbrains.vectoriadb.index

import org.junit.Test
import kotlin.random.Random

class KMeansClusteringTest {

    @Test
    fun kMeansClustering() = parallelVectorTest(VectorDataset.Sift10K) {
        val distance = L2DistanceFunction.INSTANCE

        val maxIteration = 50
        val numClusters = 33
        val centroids = FloatVectorSegment.makeNativeSegment(arena, numClusters, dimensions)
        val centroidIdxByVectorIdx = ByteCodeSegment.makeArraySegment(numVectors)

        val kmeans = KMeansClustering(
            distance,
            vectorReader,
            centroids,
            centroidIdxByVectorIdx,
            maxIteration,
            pBuddy
        )

        kmeans.calculateCentroids(progressTracker)

        val randomCentroids = Array(numClusters) { FloatArray(dimensions) }
        repeat(numClusters) { centroidIdx ->
            val vectorIdx = Random.nextInt(numVectors)
            System.arraycopy(vectors[vectorIdx], 0, randomCentroids[centroidIdx], 0, dimensions)
        }

        val coef = SilhouetteCoefficient(centroids.toArray(), vectors, distance).calculate()
        val randomCoef = SilhouetteCoefficient(randomCentroids, vectors, distance).calculate()

        println("coef: $coef, randomCoef: $randomCoef")
        assert(coef > randomCoef)
    }
}