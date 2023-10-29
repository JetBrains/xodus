package jetbrains.vectoriadb.index

import it.unimi.dsi.fastutil.ints.IntArrayList
import kotlin.math.max

/**
 * Simplified or medoid-based way to calculate Silhouette Coefficient.
 * It does the job in:
 * - O(vectors.count) if you have already clusterized the vectors
 * - O(vectors.count * centroids.count) if you have not clusterized the vectors
 * */
class SilhouetteCoefficientMedoid {
    val centroids: Array<FloatArray>
    val vectors: Array<FloatArray>
    val distanceFunction: DistanceFunction
    private val clusterByVectorIdx: IntArray
    private val closestClusterByVectorIdx: IntArray

    constructor(
        centroids: Array<FloatArray>,
        vectors: Array<FloatArray>,
        distanceFunction: DistanceFunction,
    ) {
        this.centroids = centroids
        this.vectors = vectors
        this.distanceFunction = distanceFunction

        clusterByVectorIdx = IntArray(vectors.size)
        closestClusterByVectorIdx = IntArray(vectors.size)
        for (i in vectors.indices) {
            val vector = vectors[i]
            val clusters = AbstractVectorsTest.findClosestAndSecondClosestCluster(centroids, vector, distanceFunction)
            clusterByVectorIdx[i] = clusters[0]
            closestClusterByVectorIdx[i] = clusters[1]
        }
    }

    constructor(
        centroids: Array<FloatArray>,
        vectors: Array<FloatArray>,
        clusterByVectorIdx: IntArray,
        closestClusterByVectorIdx: IntArray,
        distanceFunction: DistanceFunction,
    ) {
        this.centroids = centroids
        this.vectors = vectors
        this.distanceFunction = distanceFunction
        this.clusterByVectorIdx = clusterByVectorIdx
        this.closestClusterByVectorIdx = closestClusterByVectorIdx
    }

    fun calculate(): Float {
        var sum = 0f
        for (vectorIdx in vectors.indices) {
            val centroidIdx = clusterByVectorIdx[vectorIdx]
            val closestCentroidIdx = closestClusterByVectorIdx[vectorIdx]
            assert(closestCentroidIdx != centroidIdx)

            val v = vectors[vectorIdx]
            val centroid = centroids[centroidIdx]
            val closestCentroid = centroids[closestCentroidIdx]
            val a = distanceFunction.computeDistance(v, 0, centroid, 0, v.size)
            val b = distanceFunction.computeDistance(v, 0, closestCentroid, 0, v.size)
            val coef = (b - a) / max(a, b)
            sum += coef
        }
        return sum / vectors.size
    }
}

/**
 * The basic/traditional/standard way to calculate Silhouette Coefficient.
 * Does the job in O(vectors.count^2).
 * */
class SilhouetteCoefficient {
    val centroids: Array<FloatArray>
    val vectors: Array<FloatArray>
    val clusterByVectorIdx: IntArray
    val closestClusterByVectorIdx: IntArray
    val vectorsByClusterIdx: List<IntArrayList>
    val distanceFunction: DistanceFunction

    constructor(
        centroids: Array<FloatArray>,
        vectors: Array<FloatArray>,
        clusterByVectorIdx: IntArray,
        closestClusterByVectorIdx: IntArray,
        vectorsByClusterIdx: List<IntArrayList>,
        distanceFunction: DistanceFunction
    ) {
        this.centroids = centroids
        this.vectors = vectors
        this.clusterByVectorIdx = clusterByVectorIdx
        this.closestClusterByVectorIdx = closestClusterByVectorIdx
        this.vectorsByClusterIdx = vectorsByClusterIdx
        this.distanceFunction = distanceFunction
    }

    constructor(
        centroids: Array<FloatArray>,
        vectors: Array<FloatArray>,
        distanceFunction: DistanceFunction
    ) {
        this.centroids = centroids
        this.vectors = vectors
        this.distanceFunction = distanceFunction

        clusterByVectorIdx = IntArray(vectors.size)
        closestClusterByVectorIdx = IntArray(vectors.size)
        vectorsByClusterIdx = MutableList(centroids.size) { IntArrayList() }
        for (i in vectors.indices) {
            val vector = vectors[i]
            val clusters = AbstractVectorsTest.findClosestAndSecondClosestCluster(centroids, vector, L2DistanceFunction.INSTANCE)
            clusterByVectorIdx[i] = clusters[0]
            closestClusterByVectorIdx[i] = clusters[1]
            vectorsByClusterIdx[clusters[0]].add(i)
        }
    }

    fun calculate(): Float {
        var sum = 0f
        for (vectorIdx in vectors.indices) {
            val clusterIdx = clusterByVectorIdx[vectorIdx]
            val closestClusterIdx = closestClusterByVectorIdx[vectorIdx]
            assert(closestClusterIdx != clusterIdx)

            val a = avgDistance(vectorIdx, vectorsByClusterIdx[clusterIdx])
            val b = avgDistance(vectorIdx, vectorsByClusterIdx[closestClusterIdx])
            val coef = if (a == 0f && b == 0f) 0f else (b - a) / max(a, b)
            sum += coef
        }
        return sum / vectors.size
    }

    private fun avgDistance(fromVectorIndex: Int, toVectors: IntArrayList): Float {
        var sum = 0.0f
        var count = 0
        val v = vectors[fromVectorIndex]
        for (i in toVectors.indices) {
            val wi = toVectors.getInt(i)
            if (fromVectorIndex == wi) continue

            val w = vectors[wi]
            sum += distanceFunction.computeDistance(v, 0, w, 0, v.size)
            count++
        }
        return if (count == 0) {
            0f
        } else {
            sum / count
        }
    }
}