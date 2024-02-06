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
package jetbrains.vectoriadb.index

import it.unimi.dsi.fastutil.ints.IntArrayList
import kotlin.math.max

/**
 * Simplified or medoid-based way to calculate Silhouette Coefficient.
 * It does the job in:
 * - O(vectors.count) if you have already clusterized the vectors
 * - O(vectors.count * centroids.count) if you have not clusterized the vectors
 * */
@Suppress("unused") // it will be used in VectorDatasetContext.silhouetteCoefficient(...)
class SilhouetteCoefficientMedoid(
    val centroids: Array<FloatArray>,
    val vectors: Array<FloatArray>,
    val computeDistance: (FloatArray, FloatArray) -> Float
) {
    private val clusterByVectorIdx: IntArray
    private val closestClusterByVectorIdx: IntArray

    init {
        clusterByVectorIdx = IntArray(vectors.size)
        closestClusterByVectorIdx = IntArray(vectors.size)
        for (i in vectors.indices) {
            val vector = vectors[i]
            val clusters = findClosestAndSecondClosestCluster(centroids, vector, computeDistance)
            clusterByVectorIdx[i] = clusters[0]
            closestClusterByVectorIdx[i] = clusters[1]
        }
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
            val a = computeDistance(v, centroid)
            val b = computeDistance(v, closestCentroid)
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
class SilhouetteCoefficient(
    centroids: Array<FloatArray>,
    val vectors: Array<FloatArray>,
    val computeDistance: (FloatArray, FloatArray) -> Float
) {
    val clusterByVectorIdx: IntArray
    val closestClusterByVectorIdx: IntArray
    val vectorsByClusterIdx: List<IntArrayList>

    init {
        clusterByVectorIdx = IntArray(vectors.size)
        closestClusterByVectorIdx = IntArray(vectors.size)
        vectorsByClusterIdx = MutableList(centroids.size) { IntArrayList() }
        for (i in vectors.indices) {
            val vector = vectors[i]
            val clusters = findClosestAndSecondClosestCluster(centroids, vector, computeDistance)
            clusterByVectorIdx[i] = clusters[0]
            closestClusterByVectorIdx[i] = clusters[1]
            vectorsByClusterIdx[clusters[0]].add(i)
        }
    }

    fun calculate(): Float {
        /**
         * todo use ParallelBuddy to get things done faster
         * */
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
            sum += computeDistance(v, w)
            count++
        }
        return if (count == 0) {
            0f
        } else {
            sum / count
        }
    }
}

private fun findClosestAndSecondClosestCluster(
    centroids: Array<FloatArray>,
    vector: FloatArray,
    computeDistance: (FloatArray, FloatArray) -> Float
): IntArray {
    var closestClusterIndex = -1
    var secondClosestClusterIndex = -1
    var closestDistance = Float.MAX_VALUE
    var secondClosestDistance = Float.MAX_VALUE
    for (i in centroids.indices) {
        val centroid = centroids[i]
        val distance = computeDistance(centroid, vector)
        if (distance < closestDistance) {
            secondClosestClusterIndex = closestClusterIndex
            secondClosestDistance = closestDistance
            closestClusterIndex = i
            closestDistance = distance
        } else if (distance < secondClosestDistance) {
            secondClosestClusterIndex = i
            secondClosestDistance = distance
        }
    }
    assert(closestClusterIndex != secondClosestClusterIndex)
    return intArrayOf(closestClusterIndex, secondClosestClusterIndex)
}

fun VectorDatasetContext.silhouetteCoefficient(distanceFun: DistanceFunction, centroids: Array<FloatArray>, vectors: Array<FloatArray>): Float {
    /**
     * todo Silhouette Coefficient or Silhouette Coefficient Medoid
     * It makes sense to choose either "proper" Silhouette Coefficient or Silhouette Coefficient Medoid depending
     * on the dataset size. The "proper" Silhouette Coefficient O(numVectors^2), so for big datasets it will work
     * too long. The Silhouette Coefficient Medoid on the other side is O(numVectors * numClusters) that is much
     * better for big datasets.
     */
    val coef = when (distanceFun) {
        is L2DistanceFunction,
        is L2DistanceFunctionNew -> distanceFun.l2SilhouetteCoefficient(centroids, vectors)
        is DotDistanceFunction,
        is DotDistanceFunctionNew -> distanceFun.ipSilhouetteCoefficient(centroids, vectors, maxInnerProduct)
        else -> throw NotImplementedError()
    }
    return coef.calculate()
}

// it should be used in VectorDatasetContext.silhouetteCoefficient(...)
@Suppress("unused")
fun l2SilhouetteCoefficientMedoid(centroids: Array<FloatArray>, vectors: Array<FloatArray>): SilhouetteCoefficientMedoid {
    val distanceFun = L2DistanceFunction.INSTANCE
    return SilhouetteCoefficientMedoid(centroids, vectors) { v1, v2 ->
        distanceFun.computeDistance(v1, 0, v2, 0, v1.size)
    }
}


fun DistanceFunction.l2SilhouetteCoefficient(centroids: Array<FloatArray>, vectors: Array<FloatArray>): SilhouetteCoefficient {
    return SilhouetteCoefficient(centroids, vectors) { v1, v2 ->
        computeDistance(v1, 0, v2, 0, v1.size)
    }
}

private fun DistanceFunction.ipSilhouetteCoefficient(centroids: Array<FloatArray>, vectors: Array<FloatArray>, maxInnerProduct: Float): SilhouetteCoefficient {
    /*
    * DotDistanceFunction returns the -1 * (inner product). It is convenient when we compare distances -
    * the smaller the distance, more similar the vectors.
    *
    * But the Silhouette Coefficient does not work with negative values, so we have to fix our distances somehow.
    * We find the max overall inner product = -1 * (min overall distance) and add it to all the distances.
    * So we make all the distances positive keeping "the smaller the distance, more similar vectors" property.
    * */
    val dimensions = vectors[0].size
    return SilhouetteCoefficient(centroids, vectors) { v1, v2 ->
        val distance = computeDistance(v1, 0, v2, 0, dimensions)
        distance + maxInnerProduct
    }
}