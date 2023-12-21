package jetbrains.vectoriadb.index

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import kotlin.random.Random

internal fun createRandomFloatVectorSegment(count: Int, dimensions: Int): FloatVectorSegment {
    val v1 = FloatVectorSegment.makeArraySegment(count, dimensions)
    v1.fillRandom()
    return v1
}

internal fun Arena.createRandomFloatVectorSegment(count: Int, dimensions: Int, heapBased: Boolean = false): FloatVectorSegment {
    val v1 = if (heapBased) {
        FloatVectorSegment.makeArraySegment(count, dimensions)
    } else {
        FloatVectorSegment.makeNativeSegment(this, count, dimensions)
    }
    v1.fillRandom()
    return v1
}

private fun FloatVectorSegment.fillRandom() {
    repeat(this.count()) { vectorIdx ->
        repeat(this.dimensions()) { dimensionIdx ->
            this.set(vectorIdx, dimensionIdx, Random.nextDouble(1000.0).toFloat())
        }
    }
}

internal class FloatArrayToByteArrayVectorReader: VectorReader {

    private val vectors: Array<FloatArray>
    private val size: Int

    constructor(vectors: Array<FloatArray>) {
        this.vectors = vectors
        size = vectors.size
    }

    constructor(vectors: Array<FloatArray>, size: Int) {
        this.vectors = vectors
        this.size = size
    }

    override fun size(): Int {
        return size
    }

    override fun dimensions(): Int {
        return vectors[0].size
    }

    override fun read(index: Int): MemorySegment {
        val vectorSegment = MemorySegment.ofArray(
            ByteArray(vectors[index].size * java.lang.Float.BYTES)
        )

        MemorySegment.copy(
            MemorySegment.ofArray(vectors[index]), 0,
            vectorSegment, 0, vectors[index].size.toLong() * java.lang.Float.BYTES
        )

        return vectorSegment
    }

    override fun id(index: Int): MemorySegment? {
        return null
    }

    override fun close() {
    }
}

internal fun findTwoClosestCentroids(
    vector: FloatArray,
    centroids: Array<FloatArray>,
    distanceFun: DistanceFunction
): Pair<Int, Int> {
    var minIndex1 = -1
    var minIndex2 = -1
    var minDistance1 = Float.MAX_VALUE
    var minDistance2 = Float.MAX_VALUE

    val numClusters = centroids.count()
    val dimensions = vector.size

    for (i in 0 until numClusters) {
        val centroid = centroids[i]

        val distance = distanceFun.computeDistance(vector, 0, centroid, 0, dimensions)
        if (distance < minDistance1) {
            minDistance2 = minDistance1
            minDistance1 = distance

            minIndex2 = minIndex1
            minIndex1 = i
        } else if (distance < minDistance2) {
            minDistance2 = distance
            minIndex2 = i
        }
    }

    return Pair(minIndex1, minIndex2)
}

internal fun makeRandomCentroids(vectors: Array<FloatArray>, numClusters: Int): Array<FloatArray> {
    val numVectors = vectors.size
    val dimensions = vectors[0].size
    val randomCentroids = Array(numClusters) { FloatArray(dimensions) }
    repeat(numClusters) { centroidIdx ->
        val vectorIdx = Random.nextInt(numVectors)
        System.arraycopy(vectors[vectorIdx], 0, randomCentroids[centroidIdx], 0, dimensions)
    }
    return randomCentroids
}