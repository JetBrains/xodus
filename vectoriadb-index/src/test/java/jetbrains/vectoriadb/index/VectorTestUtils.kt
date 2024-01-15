package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.bench.VectorDatasetInfo
import jetbrains.vectoriadb.index.bench.downloadDatasetArchives
import jetbrains.vectoriadb.index.bench.readGroundTruth
import jetbrains.vectoriadb.index.bench.readVectors
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.file.Files
import java.nio.file.Path
import kotlin.random.Random

fun requireBuildPath(): Path {
    val buildDirStr = System.getProperty("exodus.tests.buildDirectory")
        ?: throw RuntimeException("exodus.tests.buildDirectory is not set")
    return Path.of(buildDirStr)
}

fun VectorDatasetInfo.requireDatasetPath(): Path = requireBuildPath().resolve(name)

private fun VectorDatasetInfo.makeSureDatasetIsDownloadedAndExtracted(targetDir: Path) {
    Files.createDirectories(targetDir)
    downloadDatasetArchives(targetDir).forEach { archive ->
        archive.extractTo(targetDir)
    }
}

fun VectorDatasetInfo.readBaseVectors(): Array<FloatArray> {
    val targetDir = requireDatasetPath()
    makeSureDatasetIsDownloadedAndExtracted(targetDir)

    val baseFilePath = targetDir.resolve(baseFile)
    return readVectors(baseFilePath, vectorDimensions, vectorCount)
}

fun VectorDatasetInfo.readQueryVectors(): Array<FloatArray> {
    val targetDir = requireDatasetPath()
    makeSureDatasetIsDownloadedAndExtracted(targetDir)

    val queryFilePath = targetDir.resolve(queryFile)
    return readVectors(queryFilePath, vectorDimensions)
}

fun VectorDatasetInfo.readGroundTruthL2(): Array<IntArray> {
    val targetDir = requireDatasetPath()
    makeSureDatasetIsDownloadedAndExtracted(targetDir)

    val groundTruthFilePath = targetDir.resolve(l2GroundTruthFile)
    return readGroundTruth(groundTruthFilePath)
}

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

internal class FloatArrayVectorReader: VectorReader {

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
        return MemorySegment.ofArray(vectors[index])
    }

    override fun read(vectorIdx: Int, dimension: Int): Float {
        return vectors[vectorIdx][dimension]
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