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

import jetbrains.vectoriadb.index.bench.VectorDatasetInfo
import jetbrains.vectoriadb.index.bench.downloadDatasetArchives
import jetbrains.vectoriadb.index.bench.readGroundTruth
import jetbrains.vectoriadb.index.bench.readVectors
import org.junit.Assert
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
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

internal fun randomFloatVectorSegment(count: Int, dimensions: Int): FloatVectorSegment {
    val v1 = FloatVectorSegment.makeSegment(count, dimensions)
    v1.fillRandom()
    return v1
}

internal fun randomFloatArray(count: Int): FloatArray {
    return FloatArray(count) { Random.nextFloat() }
}

internal fun randomFloatArray(count: Int, offset: Int): FloatArray {
    val result = FloatArray(count + offset * 2)
    for (i in 0 until count) {
        result[i + offset] = Random.nextFloat()
    }
    return result
}

internal fun FloatArray.assertEquals(another: FloatArray) {
    Assert.assertArrayEquals(this, another, 1e-5f)
}

internal fun FloatArray.copyToNativeSegment(arena: Arena): MemorySegment {
    val res = arena.allocateFloat(this.size)
    MemorySegment.copy(this, 0, res, ValueLayout.JAVA_FLOAT, 0, this.size)
    return res
}

internal fun Arena.allocateFloat(count: Int): MemorySegment {
    return allocate(count.toLong() * Float.SIZE_BYTES, ValueLayout.JAVA_FLOAT.byteAlignment())
}

internal fun MemorySegment.toFloatArray(): FloatArray = this.toArray(ValueLayout.JAVA_FLOAT)

internal fun createRandomFloatArray2d(count: Int, dimensions: Int): Array<FloatArray> {
    return Array(count) {
        FloatArray(dimensions) {
            Random.nextFloat()
        }
    }
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