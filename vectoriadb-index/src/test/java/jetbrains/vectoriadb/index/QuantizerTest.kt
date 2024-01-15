package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.io.*
import kotlin.math.abs
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

class L2QuantizerTest {
    @Test
    fun `get vector approximation`() = getVectorApproximationTest(VectorDataset.Sift10K, 1e-5) { L2PQQuantizer() }

    @Test
    fun `lookup table`() = lookupTableTest(VectorDataset.Sift10K, L2DistanceFunction.INSTANCE, 1f) { L2PQQuantizer() }

    @Test
    fun `split vectors by partitions`() = splitVectorsByPartitions(
        VectorDataset.Sift10K,
        L2DistanceFunction.INSTANCE,
        // random clustering gives a better quality once in a while, so we do not check the clustering quality here
        checkClusteringQuality = false,
        expectedClosestVectorsShare = 0.65
    ) { L2PQQuantizer() }

    @Test
    fun `split vectors by a single partition`() = splitVectorsBySinglePartition(VectorDataset.Sift10K, L2DistanceFunction.INSTANCE) { L2PQQuantizer() }

    @Test
    fun `calculate centroids`() = calculateCentroids(VectorDataset.Sift10K, L2DistanceFunction.INSTANCE, L2PQQuantizer())

    @Test
    fun `calculate a single centroid, quantizer does calculate the centroid`() = vectorTest(VectorDataset.Sift10K) {
        val numClusters = 1
        val maxIterations = 50
        val compressionRatio = 32
        val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
        val distanceFun = L2DistanceFunction.INSTANCE

        val quantizer = L2PQQuantizer()
        quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

        val centroid = quantizer.calculateCentroids(vectorReader, numClusters, maxIterations, distanceFun, progressTracker)[0]

        for (i in 0 until numVectors) {
            val vectorApproximation = quantizer.getVectorApproximation(i)
            var diff = 0f
            for (d in 0 until dimensions) {
                diff += abs(centroid[d] - vectorApproximation[d])
            }
            // if diff = 0, it means quantizer has not calculated anything and just returned a random vector
            Assert.assertNotEquals(0f, diff)
        }
    }

    @Test
    fun `store, load`() = storeLoad(VectorDataset.Sift10K, 1e-5) { L2PQQuantizer() }
}

class NormExplicitQuantizerTest {

    @Test
    fun `get vector approximation`() = getVectorApproximationTest(VectorDataset.Sift10K, 1e-2) { NormExplicitQuantizer(1) }

    @Test
    fun `lookup table`() = lookupTableTest(VectorDataset.Sift10K, DotDistanceFunction.INSTANCE, 1f) { NormExplicitQuantizer(1) }

    @Test
    fun `split vectors by partitions`() = splitVectorsByPartitions(VectorDataset.Sift10K, DotDistanceFunction.INSTANCE, checkClusteringQuality = true, expectedClosestVectorsShare = 1.0) { NormExplicitQuantizer(1) }

    @Test
    fun `split vectors by a single partition`() = splitVectorsBySinglePartition(VectorDataset.Sift10K, L2DistanceFunction.INSTANCE) { NormExplicitQuantizer(1) }

    @Test
    fun `calculate centroids`() = calculateCentroids(VectorDataset.Sift10K, DotDistanceFunction.INSTANCE, NormExplicitQuantizer())

    @Test
    fun `calculate a single centroid`() = vectorTest(VectorDataset.Sift10K) {
        val numClusters = 1
        val maxIterations = 50
        val distanceFun = DotDistanceFunction.INSTANCE

        val quantizer = NormExplicitQuantizer()

        val centroid = quantizer.calculateCentroids(vectorReader, numClusters, maxIterations, distanceFun, progressTracker)[0]

        val expectedCentroid = FloatArray(dimensions)
        for (vector in vectors) {
            for (i in 0 until dimensions) {
                expectedCentroid[i] += vector[i]
            }
        }
        for (i in 0 until dimensions) {
            expectedCentroid[i] = expectedCentroid[i] / numVectors
            Assert.assertEquals(expectedCentroid[i], centroid[i], 1e-3f)
        }
    }

    @Test
    fun `store, load`() = storeLoad(VectorDataset.Sift10K, 1e-5) { NormExplicitQuantizer() }

    @Test
    fun `average norm error for norm-explicit quantization should be less that for l2 quantization`() = vectorTest(VectorDataset.Sift10K) {
        val compressionRatio = 32
        val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
        val l2Distance = L2DistanceFunction.INSTANCE

        val ipQuantizer1 = NormExplicitQuantizer(1)
        ipQuantizer1.generatePQCodes(vectorReader, codebookCount, progressTracker)

        val ipQuantizer2 = NormExplicitQuantizer(2)
        ipQuantizer2.generatePQCodes(vectorReader, codebookCount, progressTracker)

        val l2Quantizer = L2PQQuantizer()
        l2Quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

        var ip1TotalError = 0.0
        var ip2TotalError = 0.0
        var l2TotalError = 0.0
        var ip1TotalNormError = 0.0
        var ip2TotalNormError = 0.0
        var l2TotalNormError = 0.0
        var totalNorm = 0.0
        repeat(numVectors) { vectorIdx ->
            val vector = vectors[vectorIdx]
            val ip1Vector = ipQuantizer1.getVectorApproximation(vectorIdx)
            val ip2Vector = ipQuantizer2.getVectorApproximation(vectorIdx)
            val l2Vector = l2Quantizer.getVectorApproximation(vectorIdx)

            ip1TotalError += l2Distance.computeDistance(vector, 0, ip1Vector, 0, dimensions)
            ip2TotalError += l2Distance.computeDistance(vector, 0, ip2Vector, 0, dimensions)
            l2TotalError += l2Distance.computeDistance(vector, 0, l2Vector, 0, dimensions)

            val vectorNorm = VectorOperations.calculateL2Norm(vector)
            val ip1VectorNorm = VectorOperations.calculateL2Norm(ip1Vector)
            val ip2VectorNorm = VectorOperations.calculateL2Norm(ip2Vector)
            val l2VectorNorm = VectorOperations.calculateL2Norm(l2Vector)
            totalNorm += vectorNorm
            ip1TotalNormError += abs(vectorNorm - ip1VectorNorm)
            ip2TotalNormError += abs(vectorNorm - ip2VectorNorm)
            l2TotalNormError += abs(vectorNorm - l2VectorNorm)
        }
        val averageNorm = totalNorm / numVectors
        val l2AverageNormError = l2TotalNormError / numVectors
        val ip1AverageNormError = ip1TotalNormError / numVectors
        val ip2AverageNormError = ip2TotalNormError / numVectors
        println("""
            Codebook count:      $codebookCount
            Average vector norm: $averageNorm
         
            Average approximation error (L2 distance)
            l2 quantization:                               ${l2TotalError / numVectors}
            norm-explicit quantization (1 norm codebook):  ${ip1TotalError / numVectors}
            norm-explicit quantization (2 norm codebooks): ${ip2TotalError / numVectors}
            
            Average approximation norm error
            l2 quantization:                               $l2AverageNormError ${(l2AverageNormError / averageNorm).formatPercent(3)} 
            norm-explicit quantization (1 norm codebook):  $ip1AverageNormError ${(ip1AverageNormError / averageNorm).formatPercent(3)}
            norm-explicit quantization (2 norm codebooks): $ip2AverageNormError ${(ip2AverageNormError / averageNorm).formatPercent(3)}
        """.trimIndent())

        assert(ip2TotalNormError < ip1TotalNormError)
        assert(ip1TotalNormError < l2TotalNormError)
    }

    /*
    * It is not a test, it is a util method. Helps to calculate the max inner product for a dataset.
    * The max inner product is used to calculate the Silhouette Coefficient when using DotDistanceFunction.
    * */
    @Ignore
    @Test
    fun findMaxInnerProduct() = parallelVectorTest(VectorDataset.Sift1M) {
        val distanceFun = DotDistanceFunction.INSTANCE

        val minDistancePerWorker = FloatArray(pBuddy.numWorkers()) { Float.MAX_VALUE }
        pBuddy.runSplitEvenly(
            "Calculate max inner product",
            numVectors,
            progressTracker
        ) { workerIdx, vectorIdx ->
            for (j in vectorIdx until vectors.size) {
                val distance = distanceFun.computeDistance(vectors[vectorIdx], 0, vectors[j], 0, dimensions)
                if (distance < minDistancePerWorker[workerIdx]) {
                    minDistancePerWorker[workerIdx] = distance
                }
            }
        }
        val minDistance = minDistancePerWorker.min()
        val maxInnerProduct = -1 * minDistance
        println("maxInnerProduct: $maxInnerProduct")
    }

    fun Double.formatPercent(digits: Int = 2): String = "%.${digits}f".format(this * 100) + "%"
}

internal fun storeLoad(dataset: VectorDataset, delta: Double, buildQuantizer: () -> Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)

    val quantizer1 = buildQuantizer()
    quantizer1.generatePQCodes(vectorReader, codebookCount, progressTracker)

    val approximationError1 = quantizer1.calculateApproximationError(vectors, numVectors)

    val data = ByteArrayOutputStream().use { outputStream ->
        DataOutputStream(outputStream).use { dataOutputStream ->
            quantizer1.store(dataOutputStream)
            dataOutputStream.flush()
        }
        outputStream.toByteArray()
    }

    val quantizer2 = buildQuantizer()
    ByteArrayInputStream(data).use { inputStream ->
        DataInputStream(inputStream).use { dataInputStream ->
            quantizer2.load(dataInputStream)
        }
    }

    val approximationError2 = quantizer2.calculateApproximationError(vectors, numVectors)

    println("approximationError1: $approximationError1, approximationError2: $approximationError2")

    Assert.assertEquals(approximationError1, approximationError2, delta)
}

internal fun calculateCentroids(dataset:VectorDataset, distanceFun: DistanceFunction, quantizer: Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
    val numClusters = 33
    val maxIterations = 50

    quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

    val centroids = quantizer.calculateCentroids(vectorReader, numClusters, maxIterations, distanceFun, progressTracker)

    val randomCentroids = makeRandomCentroids(vectors, numClusters)

    val coef = silhouetteCoefficient(distanceFun, centroids, vectors)
    val randomCoef = silhouetteCoefficient(distanceFun, randomCentroids, vectors)
    println("coef: $coef, randomCoef: $randomCoef")

    assert(coef > randomCoef)
}

internal fun splitVectorsByPartitions(dataset: VectorDataset, distanceFun: DistanceFunction, expectedClosestVectorsShare: Double, checkClusteringQuality: Boolean, quantizerBuilder: () -> Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
    val numClusters = 35
    val maxIterations = 50

    val quantizer = quantizerBuilder()
    quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

    val result = quantizer.splitVectorsByPartitions(vectorReader, numClusters, maxIterations, distanceFun, progressTracker)
    val centroids = result.partitionCentroids
    val vectorsByCentroid = result.vectorsByCentroidIdx

    assert(result.partitionCentroids.size == numClusters)
    assert(result.vectorsByCentroidIdx.size == numClusters)
    Assert.assertEquals(numVectors * 2, result.vectorsByCentroidIdx.sumOf { it.size })

    if (checkClusteringQuality) {
        val randomCentroids = makeRandomCentroids(vectors, numClusters)
        val coef = silhouetteCoefficient(distanceFun, centroids, vectors)
        val randomCoef = silhouetteCoefficient(distanceFun, randomCentroids, vectors)
        println("coef: $coef, randomCoef: $randomCoef")
        assert(coef > randomCoef)
    }

    val closestCentroidByVector1 = HashMap<Int, Int>(numVectors)
    val closestCentroidByVector2 = HashMap<Int, Int>(numVectors)

    vectors.forEachIndexed { idx, v ->
        val (closestCentroid1, closestCentroid2) = findTwoClosestCentroids(v, centroids, distanceFun)
        closestCentroidByVector1[idx] = closestCentroid1
        closestCentroidByVector2[idx] = closestCentroid2
    }

    vectorsByCentroid.forEachIndexed { centroidIdx, vectorIndices ->
        for (vectorIdx in vectorIndices.elements()) {
            if (closestCentroidByVector1[vectorIdx] == centroidIdx) {
                closestCentroidByVector1.remove(vectorIdx)
            } else if (closestCentroidByVector2[vectorIdx] == centroidIdx) {
                closestCentroidByVector2.remove(vectorIdx)
            }
        }
    }
    println("Closest1 vectors remain ${closestCentroidByVector1.size}")
    println("Closest2 vectors remain ${closestCentroidByVector2.size}")

    val actuallyClosestVectorsShare = (numVectors * 2 - (closestCentroidByVector1.size + closestCentroidByVector2.size)).toDouble() / (numVectors * 2)
    println("Actual closest vectors share $actuallyClosestVectorsShare")
    assert(actuallyClosestVectorsShare >= expectedClosestVectorsShare)
}

internal fun splitVectorsBySinglePartition(dataset: VectorDataset, distanceFun: DistanceFunction, quantizerBuilder: () -> Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
    val numClusters = 1
    val maxIterations = 50

    val quantizer = quantizerBuilder()
    quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

    val result = quantizer.splitVectorsByPartitions(vectorReader, numClusters, maxIterations, distanceFun, progressTracker)
    val centroids = result.partitionCentroids
    val vectorsByCentroid = result.vectorsByCentroidIdx

    assert(centroids.size == 1)
    assert(vectorsByCentroid.size == 1)
    assert(vectorsByCentroid[0].size == numVectors)

    val allVectorIndices = mutableSetOf<Int>()
    repeat(numVectors) { vectorIdx  ->
        allVectorIndices.add(vectorIdx)
    }

    vectorsByCentroid[0].forEach { vectorIdx ->
        allVectorIndices.remove(vectorIdx)
    }
    assert(allVectorIndices.size == 0)
}

internal fun getVectorApproximationTest(dataset: VectorDataset, delta: Double, quantizerBuilder: () -> Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)
    val codeBaseSize = CodebookInitializer.CODE_BASE_SIZE

    val averageErrors = buildList {
        listOf(codeBaseSize, codeBaseSize * 2, codeBaseSize * 4, codeBaseSize * 8).forEach { numVectors ->
            val vectorReader = FloatArrayVectorReader(vectors, numVectors)
            val quantizer = quantizerBuilder()
            quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

            add(quantizer.calculateApproximationError(vectors, numVectors))
        }
    }

    // if we quantize X vectors with X code base size, the approximated vectors should be equals (at least almost) to original vectors
    Assert.assertEquals(0.0, averageErrors[0], delta)

    // the more vectors the bigger average error we should get
    for (i in 1 until 4) {
        assert(averageErrors[i] > averageErrors[i - 1])
    }
}

private fun Quantizer.calculateApproximationError(vectors: Array<FloatArray>, numVectors: Int): Double {
    val l2Distance = L2DistanceFunction.INSTANCE
    var totalError = 0.0
    val dimensions = vectors[0].size
    repeat(numVectors) { vectorIdx ->
        val vector = vectors[vectorIdx]
        val vectorApproximation = this.getVectorApproximation(vectorIdx)

        val error = l2Distance.computeDistance(vector, 0, vectorApproximation, 0, dimensions)
        totalError += error
    }
    return totalError / numVectors
}

internal fun lookupTableTest(dataset:VectorDataset, distanceFun: DistanceFunction, delta: Float, buildQuantizer: () -> Quantizer) = vectorTest(dataset) {
    val compressionRatio = 32
    val codebookCount = CodebookInitializer.getCodebookCount(dimensions, compressionRatio)

    val quantizer = buildQuantizer()
    quantizer.generatePQCodes(vectorReader, codebookCount, progressTracker)

    var totalComputeDistanceDuration = 0.seconds
    var totalComputeDistance4BatchDuration = 0.seconds
    repeat(numVectors - 4) { vector1Idx ->
        val vector2Idx = vector1Idx + 1
        val vector3Idx = vector2Idx + 1
        val vector4Idx = vector3Idx + 1

        val q = vectors[vector1Idx]

        val vector1Approximation = quantizer.getVectorApproximation(vector1Idx)
        val vector2Approximation = quantizer.getVectorApproximation(vector2Idx)
        val vector3Approximation = quantizer.getVectorApproximation(vector3Idx)
        val vector4Approximation = quantizer.getVectorApproximation(vector4Idx)

        val distance1 = distanceFun.computeDistance(q, 0, vector1Approximation, 0, dimensions)
        val distance2 = distanceFun.computeDistance(q, 0, vector2Approximation, 0, dimensions)
        val distance3 = distanceFun.computeDistance(q, 0, vector3Approximation, 0, dimensions)
        val distance4 = distanceFun.computeDistance(q, 0, vector4Approximation, 0, dimensions)

        val lookupTable = quantizer.blankLookupTable()
        quantizer.buildLookupTable(q, lookupTable, distanceFun)

        quantizer.computeDistanceUsingLookupTable(lookupTable, vector1Idx)
        quantizer.computeDistanceUsingLookupTable(lookupTable, vector2Idx)
        quantizer.computeDistanceUsingLookupTable(lookupTable, vector3Idx)
        quantizer.computeDistanceUsingLookupTable(lookupTable, vector4Idx)

        val batchTableDistances = FloatArray(4)
        totalComputeDistance4BatchDuration += measureTime { quantizer.computeDistance4BatchUsingLookupTable(lookupTable, vector1Idx, vector2Idx, vector3Idx, vector4Idx, batchTableDistances) }

        val (tableDistance1, duration1) = measureTimedValue { quantizer.computeDistanceUsingLookupTable(lookupTable, vector1Idx) }
        val (tableDistance2, duration2) = measureTimedValue { quantizer.computeDistanceUsingLookupTable(lookupTable, vector2Idx) }
        val (tableDistance3, duration3) = measureTimedValue { quantizer.computeDistanceUsingLookupTable(lookupTable, vector3Idx) }
        val (tableDistance4, duration4) = measureTimedValue { quantizer.computeDistanceUsingLookupTable(lookupTable, vector4Idx) }
        totalComputeDistanceDuration = totalComputeDistanceDuration + duration1 + duration2 + duration3 + duration4

        Assert.assertEquals(distance1, tableDistance1, delta)
        Assert.assertEquals(distance2, tableDistance2, delta)
        Assert.assertEquals(distance3, tableDistance3, delta)
        Assert.assertEquals(distance4, tableDistance4, delta)

        Assert.assertEquals(distance1, batchTableDistances[0], delta)
        Assert.assertEquals(distance2, batchTableDistances[1], delta)
        Assert.assertEquals(distance4, batchTableDistances[3], delta)
        Assert.assertEquals(distance3, batchTableDistances[2], delta)
    }
    println("""
        Avg. computeDistanceUsingLookupTable duration:                  ${totalComputeDistanceDuration / 4 / (numVectors - 4)}
        Avg. computeDistance4BatchUsingLookupTable duration:            ${totalComputeDistance4BatchDuration / (numVectors - 4)}
        Avg. computeDistance4BatchUsingLookupTable duration per vector: ${totalComputeDistance4BatchDuration / (numVectors - 4) / 4}
    """.trimIndent())
}