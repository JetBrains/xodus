package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import kotlin.math.abs

class L2QuantizerTest {
    @Test
    fun `get vector approximation`() {
        getVectorApproximationTest(1e-5) { L2PQQuantizer() }
    }

    @Test
    fun `lookup table`() = lookupTableTest(L2DistanceFunction.INSTANCE, 1e-1f) { L2PQQuantizer() }
}

internal fun getVectorApproximationTest(delta: Double, quantizerBuilder: () -> Quantizer) = vectorTest(VectorDataset.Sift10K) {
    val compressionRatio = 32
    val l2Distance = L2DistanceFunction.INSTANCE

    val averageErrors = buildList {
        listOf(Quantizer.CODE_BASE_SIZE, Quantizer.CODE_BASE_SIZE * 2, Quantizer.CODE_BASE_SIZE * 4, Quantizer.CODE_BASE_SIZE * 8).forEach { numVectors ->
            val quantizer = quantizerBuilder()
            val vectorReader = FloatArrayToByteArrayVectorReader(vectors, numVectors)
            quantizer.generatePQCodes(compressionRatio, vectorReader, progressTracker)

            var totalError = 0.0
            repeat(numVectors) { vectorIdx ->
                val vector = vectors[vectorIdx]
                val vectorApproximation = quantizer.getVectorApproximation(vectorIdx)

                val error = l2Distance.computeDistance(vector, 0, vectorApproximation, 0, dimensions)
                totalError += error
            }
            add(totalError / numVectors)
        }
    }

    // if we quantize X vectors with X code base size, the approximated vectors should be equals (at least almost) to original vectors
    Assert.assertEquals(0.0, averageErrors[0], delta)

    // the more vectors the bigger average error we should get
    for (i in 1 until 4) {
        assert(averageErrors[i] > averageErrors[i - 1])
    }
}

fun lookupTableTest(distanceFun: DistanceFunction, delta: Float, buildQuantizer: () -> Quantizer) = vectorTest(VectorDataset.Sift10K) {
    val compressionRatio = 32

    val l2Quantizer = buildQuantizer()
    l2Quantizer.generatePQCodes(compressionRatio, vectorReader, progressTracker)

    repeat(numVectors - 4) { vector1Idx ->
        val vector2Idx = vector1Idx + 1
        val vector3Idx = vector2Idx + 1
        val vector4Idx = vector3Idx + 1

        val q = vectors[vector1Idx]

        val vector1Approximation = l2Quantizer.getVectorApproximation(vector1Idx)
        val vector2Approximation = l2Quantizer.getVectorApproximation(vector2Idx)
        val vector3Approximation = l2Quantizer.getVectorApproximation(vector3Idx)
        val vector4Approximation = l2Quantizer.getVectorApproximation(vector4Idx)

        val distance1 = distanceFun.computeDistance(q, 0, vector1Approximation, 0, dimensions)
        val distance2 = distanceFun.computeDistance(q, 0, vector2Approximation, 0, dimensions)
        val distance3 = distanceFun.computeDistance(q, 0, vector3Approximation, 0, dimensions)
        val distance4 = distanceFun.computeDistance(q, 0, vector4Approximation, 0, dimensions)

        val lookupTable = l2Quantizer.blankLookupTable()
        l2Quantizer.buildLookupTable(q, lookupTable, distanceFun)

        val tableDistance1 = l2Quantizer.computeDistanceUsingLookupTable(lookupTable, vector1Idx)
        val tableDistance2 = l2Quantizer.computeDistanceUsingLookupTable(lookupTable, vector2Idx)
        val tableDistance3 = l2Quantizer.computeDistanceUsingLookupTable(lookupTable, vector3Idx)
        val tableDistance4 = l2Quantizer.computeDistanceUsingLookupTable(lookupTable, vector4Idx)

        val batchTableDistances = FloatArray(4)
        l2Quantizer.computeDistance4BatchUsingLookupTable(lookupTable, vector1Idx, vector2Idx, vector3Idx, vector4Idx, batchTableDistances)

        Assert.assertEquals(distance1, tableDistance1, delta)
        Assert.assertEquals(distance2, tableDistance2, delta)
        Assert.assertEquals(distance3, tableDistance3, delta)
        Assert.assertEquals(distance4, tableDistance4, delta)

        Assert.assertEquals(distance1, batchTableDistances[0], delta)
        Assert.assertEquals(distance2, batchTableDistances[1], delta)
        Assert.assertEquals(distance4, batchTableDistances[3], delta)
        Assert.assertEquals(distance3, batchTableDistances[2], delta)
    }
}

class NormExplicitQuantizerTest {

    @Test
    fun `get vector approximation`() {
        getVectorApproximationTest(1e-2) { NormExplicitQuantizer(1) }
    }

    @Test
    fun `lookup table`() = lookupTableTest(DotDistanceFunction.INSTANCE, 1e-1f) { NormExplicitQuantizer(1) }

    @Test
    fun `average norm error for norm-explicit quantization should be less that for l2 quantization`() = vectorTest(VectorDataset.Sift10K) {
        val compressionRatio = 32
        val l2Distance = L2DistanceFunction.INSTANCE

        val ipQuantizer1 = NormExplicitQuantizer(1)
        ipQuantizer1.generatePQCodes(compressionRatio, vectorReader, progressTracker)

        val ipQuantizer2 = NormExplicitQuantizer(2)
        ipQuantizer2.generatePQCodes(compressionRatio, vectorReader, progressTracker)

        val l2Quantizer = L2PQQuantizer()
        l2Quantizer.generatePQCodes(compressionRatio, vectorReader, progressTracker)

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
            Average approximation error (L2 distance)
            l2 quantization:                               ${l2TotalError / numVectors}
            norm explicit quantization (1 norm codebook):  ${ip1TotalError / numVectors}
            norm explicit quantization (2 norm codebooks): ${ip2TotalError / numVectors}
            
            Average approximation norm error
            average vector norm:                           $averageNorm
            l2 quantization:                               $l2AverageNormError ${(l2AverageNormError / averageNorm).formatPercent(3)} 
            norm explicit quantization (1 norm codebook):  $ip1AverageNormError ${(ip1AverageNormError / averageNorm).formatPercent(3)}
            norm explicit quantization (2 norm codebooks): $ip2AverageNormError ${(ip2AverageNormError / averageNorm).formatPercent(3)}
        """.trimIndent())

        assert(ip2TotalNormError < ip1TotalNormError)
        assert(ip1TotalNormError < l2TotalNormError)
    }

    fun Double.formatPercent(digits: Int = 2): String = "%.${digits}f".format(this * 100) + "%"
}