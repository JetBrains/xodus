package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import kotlin.math.abs

class L2QuantizerTest {
    @Test
    fun `get vector approximation`() {
        getVectorApproximationTest { L2PQQuantizer() }
    }
}

internal fun getVectorApproximationTest(quantizerBuilder: () -> Quantizer) {
    val averageErrors = buildList {
        val vectors = LoadVectorsUtil.loadSift10KVectors()
        listOf(Quantizer.CODE_BASE_SIZE, Quantizer.CODE_BASE_SIZE * 2, Quantizer.CODE_BASE_SIZE * 4, Quantizer.CODE_BASE_SIZE * 8).forEach { numVectors ->
            val compressionRatio = 32
            val l2Distance = L2DistanceFunction.INSTANCE

            val dimensions = LoadVectorsUtil.SIFT_VECTOR_DIMENSIONS
            val vectorReader = FloatArrayToByteArrayVectorReader(vectors, numVectors)

            val progressTracker = NoOpProgressTracker()

            val quantizer = quantizerBuilder()
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
    Assert.assertEquals(0.0, averageErrors[0], 1e-3)

    // the more vectors the bigger average error we should get
    for (i in 1 until 4) {
        assert(averageErrors[i] > averageErrors[i - 1])
    }
}

class NormExplicitQuantizerTest {

    @Test
    fun `get vector approximation`() {
        getVectorApproximationTest { NormExplicitQuantizer(1) }
    }

    @Test
    fun `average norm error for norm-explicit quantization should be less that for l2 quantization`() {
        val compressionRatio = 32
        val l2Distance = L2DistanceFunction.INSTANCE

        val dimensions = LoadVectorsUtil.SIFT_VECTOR_DIMENSIONS
        val vectors = LoadVectorsUtil.loadSift10KVectors()
        val numVectors = vectors.count()
        val vectorReader = FloatArrayToByteArrayVectorReader(vectors)

        val progressTracker = ConsolePeriodicProgressTracker(1)
        progressTracker.start("test")

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