package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.VectorOperations.Companion.PRECISION
import org.junit.Assert
import org.junit.Test

class NormalizedVectorReaderTest {

    @Test
    fun `read normalized vectors`() {
        val progressTracker = NoOpProgressTracker()
        val pBuddy = ParallelBuddy(1, "test")

        val count = 10
        val dimensions = 30
        val reader = FloatArrayVectorReader(createRandomFloatArray2d(count, dimensions))
        val normReader = NormalizedVectorReader(reader)

        normReader.precalculateOriginalVectorNorms(pBuddy, progressTracker)

        repeat(count) { vectorIdx ->
            val precalcNorm = normReader.getOriginalVectorNorm(vectorIdx)
            val norm = VectorOperations.calculateL2Norm(reader.read(vectorIdx), dimensions)
            Assert.assertEquals(norm, precalcNorm, PRECISION)
        }

        repeat(count) { vectorIdx ->
            val vector = normReader.read(vectorIdx)
            val norm = VectorOperations.calculateL2Norm(vector, dimensions)
            Assert.assertEquals(1f, norm, PRECISION)
        }
    }
}