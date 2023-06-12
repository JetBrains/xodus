package jetbrains.exodus.diskann

import org.junit.Assert
import org.junit.Test
import java.nio.ByteBuffer
import java.security.SecureRandom
import kotlin.random.Random

class DiskANNTest {
    @Test
    fun testFindLoadedVertices() {
        val vectorDimensions = 64

        val vectorsCount = 10_000
        val secureRandom = SecureRandom()
        val seed = ByteBuffer.wrap(secureRandom.generateSeed(8)).long
        try {
            val rnd = Random(seed)
            val vectors = Array(vectorsCount, { FloatArray(vectorDimensions) })
            val addedVectors = HashSet<FloatArrayHolder>()

            for (i in vectors.indices) {
                val vector = vectors[i]

                var counter = 0
                do {
                    if (counter > 0) {
                        println("duplicate vector found ${counter}, retrying...")
                    }

                    for (j in vector.indices) {
                        vector[j] = 10 * rnd.nextFloat()
                    }
                    counter++
                } while (!addedVectors.add(FloatArrayHolder(vector)))
            }

            val diskANN = DiskANN("test index", vectorDimensions, L2Distance())
            var ts1 = System.nanoTime()
            diskANN.buildIndex(ArrayVectorReader(vectors))
            var ts2 = System.nanoTime()
            println("Index built in ${(ts2 - ts1) / 1000000} ms")

            var errorsCount = 0
            ts1 = System.nanoTime()
            for (j in 0 until vectorsCount) {
                val vector = vectors[j]
                val result = diskANN.nearest(vector, 1)
                Assert.assertEquals("j = $j", 1, result.size)
                if (j.toLong() != result[0]) {
                    errorsCount++
                }
            }
            ts2 = System.nanoTime()
            val errorPercentage = errorsCount * 100.0 / vectorsCount

            println("Avg. query time : ${(ts2 - ts1) / 1000 / vectorsCount} us, errors: $errorPercentage%")

            Assert.assertTrue(errorPercentage <= 5)

        } catch (e: Throwable) {
            println("Seed: $seed")
            throw e
        }
    }

}

internal class FloatArrayHolder(val floatArray: FloatArray) {
    override fun equals(other: Any?): Boolean {
        if (other is FloatArrayHolder) {
            return floatArray.contentEquals(other.floatArray)
        }
        return false
    }

    override fun hashCode(): Int {
        return floatArray.contentHashCode()
    }
}

internal class ArrayVectorReader(val vectors: Array<FloatArray>) : VectorReader {
    override fun size(): Long {
        return vectors.size.toLong()
    }

    override fun read(index: Long): Pair<Long, FloatArray> {
        return Pair(index, vectors[index.toInt()])
    }
}