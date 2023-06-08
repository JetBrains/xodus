package jetbrains.exodus.diskann

import junit.framework.TestCase
import org.junit.Assert
import kotlin.random.Random

class DiskANNTest : TestCase() {
    fun testFindLoadedVertices() {
        val vectorDimensions = 8

        val vectorsCount = 10_000
        val seed = System.nanoTime()
        try {
            val rnd = Random(seed)
            val vectors = Array(vectorsCount + 1) { FloatArray(vectorDimensions) { rnd.nextFloat() } }

            val diskANN = DiskANN(vectorDimensions, L2Distance())
            var ts1 = System.nanoTime()
            diskANN.buildIndex(ArrayVectorReader(vectors))
            var ts2 = System.nanoTime()
            println("Index built in ${(ts2 - ts1) / 1000000} ms")


            ts1 = System.nanoTime()
            for (j in 0..vectorsCount) {
                val vector = vectors[j]
                val result = diskANN.nearest(vector, 1)

                Assert.assertEquals(1, result.size)
                Assert.assertEquals(j.toLong(), result[0])
            }
            ts2 = System.nanoTime()
            println("Avg. query time : ${(ts2 - ts1) / 1000 / vectorsCount} us")

        } catch (e: Throwable) {
            println("Seed: $seed")
            throw e
        }
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