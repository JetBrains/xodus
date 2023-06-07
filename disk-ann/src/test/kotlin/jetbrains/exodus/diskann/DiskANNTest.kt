package jetbrains.exodus.diskann

import junit.framework.TestCase
import org.junit.Assert
import kotlin.random.Random

class DiskANNTest : TestCase() {
    fun testFindLoadedVertices() {
        val vectorDimensions = 8
        for (i in 0 until 3) {
            val seed =  80692764524400L//System.nanoTime()
            println("Seed : $seed , i : $i")

            val rnd = Random(seed)
            val vectors = Array(i + 1) { FloatArray(vectorDimensions) { rnd.nextFloat() } }

            val diskANN = DiskANN(vectorDimensions, L2Distance())
            diskANN.buildIndex(ArrayVectorReader(vectors))
            for (j in 0..i) {
                val vector = vectors[j]
                val result = diskANN.nearest(vector, 1)

                Assert.assertEquals(1, result.size)
                Assert.assertEquals(j.toLong(), result[0])
            }
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