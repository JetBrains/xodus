package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.VectorOperations.Companion.PRECISION
import org.junit.Assert
import org.junit.Test
import java.lang.foreign.MemorySegment
import kotlin.random.Random

@Suppress("SameParameterValue")
class FloatVectorSegmentTest {

    private val dimensions = 111

    @Test
    fun equals() {
        val count = 5
        val v1 = FloatVectorSegment.makeSegment(count, dimensions)
        val v2 = FloatVectorSegment.makeSegment(count, dimensions)
        repeat(count) { vi ->
            repeat(dimensions) { di ->
                val value = Random.nextDouble(1000.0).toFloat()
                v1.set(vi, di, value)
                v2.set(vi, di, value)
            }
        }

        repeat(count) { vi ->
            assert(v1.equals(vi, v2, vi))
        }
    }

    @Test
    fun copy() {
        val count = 5
        val v = randomFloatVectorSegment(count, dimensions)
        val vCopy = v.copy()
        assert(v.equals(vCopy))
    }

    @Test
    fun set() {
        val v1 = randomFloatVectorSegment(1, dimensions)
        val arr = randomFloatArray(dimensions)

        v1.set(0, MemorySegment.ofArray(arr))

        Assert.assertArrayEquals(arr, v1.internalArray, 1e-5f)
    }

    @Test
    fun toArray() {
        val count = 30
        val v = randomFloatVectorSegment(count, dimensions)

        val vArray = v.toArray()

        repeat(count) { vectorIdx ->
            repeat(dimensions) { dimension ->
                Assert.assertEquals(v.get(vectorIdx, dimension), vArray[vectorIdx][dimension], PRECISION)
            }
        }
    }

    @Test
    fun `simplest add`() {
        val v1 = randomFloatVectorSegment(1, dimensions)
        val v2 = randomFloatVectorSegment(1, dimensions)

        val v2Copy = v2.copy()
        val expectedResult = naiveSum(v1, 0, v2, 0)

        v1.add(0, v2, 0)

        // v2 remains the same
        assert(v2.equals(v2Copy))

        // v1 changed
        assert(v1.equals(0, expectedResult, 0))
    }


    @Test
    fun `simple add`() {
        val count1 = 2
        val count2 = 2
        val v1 = createSequentialVectorSegment(count1, 1f)
        val v2 = createSequentialVectorSegment(count2, 3f)
        val idx1 = 1
        val idx2 = 1

        val v1Copy = v1.copy()
        val v2Copy = v2.copy()
        val expectedResult = naiveSum(v1, idx1, v2, idx2)

        v1.add(idx1, v2, idx2)

        // v2 remains the same
        for (i in 0 until count2) {
            assert(v2.equals(i, v2Copy, i))
        }

        // only idx1 changed in v1
        for (i in 0 until count1) {
            if (i == idx1) continue
            assert(v1.equals(i, v1Copy, i))
        }
        assert(v1.equals(idx1, expectedResult, 0))
    }

    @Test
    fun add() {
        val count1 = 4
        val count2 = 5
        val v1 = randomFloatVectorSegment(count1, dimensions)
        val v2 = randomFloatVectorSegment(count2, dimensions)
        val idx1 = 2
        val idx2 = 3

        val v1Copy = v1.copy()
        val v2Copy = v2.copy()
        val expectedResult = naiveSum(v1, idx1, v2, idx2)

        v1.add(idx1, v2, idx2)

        // v2 remains the same
        for (i in 0 until count2) {
            assert(v2.equals(i, v2Copy, i))
        }

        // only idx1 changed in v1
        for (i in 0 until count1) {
            if (i == idx1) continue
            assert(v1.equals(i, v1Copy, i))
        }
        assert(v1.equals(idx1, expectedResult, 0))
    }

    @Test
    fun `div by scalar`() {
        val count1 = 4
        val v1 = randomFloatVectorSegment(count1, dimensions)
        val idx1 = 1
        val scalar = 13f

        val v1Copy = v1.copy()
        val expectedResult = naiveDiv(v1, idx1, scalar)

        v1.div(idx1, scalar)

        // only idx1 changed in v1
        for (i in 0 until count1) {
            if (i == idx1) continue
            assert(v1.equals(i, v1Copy, i))
        }
        assert(v1.equals(idx1, expectedResult, 0))
    }

    private fun naiveSum(v1: FloatVectorSegment, idx1: Int, v2: FloatVectorSegment, idx2: Int): FloatVectorSegment {
        val result = FloatVectorSegment.makeSegment(1, dimensions)
        for (i in 0 until dimensions) {
            result.set(0, i, v1.get(idx1, i) + v2.get(idx2, i))
        }
        return result
    }

    @Suppress("SameParameterValue")
    private fun naiveDiv(v1: FloatVectorSegment, idx1: Int, scalar: Float): FloatVectorSegment {
        val result = FloatVectorSegment.makeSegment(1, dimensions)
        for (i in 0 until dimensions) {
            result.set(0, i, v1.get(idx1, i) / scalar)
        }
        return result
    }

    private fun createSequentialVectorSegment(count: Int, valueStartFrom: Float): FloatVectorSegment {
        val v1 = FloatVectorSegment.makeSegment(count, dimensions)

        var value = valueStartFrom
        for (vectorIdx in 0 until count) {
            for (dimension in 0 until dimensions) {
                v1.set(vectorIdx, dimension, value)
            }
            value += 1
        }
        return v1
    }
}