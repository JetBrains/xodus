package jetbrains.vectoriadb.index.segment

import jetbrains.vectoriadb.index.vector.VectorOperations.PRECISION
import org.junit.Assert
import org.junit.Test
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout
import kotlin.random.Random

class FloatVectorSegmentTest {

    private val dimensions = 111

    @Test
    fun equals() {
        val count = 5
        val v1 = FloatVectorSegment.makeArraySegment(count, dimensions)
        val v2 = FloatVectorSegment.makeArraySegment(count, dimensions)
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
        Arena.ofConfined().use { arena ->
            val count = 5
            val v = arena.createRandomVectorSegment(count)
            val vCopy = v.copy()
            assert(v.equals(vCopy))
        }
    }

    @Test
    fun `simplest add`() = Arena.ofConfined().use { arena ->
        listOf(true, false).forEach { heapBasedSegments ->
            val v1 = arena.createRandomVectorSegment(1, heapBasedSegments)
            val v2 = arena.createRandomVectorSegment(1, heapBasedSegments)

            val v2Copy = v2.copy()
            val expectedResult = naiveSum(v1, 0, v2, 0)

            v1.add(0, v2, 0)

            // v2 remains the same
            assert(v2.equals(v2Copy))

            // v1 changed
            assert(v1.equals(0, expectedResult, 0))
        }
    }


    @Test
    fun `simple add`() = Arena.ofConfined().use { arena ->
        listOf(true, false).forEach { heapBasedSegments ->
            val count1 = 2
            val count2 = 2
            val v1 = arena.createSequentialVectorSegment(count1, 1f, heapBasedSegments)
            val v2 = arena.createSequentialVectorSegment(count2, 3f, heapBasedSegments)
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
    }

    @Test
    fun add() = Arena.ofConfined().use { arena ->
        listOf(false, true).forEach { heapBasedSegments ->
            val count1 = 4
            val count2 = 5
            val v1 = arena.createRandomVectorSegment(count1, heapBasedSegments)
            val v2 = arena.createRandomVectorSegment(count2, heapBasedSegments)
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
    }

    @Test
    fun `div by scalar`() = Arena.ofConfined().use { arena ->
        listOf(false, true).forEach { heapBasedSegments ->
            val count1 = 4
            val v1 = arena.createRandomVectorSegment(count1, heapBasedSegments)
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
    }

    @Test
    fun `get whole vector`() = Arena.ofConfined().use { arena ->
        listOf(false, true).forEach { heapBasedSegments ->
            val count = 10
            val segment = arena.createRandomVectorSegment(count, heapBasedSegments)

            repeat(count) { vectorIdx ->
                val v = segment.get(vectorIdx)
                repeat(dimensions) { dimensionIdx ->
                    Assert.assertEquals(segment.get(vectorIdx, dimensionIdx), v.getAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx.toLong()), PRECISION)
                }

            }
        }
    }

    private fun naiveSum(v1: FloatVectorSegment, idx1: Int, v2: FloatVectorSegment, idx2: Int): FloatVectorSegment {
        val result = FloatVectorSegment.makeArraySegment(1, dimensions)
        for (i in 0 until dimensions) {
            result.set(0, i, v1.get(idx1, i) + v2.get(idx2, i))
        }
        return result
    }

    @Suppress("SameParameterValue")
    private fun naiveDiv(v1: FloatVectorSegment, idx1: Int, scalar: Float): FloatVectorSegment {
        val result = FloatVectorSegment.makeArraySegment(1, dimensions)
        for (i in 0 until dimensions) {
            result.set(0, i, v1.get(idx1, i) / scalar)
        }
        return result
    }

    private fun Arena.createRandomVectorSegment(count: Int, heapBased: Boolean = false): FloatVectorSegment {
        val v1 = if (heapBased) {
            FloatVectorSegment.makeArraySegment(count, dimensions)
        } else {
            FloatVectorSegment.makeNativeSegment(this, count, dimensions)
        }

        for (vectorIdx in 0 until count) {
            for (dimension in 0 until dimensions) {
                v1.set(vectorIdx, dimension, Random.nextDouble(1000.0).toFloat())
            }
        }
        return v1
    }

    private fun Arena.createSequentialVectorSegment(count: Int, valueStartFrom: Float, native: Boolean = false): FloatVectorSegment {
        val v1 = if (native) {
            FloatVectorSegment.makeNativeSegment(this, count, dimensions)
        } else {
            FloatVectorSegment.makeArraySegment(count, dimensions)
        }

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