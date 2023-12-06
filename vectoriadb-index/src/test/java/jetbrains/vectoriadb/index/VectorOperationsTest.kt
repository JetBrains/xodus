package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.VectorOperations.PRECISION
import org.junit.Assert
import org.junit.Test
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import kotlin.math.abs

class VectorOperationsTest {

    @Test
    fun add() = Arena.ofConfined().use { arena ->
        val v1 = arena.allocateFloat(300)
        v1.fill(1f, 0, 100)
        v1.fill(2f, 100, 100)
        v1.fill(3f, 200, 100)

        val v2 = arena.allocateFloat(300)
        v2.fill(10f, 0, 100)
        v2.fill(20f, 100, 100)
        v2.fill(30f, 200, 100)

        VectorOperations.add(v1, 0, v2, 0, v1, 0, 100)
        VectorOperations.add(v1, 100, v2, 100, v1, 100, 100)
        VectorOperations.add(v1, 200, v2, 200, v1, 200, 100)

        v1.check(11f, 0, 100)
        v1.check(22f, 100, 100)
        v1.check(33f, 200, 100)
        v2.check(10f, 0, 100)
        v2.check(20f, 100, 100)
        v2.check(30f, 200, 100)
    }

    @Test
    fun `add skewed vectors`() = Arena.ofConfined().use { arena ->
        val v1 = arena.allocateFloat(310)
        v1.fill(1f, 10, 100)
        v1.fill(2f, 110, 100)
        v1.fill(3f, 210, 100)

        val v2 = arena.allocateFloat(350)
        v2.fill(10f, 50, 100)
        v2.fill(20f, 150, 100)
        v2.fill(30f, 250, 100)

        VectorOperations.add(v1, 10, v2, 50, v1, 10, 100)
        VectorOperations.add(v1, 110, v2, 150, v1, 110, 100)
        VectorOperations.add(v1, 210, v2, 250, v1, 210, 100)

        v1.check(11f, 10, 100)
        v1.check(22f, 110, 100)
        v1.check(33f, 210, 100)

        v2.check(10f, 50, 100)
        v2.check(20f, 150, 100)
        v2.check(30f, 250, 100)
    }

    @Test
    fun `div by scalar`() = Arena.ofConfined().use { arena ->
        val v1 = arena.allocateFloat(300)
        v1.fill(100f, 0, 100)
        v1.fill(90f, 100, 100)
        v1.fill(50f, 200, 100)

        VectorOperations.div(v1, 0, 2f, v1, 0, 100)
        VectorOperations.div(v1, 100, 3f, v1, 100, 100)
        VectorOperations.div(v1, 200, 5f, v1, 200, 100)

        v1.check(50f, 0, 100)
        v1.check(30f, 100, 100)
        v1.check(10f, 200, 100)
    }

    @Test
    fun innerProduct() {
        val v1 = MemorySegment.ofArray(floatArrayOf(100f, 1f, 2f, 3f, 100f))
        val v2 = MemorySegment.ofArray(floatArrayOf(100f, 100f, 4f, 5f, 6f, 100f))

        val result = VectorOperations.innerProduct(v1, 1, v2, 2, 3)
        Assert.assertEquals(32f, result, PRECISION)
    }

    @Test
    fun `calculate L2 norm`() {
        val v1Array = floatArrayOf(1f, 3f, 1f, 3f, 1f, 2f)
        val v1Segment = MemorySegment.ofArray(v1Array)

        val arrayNorm = VectorOperations.calculateL2Norm(v1Array)
        val segmentNorm = VectorOperations.calculateL2Norm(v1Segment, v1Array.size)

        Assert.assertEquals(5f, segmentNorm, PRECISION)
        Assert.assertEquals(arrayNorm, segmentNorm, PRECISION)
    }

    @Test
    fun `normalize L2`() {
        val v1Array = floatArrayOf(1f, 2f, 3f, 4f, 5f)
        val v1Segment = MemorySegment.ofArray(v1Array)
        val resultArray = FloatArray(v1Array.size)
        val resultSegment = MemorySegment.ofArray(FloatArray(v1Array.size))

        VectorOperations.normalizeL2(v1Array, resultArray)
        VectorOperations.normalizeL2(v1Segment, VectorOperations.calculateL2Norm(v1Array), resultSegment, v1Array.size)

        Assert.assertEquals(1f, VectorOperations.calculateL2Norm(resultArray), PRECISION)
        Assert.assertEquals(VectorOperations.calculateL2Norm(resultSegment, v1Array.size), VectorOperations.calculateL2Norm(resultArray), PRECISION)

        Assert.assertNotEquals(1f, VectorOperations.calculateL2Norm(v1Array), PRECISION)
        Assert.assertEquals(VectorOperations.calculateL2Norm(v1Segment, v1Array.size), VectorOperations.calculateL2Norm(v1Array), PRECISION)
    }

    private fun MemorySegment.check(value: Float, fromIdx: Int, count: Int) {
        var idx = fromIdx
        repeat(count) {
            assert(abs(getAtIndex(ValueLayout.JAVA_FLOAT, idx.toLong()) - value) < PRECISION)
            idx++
        }
    }

    private fun MemorySegment.fill(value: Float, fromIdx: Int, count: Int) {
        var idx = fromIdx
        repeat(count) {
            setAtIndex(ValueLayout.JAVA_FLOAT, idx.toLong(), value)
            idx++
        }
    }

    private fun Arena.allocateFloat(count: Int): MemorySegment {
        return allocate(count.toLong() * Float.SIZE_BYTES, ValueLayout.JAVA_FLOAT.byteAlignment())
    }
}