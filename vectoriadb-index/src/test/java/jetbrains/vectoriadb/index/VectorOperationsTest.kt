package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.VectorOperations.Companion.PRECISION
import org.junit.Assert
import org.junit.Test
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import kotlin.math.abs

class VectorOperationsTest {

    @Test
    fun add() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val v1Offset = 3
        val v2Offset = 4
        val v1 = createRandomFloatArray(1, vectorSize + v1Offset * 2)
        val v2 = createRandomFloatArray(1, vectorSize + v2Offset * 2)
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveAdd(v1, v1Offset, v2, v2Offset, vectorSize)
        val result = Array(3) {
            FloatArray(vectorSize)
        }

        VectorOperations.add(v1, v1Offset, v2, v2Offset, result[0], 0, vectorSize)
        VectorOperations.add(v1, v1Offset, v2HeapSeg, v2Offset.toLong(), result[1], 0, vectorSize)
        VectorOperations.add(v1, v1Offset, v2NativeSeg, v2Offset.toLong(), result[2], 0, vectorSize)

        result.forEach {
            expectedResult.assertEquals(it)
        }
    }

    @Test
    fun `div by scalar`() {
        val v1 = FloatArray(300)
        v1.fill(100f, 0, 100)
        v1.fill(90f, 100, 200)
        v1.fill(50f, 200, 300)

        VectorOperations.div(v1, 0, 2f, v1, 0, 100)
        VectorOperations.div(v1, 100, 3f, v1, 100, 100)
        VectorOperations.div(v1, 200, 5f, v1, 200, 100)

        v1.check(50f, 0, 100)
        v1.check(30f, 100, 100)
        v1.check(10f, 200, 100)
    }

    @Test
    fun innerProduct() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val v1Offset = 3
        val v2Offset = 2
        val v1 = createRandomFloatArray(1, vectorSize + 5)
        val v2 = createRandomFloatArray(1, vectorSize + 5)
        val v1HeapSeg = MemorySegment.ofArray(v1)
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v1NativeSeg = v1.copyToNativeSegment(arena)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveInnerProduct(v1, v1Offset, v2, v2Offset, vectorSize)

        listOf(
            VectorOperationsJ.innerProduct(v1, v1Offset, v2, v2Offset, vectorSize),
            VectorOperationsJ.innerProduct(v1HeapSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.innerProduct(v1NativeSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.innerProduct(v1NativeSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.innerProduct(v1HeapSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize)
        ).forEach { result ->
            Assert.assertEquals(expectedResult, result, PRECISION)
        }
    }

    @Test
    fun innerProductBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { createRandomFloatArray(1, vectorSize + 5) }
        val vHeapSeg = v.map { MemorySegment.ofArray(it) }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val expectedResult = FloatArray(4) { i ->
            naiveInnerProduct(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = List(3) { FloatArray(4) }

        VectorOperationsJ.innerProductBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperationsJ.innerProductBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperationsJ.innerProductBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.assertEquals(expectedResult)
        }
    }

    @Test
    fun l2Distance() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val v1Offset = 3
        val v2Offset = 2
        val v1 = createRandomFloatArray(1, vectorSize + 5)
        val v2 = createRandomFloatArray(1, vectorSize + 5)
        val v1HeapSeg = MemorySegment.ofArray(v1)
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v1NativeSeg = v1.copyToNativeSegment(arena)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveL2Distance(v1, v1Offset, v2, v2Offset, vectorSize)

        listOf(
            VectorOperationsJ.l2Distance(v1, v1Offset, v2, v2Offset, vectorSize),

            VectorOperationsJ.l2Distance(v1HeapSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.l2Distance(v1NativeSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.l2Distance(v1NativeSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperationsJ.l2Distance(v1HeapSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize)
        ).forEach { result ->
            Assert.assertEquals(expectedResult, result, PRECISION)
        }
    }

    @Test
    fun l2DistanceBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { createRandomFloatArray(1, vectorSize + 5) }
        val vHeapSeg = v.map { MemorySegment.ofArray(it) }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val expectedResult = FloatArray(4) { i ->
            naiveL2Distance(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = List(3) { FloatArray(4) }

        VectorOperationsJ.l2DistanceBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperationsJ.l2DistanceBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperationsJ.l2DistanceBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.assertEquals(expectedResult)
        }
    }

    private fun FloatArray.assertEquals(arr: FloatArray) {
        assert(this.size == arr.size)
        for (i in this.indices) {
            Assert.assertEquals(this[i], arr[i], 1e-3f)
        }
    }

    private fun naiveInnerProduct(v1: FloatArray, idx1: Int, v2: FloatArray, idx2: Int, size: Int): Float {
        var res = 0f
        repeat(size) { i ->
            res += v1[idx1 + i] * v2[idx2 + i]
        }

        return res
    }

    private fun naiveAdd(v1: FloatArray, idx1: Int, v2: FloatArray, idx2: Int, size: Int): FloatArray {
        return FloatArray(size) { i ->
            v1[idx1 + i] + v2[idx2 + i]
        }
    }

    private fun naiveL2Distance(v1: FloatArray, idx1: Int, v2: FloatArray, idx2: Int, size: Int): Float {
        var res = 0f
        repeat(size) { i ->
            res += (v1[idx1 + i] - v2[idx2 + i]) * (v1[idx1 + i] - v2[idx2 + i])
        }

        return res
    }

    private fun FloatArray.copyToNativeSegment(arena: Arena): MemorySegment {
        val res = arena.allocateFloat(this.size)
        MemorySegment.copy(this, 0, res, ValueLayout.JAVA_FLOAT, 0, this.size)
        return res
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
    fun `normalize L2`() = Arena.ofShared().use { arena ->
        val v1Array = floatArrayOf(1f, 2f, 3f, 4f, 5f)
        val v1Segment = v1Array.copyToNativeSegment(arena)
        val resultArray1 = FloatArray(v1Array.size)
        val resultArray2 = FloatArray(v1Array.size)

        VectorOperations.normalizeL2(v1Segment, VectorOperations.calculateL2Norm(v1Segment, v1Array.size), resultArray1, v1Array.size)
        VectorOperations.normalizeL2(v1Array, resultArray2)

        Assert.assertEquals(1f, VectorOperations.calculateL2Norm(resultArray1), PRECISION)
        Assert.assertEquals(1f, VectorOperations.calculateL2Norm(resultArray2), PRECISION)

        Assert.assertNotEquals(1f, VectorOperations.calculateL2Norm(v1Array), PRECISION)
        Assert.assertNotEquals(1f, VectorOperations.calculateL2Norm(v1Segment, v1Array.size), PRECISION)

        Assert.assertEquals(VectorOperations.calculateL2Norm(v1Segment, v1Array.size), VectorOperations.calculateL2Norm(v1Array), PRECISION)
    }

    @Test
    fun computeGradientStep() = Arena.ofShared().use { arena ->
        val size = 5
        val current = floatArrayOf(100f, 21f, 22f, 23f, 24f, 25f, 100f)
        val target = floatArrayOf(100f, 100f, 11f, 12f, 13f, 14f, 15f, 100f)
        val targetHeapBased = MemorySegment.ofArray(target)
        val targetNative = target.copyToNativeSegment(arena)
        val learningRate = 0.1f

        val expectedResult = floatArrayOf(20f, 21f, 22f, 23f, 24f)
        val result = FloatArray(size)

        VectorOperations.computeGradientStep(
            currentV = current, currentVIdx = 1,
            targetV = target, targetVIdx = 2,
            result = result, resultIdx = 0,
            size, learningRate
        )
        Assert.assertArrayEquals(expectedResult, result, 1e-5f)
        result.fill(0f)

        VectorOperations.computeGradientStep(
            currentV = current, currentVIdx = 1,
            targetV = targetHeapBased, targetVIdx = 2,
            result = result, resultIdx = 0,
            size, learningRate
        )
        Assert.assertArrayEquals(expectedResult, result, 1e-5f)
        result.fill(0f)

        VectorOperations.computeGradientStep(
            currentV = current, currentVIdx = 1,
            targetV = targetNative, targetVIdx = 2,
            result = result, resultIdx = 0,
            size, learningRate
        )
        Assert.assertArrayEquals(expectedResult, result, 1e-5f)
        result.fill(0f)
    }

    private fun FloatArray.check(value: Float, fromIdx: Int, count: Int) {
        var idx = fromIdx
        repeat(count) {
            assert(abs(this[idx] - value) < PRECISION)
            idx++
        }
    }

    private fun Arena.allocateFloat(count: Int): MemorySegment {
        return allocate(count.toLong() * Float.SIZE_BYTES, ValueLayout.JAVA_FLOAT.byteAlignment())
    }
}
