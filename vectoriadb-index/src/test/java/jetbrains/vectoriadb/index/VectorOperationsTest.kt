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
        val v1 = randomFloatArray(vectorSize, v1Offset)
        val v1Clone = v1.copyOf()
        val v2 = randomFloatArray(vectorSize, v2Offset)
        val v2Clone = v2.copyOf()
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveAdd(v1, v1Offset, v2, v2Offset, vectorSize)

        val operationsCount = 3
        val resultOffset = IntArray(operationsCount) { it + 1 }
        val result = Array(operationsCount) { i ->
            randomFloatArray(vectorSize, resultOffset[i])
        }

        VectorOperations.add(v1, v1Offset, v2, v2Offset, result[0], resultOffset[0], vectorSize)
        VectorOperations.add(v1, v1Offset, v2HeapSeg, v2Offset.toLong(), result[1], resultOffset[1], vectorSize)
        VectorOperations.add(v1, v1Offset, v2NativeSeg, v2Offset.toLong(), result[2], resultOffset[2], vectorSize)

        repeat(operationsCount) { i ->
            result[i].checkResult(expectedResult, resultOffset[i])
        }

        // original arrays has not changed
        v1.assertEquals(v1Clone)
        v2.assertEquals(v2Clone)
        v2HeapSeg.toArray(ValueLayout.JAVA_FLOAT).assertEquals(v2)
        v2NativeSeg.toArray(ValueLayout.JAVA_FLOAT).assertEquals(v2)
    }



   /* @Test
    fun `div by scalar1`() {
        val v1Offset = 4
        val size = 133
        val v1 = randomFloatArray(133 + v1Offset * 2)
        val scalar = Random.nextDouble(1.0, 10.0).toFloat()

        val expectedResult = naiveDiv(v1, v1Offset, scalar, size)

        val resultOffset = 3
        val result = randomFloatArray(size + resultOffset * 2)

        //VectorOperations.div(v1, v1Offset, scalar, )
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

    private fun naiveDiv(v: FloatArray, idx: Int, scalar: Float, size: Int): FloatArray {
        val result = FloatArray(size)
        for (i in result.indices) {
            result[i] = v[idx + i] / scalar
        }
        return result
    }*/

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
        val v1 = randomFloatArray(vectorSize + 5)
        val v2 = randomFloatArray(vectorSize + 5)
        val v1HeapSeg = MemorySegment.ofArray(v1)
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v1NativeSeg = v1.copyToNativeSegment(arena)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveInnerProduct(v1, v1Offset, v2, v2Offset, vectorSize)

        listOf(
            VectorOperations.innerProduct(v1, v1Offset, v2, v2Offset, vectorSize),
            VectorOperations.innerProduct(v1HeapSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.innerProduct(v1NativeSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.innerProduct(v1NativeSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.innerProduct(v1HeapSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize)
        ).forEach { result ->
            Assert.assertEquals(expectedResult, result, 1e-3f)
        }
    }

    @Test
    fun innerProductBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { randomFloatArray(vectorSize + 5) }
        val vHeapSeg = v.map { MemorySegment.ofArray(it) }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val expectedResult = FloatArray(4) { i ->
            naiveInnerProduct(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = List(3) { FloatArray(4) }

        VectorOperations.innerProductBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperations.innerProductBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperations.innerProductBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.checkResult(expectedResult)
        }
    }

    @Test
    fun l2Distance() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val v1Offset = 3
        val v2Offset = 2
        val v1 = randomFloatArray(vectorSize + 5)
        val v2 = randomFloatArray(vectorSize + 5)
        val v1HeapSeg = MemorySegment.ofArray(v1)
        val v2HeapSeg = MemorySegment.ofArray(v2)
        val v1NativeSeg = v1.copyToNativeSegment(arena)
        val v2NativeSeg = v2.copyToNativeSegment(arena)

        val expectedResult = naiveL2Distance(v1, v1Offset, v2, v2Offset, vectorSize)

        listOf(
            VectorOperations.l2Distance(v1, v1Offset, v2, v2Offset, vectorSize),

            VectorOperations.l2Distance(v1HeapSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.l2Distance(v1NativeSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.l2Distance(v1NativeSeg, v1Offset.toLong(), v2HeapSeg, v2Offset.toLong(), vectorSize),
            VectorOperations.l2Distance(v1HeapSeg, v1Offset.toLong(), v2NativeSeg, v2Offset.toLong(), vectorSize)
        ).forEach { result ->
            Assert.assertEquals(expectedResult, result, PRECISION)
        }
    }

    @Test
    fun l2DistanceBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { randomFloatArray(vectorSize + 5) }
        val vHeapSeg = v.map { MemorySegment.ofArray(it) }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val expectedResult = FloatArray(4) { i ->
            naiveL2Distance(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = List(3) { FloatArray(4) }

        VectorOperations.l2DistanceBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperations.l2DistanceBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperations.l2DistanceBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.checkResult(expectedResult)
        }
    }

    private fun FloatArray.checkResult(expectedResult: FloatArray, arrOffset: Int = 0) {
        assert(this.size - (arrOffset * 2) >= expectedResult.size)
        for (i in 0 until arrOffset) {
            Assert.assertEquals(this[i], 0f)
        }
        for (i in arrOffset + expectedResult.size until arrOffset + expectedResult.size + arrOffset) {
            Assert.assertEquals(this[i], 0f)
        }
        for (i in expectedResult.indices) {
            Assert.assertEquals(expectedResult[i], this[i + arrOffset], 1e-3f)
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


}
