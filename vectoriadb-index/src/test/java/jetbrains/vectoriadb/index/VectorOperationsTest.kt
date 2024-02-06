/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import kotlin.math.sqrt
import kotlin.random.Random

@Suppress("SameParameterValue")
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
        v2HeapSeg.toFloatArray().assertEquals(v2)
        v2NativeSeg.toFloatArray().assertEquals(v2)
    }


    @Test
    fun `div by scalar`() {
        val v1Offset = 4
        val size = 133
        val v1 = randomFloatArray(size, v1Offset)
        val v1Clone = v1.copyOf()
        val scalar = Random.nextDouble(1.0, 10.0).toFloat()

        val expectedResult = naiveDiv(v1, v1Offset, scalar, size)

        val resultOffset = 3
        val result = randomFloatArray(size, resultOffset)

        VectorOperations.div(v1, v1Offset, scalar, result, resultOffset, size)

        result.checkResult(expectedResult, resultOffset)

        v1.assertEquals(v1Clone)
    }

    @Test
    fun `mul by scalar`() {
        val v1Offset = 4
        val size = 147
        val v1 = randomFloatArray(size, v1Offset)
        val v1Clone = v1.copyOf()
        val scalar = Random.nextDouble(1.0, 10.0).toFloat()

        val expectedResult = naiveMul(v1, v1Offset, scalar, size)

        val resultOffset = 3
        val result = randomFloatArray(size, resultOffset)

        VectorOperations.mul(v1, v1Offset, scalar, result, resultOffset, size)

        result.checkResult(expectedResult, resultOffset)

        v1.assertEquals(v1Clone)
    }

    @Test
    fun `calculate L2 norm`() = Arena.ofShared().use { arena ->
        val size = 191
        val v1Offset = 0
        val v1 = randomFloatArray(size, v1Offset)
        val v1Clone = v1.copyOf()
        val v1HeapSeg = MemorySegment.ofArray(v1)
        val v1NativeSeg = v1.copyToNativeSegment(arena)

        val expectedResult = naiveL2Norm(v1, v1Offset, size)

        listOf(
            VectorOperations.calculateL2Norm(v1),
            VectorOperations.calculateL2Norm(v1HeapSeg, size),
            VectorOperations.calculateL2Norm(v1NativeSeg, size)
        ).forEach {
            Assert.assertEquals(expectedResult, it, 1e-3f)
        }

        v1.assertEquals(v1Clone)
        v1HeapSeg.toFloatArray().assertEquals(v1)
        v1NativeSeg.toFloatArray().assertEquals(v1)
    }

    @Test
    fun `normalize L2`() = Arena.ofShared().use { arena ->
        val size = 173
        val v = randomFloatArray(size)
        val vNorm = VectorOperations.calculateL2Norm(v)
        val vClone = v.copyOf()
        val v1HeapSeg = MemorySegment.ofArray(v)
        val v1NativeSeg = v.copyToNativeSegment(arena)

        val expectedResult = naiveNormalizeL2(v, vNorm)

        val operationsCount = 3

        val result = Array(operationsCount) {
            randomFloatArray(size)
        }

        VectorOperations.normalizeL2(v, result[0])
        VectorOperations.normalizeL2(v1HeapSeg, vNorm, result[1], v.size)
        VectorOperations.normalizeL2(v1NativeSeg, vNorm, result[2], v.size)

        repeat(operationsCount) { i ->
            result[i].checkResult(expectedResult)
        }

        Assert.assertEquals(1f, VectorOperations.calculateL2Norm(result[0]), 1e-3f)

        v.assertEquals(vClone)
        v1HeapSeg.toFloatArray().assertEquals(v)
        v1NativeSeg.toFloatArray().assertEquals(v)
    }

    @Test
    fun innerProduct() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val v1Offset = 3
        val v2Offset = 2
        val v1 = randomFloatArray(vectorSize, v1Offset)
        val v2 = randomFloatArray(vectorSize, v2Offset)
        val v1Clone = v1.copyOf()
        val v2Clone = v2.copyOf()
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
        v1.assertEquals(v1Clone)
        v1HeapSeg.toFloatArray().assertEquals(v1)
        v1NativeSeg.toFloatArray().assertEquals(v1)
        v2.assertEquals(v2Clone)
        v2HeapSeg.toFloatArray().assertEquals(v2)
        v2NativeSeg.toFloatArray().assertEquals(v2)
    }
    
    @Test
    fun innerProductBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { i -> randomFloatArray(vectorSize, offset[i]) }
        val vClone = v.map { it.copyOf() }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val operationsCount = 3
        val expectedResult = FloatArray(operationsCount) { i ->
            naiveInnerProduct(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = Array(operationsCount) { FloatArray(4) }

        VectorOperations.innerProductBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperations.innerProductBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperations.innerProductBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.checkResult(expectedResult)
        }
        repeat(5) { i ->
            v[i].assertEquals(vClone[i])
            vNativeSeg[i].toFloatArray().assertEquals(vClone[i])
        }
    }

    @Test
    fun l2Distance() = Arena.ofShared().use { arena ->
        val vectorSize = 153
        val v1Offset = 3
        val v2Offset = 4
        val v1 = randomFloatArray(vectorSize, v1Offset)
        val v2 = randomFloatArray(vectorSize, v2Offset)
        val v1Clone = v1.copyOf()
        val v2Clone = v2.copyOf()
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
            Assert.assertEquals(expectedResult, result, 1e-3f)
        }

        v1.assertEquals(v1Clone)
        v1HeapSeg.toFloatArray().assertEquals(v1)
        v1NativeSeg.toFloatArray().assertEquals(v1)
        v2.assertEquals(v2Clone)
        v2HeapSeg.toFloatArray().assertEquals(v2)
        v2NativeSeg.toFloatArray().assertEquals(v2)
    }

    @Test
    fun l2DistanceBatch() = Arena.ofShared().use { arena ->
        val vectorSize = 131
        val offset = intArrayOf(1, 2, 3, 4, 5)
        val offsetL = offset.map { it.toLong() }
        val v = Array(5) { i -> randomFloatArray(vectorSize, offset[i]) }
        val vClone = v.map { it.copyOf() }
        val vNativeSeg = v.map { it.copyToNativeSegment(arena) }

        val operationsCount = 3
        val expectedResult = FloatArray(operationsCount) { i ->
            naiveL2Distance(v[0], offset[0], v[i + 1], offset[i + 1], vectorSize)
        }

        val result = List(operationsCount) { FloatArray(4) }

        VectorOperations.l2DistanceBatch(v[0], offset[0], v[1], offset[1], v[2], offset[2], v[3], offset[3], v[4], offset[4], vectorSize, result[0])

        VectorOperations.l2DistanceBatch(v[0], offset[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[1])

        VectorOperations.l2DistanceBatch(vNativeSeg[0], offsetL[0], vNativeSeg[1], offsetL[1], vNativeSeg[2], offsetL[2], vNativeSeg[3], offsetL[3], vNativeSeg[4], offsetL[4], vectorSize, result[2])

        result.forEach {
            it.checkResult(expectedResult)
        }
        repeat(5) { i ->
            v[i].assertEquals(vClone[i])
            vNativeSeg[i].toFloatArray().assertEquals(vClone[i])
        }
    }

    @Test
    fun computeGradientStep() = Arena.ofShared().use { arena ->
        val size = 173
        val currentOffset = 3
        val targetOffset = 4
        val current = randomFloatArray(size, currentOffset)
        val target = randomFloatArray(size, targetOffset)
        val currentClone = current.copyOf()
        val targetClone = target.copyOf()
        val targetHeapSeg = MemorySegment.ofArray(target)
        val targetNativeSeg = target.copyToNativeSegment(arena)
        val learningRate = Random.nextFloat()

        val expectedResult = naiveGradientStep(current, currentOffset, target, targetOffset, learningRate, size)

        val operationsCount = 3
        val resultOffset = Array(operationsCount) { it + 3 }
        val result = Array(operationsCount) { i ->
            randomFloatArray(size, resultOffset[i])
        }

        VectorOperations.computeGradientStep(
            current = current, currentIdx = currentOffset,
            target = target, targetIdx = targetOffset,
            result = result[0], resultIdx = resultOffset[0],
            size, learningRate
        )
        VectorOperations.computeGradientStep(
            current = current, currentIdx = currentOffset,
            target = targetHeapSeg, targetIdx = targetOffset.toLong(),
            result = result[1], resultIdx = resultOffset[1],
            size, learningRate
        )
        VectorOperations.computeGradientStep(
            current = current, currentIdx = currentOffset,
            target = targetNativeSeg, targetIdx = targetOffset.toLong(),
            result = result[2], resultIdx = resultOffset[2],
            size, learningRate
        )

        repeat(operationsCount) { i ->
            result[i].checkResult(expectedResult, resultOffset[i])
        }

        current.assertEquals(currentClone)
        target.assertEquals(targetClone)
        targetHeapSeg.toFloatArray().assertEquals(targetClone)
        targetNativeSeg.toFloatArray().assertEquals(targetClone)
    }

    private fun naiveDiv(v: FloatArray, idx: Int, scalar: Float, size: Int): FloatArray {
        val result = FloatArray(size)
        for (i in result.indices) {
            result[i] = v[idx + i] / scalar
        }
        return result
    }

    private fun naiveMul(v: FloatArray, idx: Int, scalar: Float, size: Int): FloatArray {
        val result = FloatArray(size)
        for (i in result.indices) {
            result[i] = v[idx + i] * scalar
        }
        return result
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

    private fun naiveL2Norm(v: FloatArray, offset: Int, size: Int): Float {
        var res = 0f
        repeat(size) { i ->
            res += v[i + offset] * v[i + offset]
        }
        return sqrt(res)
    }

    private fun naiveNormalizeL2(v: FloatArray, norm: Float): FloatArray {
        return FloatArray(v.size) { i ->
            v[i] / norm
        }
    }

    private fun naiveGradientStep(current: FloatArray, currentOffset: Int, target: FloatArray, targetOffset: Int, learningRate: Float, size: Int): FloatArray {
        return FloatArray(size) { i ->
            current[i + currentOffset] + (target[i + targetOffset] - current[i + currentOffset]) * learningRate
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

}
