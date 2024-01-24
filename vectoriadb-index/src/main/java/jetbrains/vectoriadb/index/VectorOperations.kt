package jetbrains.vectoriadb.index

import jdk.incubator.vector.FloatVector
import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.nio.ByteOrder
import kotlin.math.abs
import kotlin.math.sqrt

class VectorOperations {
    /*
    * Important:
    * 1. If you change something, make sure you have not hurt performance. Run DistanceComputationBenchmark before and after.
    * 2. 'protected' is used around here because using 'private' brakes jmh benchmarks, so do not replace it.
    *
    * Performance optimizations:
    * 1. It is important to have implementation functions inline. Otherwise, it affects performance dramatically.
    * 2. Do not use masked operations (at least for now, JVM 21). They affect performance very much.
    * 3. heap-byte-array-based segments are not supported intentionally. Using them affects performance when using
    * native-float segments, heap-float-array-based segments and just heap-float arrays.
    * 4. Checking what MemorySegment we have (native one or heap-float-array-based) before running
    * any computation helps JVM not to go crazy. Checking that inside the computation loop makes JVM
    * "optimize" performance the way that all the operations become 2x slower.
    * */
    @Suppress("ProtectedInFinal")
    companion object {

        const val PRECISION: Float = 1e-5f

        /*
        *
        * L2 DISTANCE
        *
        * */

        @JvmStatic
        fun l2Distance(
            v1: FloatArray,
            idx1: Int,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float {
            return l2DistanceImpl(
                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                size
            )
        }

        @JvmStatic
        fun l2Distance(
            v1: MemorySegment,
            idx1: Long,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float = if (v1.isNative) {
            l2DistanceImpl(v1, idx1, v2, idx2, size)
        } else {
            l2Distance(v1.heapBase().get() as FloatArray, idx1.toInt(), v2, idx2, size)
        }

        @JvmStatic
        fun l2Distance(
            v1: MemorySegment,
            idx1: Long,
            v2: MemorySegment,
            idx2: Long,
            size: Int
        ): Float = when {
            v1.isNative && v2.isNative -> l2DistanceImpl(v1, idx1, v2, idx2, size)
            v1.isNative -> l2DistanceImpl(v1, idx1, v2.heapBase().get() as FloatArray, idx2.toInt(), size)
            v2.isNative -> l2DistanceImpl(v2, idx2, v1.heapBase().get() as FloatArray, idx1.toInt(), size)
            else -> l2Distance(
                v1.heapBase().get() as FloatArray,
                idx1.toInt(),
                v2.heapBase().get() as FloatArray,
                idx2.toInt(),
                size
            )
        }

        @JvmStatic
        protected fun l2DistanceImpl(
            v1: MemorySegment,
            idx1: Long,
            v2: MemorySegment,
            idx2: Long,
            size: Int
        ): Float {
            return l2DistanceImpl(
                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                size
            )
        }

        @JvmStatic
        protected fun l2DistanceImpl(
            v1: MemorySegment,
            idx1: Long,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float = l2DistanceImpl(
            v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
            v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

            v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
            v2Value = { i -> v2[idx2 + i] },

            size
        )


        @JvmStatic
        protected inline fun l2DistanceImpl(
            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            v2: (Int, VectorSpecies<Float>) -> FloatVector,
            v2Value: (Int) -> Float,

            size: Int
        ): Float {
            val species1 = FloatVector.SPECIES_PREFERRED
            var sumV1 = FloatVector.zero(species1)
            var sum = 0f

            processVectors(
                process = { i, species ->
                    val V1 = v1(i, species)
                    val V2 = v2(i, species)

                    val diff = V1.sub(V2)
                    sumV1 = diff.fma(diff, sumV1)
                },
                processOneByOne = { i ->
                    val diff = v1Value(i) - v2Value(i)
                    sum += diff * diff
                },
                size
            )
            return sum + sumV1.reduceLanes(VectorOperators.ADD)
        }

        @JvmStatic
        fun l2DistanceBatch(
            q: FloatArray, idxQ: Int,
            v1: FloatArray, idx1: Int,
            v2: FloatArray, idx2: Int,
            v3: FloatArray, idx3: Int,
            v4: FloatArray, idx4: Int,
            size: Int, result: FloatArray
        ) {
            l2DistanceBatchImpl(
                q = { i, species ->  FloatVector.fromArray(species, q, idxQ + i) },
                qValue = { i -> q[idxQ + i] },

                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                v3 = { i, species -> FloatVector.fromArray(species, v3, idx3 + i) },
                v3Value = { i -> v3[idx3 + i] },

                v4 = { i, species -> FloatVector.fromArray(species, v4, idx4 + i) },
                v4Value = { i -> v4[idx4 + i] },

                size, result
            )
        }

        @JvmStatic
        fun l2DistanceBatch(
            q: MemorySegment, idxQ: Long,
            v1: MemorySegment, idx1: Long,
            v2: MemorySegment, idx2: Long,
            v3: MemorySegment, idx3: Long,
            v4: MemorySegment, idx4: Long,
            size: Int, result: FloatArray
        ) {
            l2DistanceBatchImpl(
                q = { i, species ->  FloatVector.fromMemorySegment(species, q, (idxQ + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                qValue = { i -> q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) },

                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                v3 = { i, species -> FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v3Value = { i -> v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i) },

                v4 = { i, species -> FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v4Value = { i -> v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i) },

                size, result
            )
        }

        @JvmStatic
        fun l2DistanceBatch(
            q: FloatArray, idxQ: Int,
            v1: MemorySegment, idx1: Long,
            v2: MemorySegment, idx2: Long,
            v3: MemorySegment, idx3: Long,
            v4: MemorySegment, idx4: Long,
            size: Int, result: FloatArray
        ) =  if (v1.isNative) { // all the other are also native
            l2DistanceBatchImpl(q, idxQ, v1, idx1, v2, idx2, v3, idx3, v4, idx4, size ,result)
        } else {
            l2DistanceBatch(q, idxQ,
                v1.heapBase().get() as FloatArray, idx1.toInt(),
                v2.heapBase().get() as FloatArray, idx2.toInt(),
                v3.heapBase().get() as FloatArray, idx3.toInt(),
                v4.heapBase().get() as FloatArray, idx4.toInt(),
                size, result
            )
        }

        @JvmStatic
        protected fun l2DistanceBatchImpl(
            q: FloatArray, idxQ: Int,
            v1: MemorySegment, idx1: Long,
            v2: MemorySegment, idx2: Long,
            v3: MemorySegment, idx3: Long,
            v4: MemorySegment, idx4: Long,
            size: Int, result: FloatArray
        ) {
            l2DistanceBatchImpl(
                q = { i, species -> FloatVector.fromArray(species, q, idxQ + i) },
                qValue = { i -> q[idxQ + i] },

                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                v3 = { i, species -> FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v3Value = { i -> v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i) },

                v4 = { i, species -> FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v4Value = { i -> v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i) },

                size, result
            )
        }

        @JvmStatic
        protected inline fun l2DistanceBatchImpl(
            q: (Int, VectorSpecies<Float>) -> FloatVector,
            qValue: (Int) -> Float,

            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            v2: (Int, VectorSpecies<Float>) -> FloatVector,
            v2Value: (Int) -> Float,

            v3: (Int, VectorSpecies<Float>) -> FloatVector,
            v3Value: (Int) -> Float,

            v4: (Int, VectorSpecies<Float>) -> FloatVector,
            v4Value: (Int) -> Float,

            size: Int,
            result: FloatArray
        ) {
            val species1 = FloatVector.SPECIES_PREFERRED

            var sumV1 = FloatVector.zero(species1)
            var sumV2 = FloatVector.zero(species1)
            var sumV3 = FloatVector.zero(species1)
            var sumV4 = FloatVector.zero(species1)

            var sum1 = 0f
            var sum2 = 0f
            var sum3 = 0f
            var sum4 = 0f

            processVectors(
                process = { i, species ->
                    val Q = q(i, species)

                    val V1 = v1(i, species)
                    val V2 = v2(i, species)
                    val V3 = v3(i, species)
                    val V4 = v4(i, species)

                    val diff1 = Q.sub(V1)
                    val diff2 = Q.sub(V2)
                    val diff3 = Q.sub(V3)
                    val diff4 = Q.sub(V4)

                    sumV1 = diff1.fma(diff1, sumV1)
                    sumV2 = diff2.fma(diff2, sumV2)
                    sumV3 = diff3.fma(diff3, sumV3)
                    sumV4 = diff4.fma(diff4, sumV4)
                },
                processOneByOne = { i ->
                    val qVal = qValue(i)
                    val v1Val = v1Value(i)
                    val v2Val = v2Value(i)
                    val v3Val = v3Value(i)
                    val v4Val = v4Value(i)
                    sum1 += (qVal - v1Val) * (qVal - v1Val)
                    sum2 += (qVal - v2Val) * (qVal - v2Val)
                    sum3 += (qVal - v3Val) * (qVal - v3Val)
                    sum4 += (qVal - v4Val) * (qVal - v4Val)
                },
                size
            )

            result[0] = sum1 + sumV1.reduceLanes(VectorOperators.ADD)
            result[1] = sum2 + sumV2.reduceLanes(VectorOperators.ADD)
            result[2] = sum3 + sumV3.reduceLanes(VectorOperators.ADD)
            result[3] = sum4 + sumV4.reduceLanes(VectorOperators.ADD)
        }

        /*
        *
        * INNER PRODUCT
        *
        * */

        @JvmStatic
        fun innerProduct(
            v1: FloatArray,
            idx1: Int,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float {
            return innerProductImpl(
                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                size
            )
        }

        @JvmStatic
        fun innerProduct(
            v1: MemorySegment,
            idx1: Long,
            v2: MemorySegment,
            idx2: Long,
            size: Int
        ): Float = when {
            v1.isNative && v2.isNative -> innerProductImpl(v1, idx1, v2, idx2, size)
            v1.isNative -> innerProductImpl(v1, idx1, v2.heapBase().get() as FloatArray, idx2.toInt(), size)
            v2.isNative -> innerProductImpl(v2, idx2, v1.heapBase().get() as FloatArray, idx1.toInt(), size)
            else -> innerProduct(v1.heapBase().get() as FloatArray, idx1.toInt(), v2.heapBase().get() as FloatArray, idx2.toInt(), size)
        }

        @JvmStatic
        fun innerProduct(
            v1: MemorySegment,
            idx1: Long,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float = if (v1.isNative) {
            innerProductImpl(v1, idx1, v2, idx2, size)
        } else {
            innerProduct(v1.heapBase().get() as FloatArray, idx1.toInt(), v2, idx2, size)
        }

        @JvmStatic
        protected fun innerProductImpl(
            v1: MemorySegment,
            idx1: Long,
            v2: MemorySegment,
            idx2: Long,
            size: Int
        ): Float {
            return innerProductImpl(
                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                size
            )
        }

        @JvmStatic
        protected fun innerProductImpl(
            v1: MemorySegment,
            idx1: Long,
            v2: FloatArray,
            idx2: Int,
            size: Int
        ): Float {
            return innerProductImpl(
                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                size
            )
        }

        @JvmStatic
        protected inline fun innerProductImpl(
            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            v2: (Int, VectorSpecies<Float>) -> FloatVector,
            v2Value: (Int) -> Float,

            size: Int
        ): Float {
            var sumVector = FloatVector.zero(FloatVector.SPECIES_PREFERRED)
            var sum = 0f
            processVectors(
                process = { i, species ->
                    val V1 = v1(i, species)
                    val V2 = v2(i, species)

                    sumVector = V1.fma(V2, sumVector)
                },
                processOneByOne = { i ->
                    sum += v1Value(i) * v2Value(i)
                },
                size
            )
            return sum + sumVector.reduceLanes(VectorOperators.ADD)
        }

        @JvmStatic
        fun innerProductBatch(
            q: FloatArray, idxQ: Int,
            v1: FloatArray, idx1: Int,
            v2: FloatArray, idx2: Int,
            v3: FloatArray, idx3: Int,
            v4: FloatArray, idx4: Int,
            size: Int, result: FloatArray
        ) {
            innerProductBatchImpl(
                q = { i, species -> FloatVector.fromArray(species, q, idxQ + i) },
                qValue = { i -> q[idxQ + i] },

                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                v3 = { i, species -> FloatVector.fromArray(species, v3, idx3 + i) },
                v3Value = { i -> v3[idx3 + i] },

                v4 = { i, species -> FloatVector.fromArray(species, v4, idx4 + i) },
                v4Value = { i -> v4[idx4 + i] },

                size, result
            )
        }

        @JvmStatic
        fun innerProductBatch(
            q: FloatArray, idxQ: Int,
            v1: MemorySegment, idx1: Long,
            v2: MemorySegment, idx2: Long,
            v3: MemorySegment, idx3: Long,
            v4: MemorySegment, idx4: Long,
            size: Int, result: FloatArray
        ) {
            innerProductBatchImpl(
                q = { i, species -> FloatVector.fromArray(species, q, idxQ + i) },
                qValue = { i -> q[idxQ + i] },

                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                v3 = { i, species -> FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v3Value = { i -> v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i) },

                v4 = { i, species -> FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v4Value = { i -> v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i) },

                size, result
            )
        }

        @JvmStatic
        fun innerProductBatch(
            q: MemorySegment, idxQ: Long,
            v1: MemorySegment, idx1: Long,
            v2: MemorySegment, idx2: Long,
            v3: MemorySegment, idx3: Long,
            v4: MemorySegment, idx4: Long,
            size: Int, result: FloatArray
        ) {
            innerProductBatchImpl(
                q = { i, species ->  FloatVector.fromMemorySegment(species, q, (idxQ + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                qValue = { i -> q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) },

                v1 = { i, species -> FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                v3 = { i, species -> FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v3Value = { i -> v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i) },

                v4 = { i, species -> FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v4Value = { i -> v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i) },

                size, result
            )
        }

        @JvmStatic
        protected inline fun innerProductBatchImpl(
            q: (Int, VectorSpecies<Float>) -> FloatVector,
            qValue: (Int) -> Float,

            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            v2: (Int, VectorSpecies<Float>) -> FloatVector,
            v2Value: (Int) -> Float,

            v3: (Int, VectorSpecies<Float>) -> FloatVector,
            v3Value: (Int) -> Float,

            v4: (Int, VectorSpecies<Float>) -> FloatVector,
            v4Value: (Int) -> Float,

            size: Int,
            result: FloatArray
        ) {
            val species1 = FloatVector.SPECIES_PREFERRED

            var sumV1 = FloatVector.zero(species1)
            var sumV2 = FloatVector.zero(species1)
            var sumV3 = FloatVector.zero(species1)
            var sumV4 = FloatVector.zero(species1)

            var sum1 = 0f
            var sum2 = 0f
            var sum3 = 0f
            var sum4 = 0f

            processVectors(
                process = { i, species ->
                    val Q = q(i, species)

                    val V1 = v1(i, species)
                    val V2 = v2(i, species)
                    val V3 = v3(i, species)
                    val V4 = v4(i, species)

                    sumV1 = Q.fma(V1, sumV1)
                    sumV2 = Q.fma(V2, sumV2)
                    sumV3 = Q.fma(V3, sumV3)
                    sumV4 = Q.fma(V4, sumV4)
                },
                processOneByOne = { i ->
                    val qVal = qValue(i)
                    sum1 += qVal * v1Value(i)
                    sum2 += qVal * v2Value(i)
                    sum3 += qVal * v3Value(i)
                    sum4 += qVal * v4Value(i)
                },
                size
            )

            result[0] = sum1 + sumV1.reduceLanes(VectorOperators.ADD)
            result[1] = sum2 + sumV2.reduceLanes(VectorOperators.ADD)
            result[2] = sum3 + sumV3.reduceLanes(VectorOperators.ADD)
            result[3] = sum4 + sumV4.reduceLanes(VectorOperators.ADD)
        }

        /**
         * vector1 + vector2 -> vector1
         */
        @JvmStatic
        fun add(
            v1: FloatArray, idx1: Int,
            v2: FloatArray, idx2: Int,
            result: FloatArray, resultIdx: Int,
            size: Int
        ) {
            addImpl(
                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromArray(species, v2, idx2 + i) },
                v2Value = { i -> v2[idx2 + i] },

                intoResult = { i, R -> R.intoArray(result, resultIdx + i) },
                intoResultValue = { i, value -> result[resultIdx + i] = value },

                size
            )
        }

        /**
         * vector1 + vector2 -> vector1
         */
        @JvmStatic
        fun add(
            v1: FloatArray, idx1: Int,
            v2: MemorySegment, idx2: Long,
            result: FloatArray, resultIdx: Int,
            size: Int
        ) = if (v2.isNative) {
            addImpl(v1, idx1, v2, idx2, result, resultIdx, size)
        } else {
            add(v1, idx1, v2.heapBase().get() as FloatArray, idx2.toInt(), result, resultIdx, size)
        }

        @JvmStatic
        protected fun addImpl(
            v1: FloatArray, idx1: Int,
            v2: MemorySegment, idx2: Long,
            result: FloatArray, resultIdx: Int,
            size: Int
        ) {
            addImpl(
                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                v2 = { i, species -> FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v2Value = { i -> v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i) },

                intoResult = { i, R -> R.intoArray(result, resultIdx + i) },
                intoResultValue = { i, value -> result[resultIdx + i] = value },

                size
            )
        }

        @JvmStatic
        protected fun addImpl(
            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            v2: (Int, VectorSpecies<Float>) -> FloatVector,
            v2Value: (Int) -> Float,

            intoResult: (Int, FloatVector) -> Unit,
            intoResultValue: (Int, Float) -> Unit,

            size: Int
        ) {
            processVectors(
                process = { i, species ->
                    val V1 = v1(i, species)
                    val V2 = v2(i, species)

                    val R = V1.add(V2)
                    intoResult(i, R)
                },
                processOneByOne = { i ->
                    val sum = v1Value(i) + v2Value(i)
                    intoResultValue(i, sum)
                },
                size
            )
        }

        /**
         * vector1 * scalar -> result
         */
        @JvmStatic
        fun mul(
            v1: FloatArray, idx1: Int,
            scalar: Float,
            result: FloatArray, resultIdx: Int,
            size: Int
        ) {
            processVectors(
                process = { i, species ->
                    val V1 = FloatVector.fromArray(species, v1, idx1 + i)
                    val R = V1.mul(scalar)
                    R.intoArray(result, resultIdx + i)
                },
                processOneByOne = { i ->
                    val res = v1[idx1 + i] * scalar
                    result[resultIdx + i] = res
                },
                size
            )
        }


        /**
         * vector1 / scalar -> result
         */
        @JvmStatic
        fun div(
            v1: FloatArray, idx1: Int,
            scalar: Float,
            result: FloatArray, resultIdx: Int,
            size: Int
        ) {
            divImpl(
                v1 = { i, species -> FloatVector.fromArray(species, v1, idx1 + i) },
                v1Value = { i -> v1[idx1 + i] },

                scalar = scalar,

                intoResult = { i, R -> R.intoArray(result, resultIdx + i) },
                intoResultValue = { i, value -> result[resultIdx + i] = value },

                size
            )
        }

        @JvmStatic
        protected inline fun divImpl(
            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            scalar: Float,

            intoResult: (Int, FloatVector) -> Unit,
            intoResultValue: (Int, Float) -> Unit,

            size: Int
        ) {
            processVectors(
                process = { i, species ->
                    val V1 = v1(i, species)
                    val R = V1.div(scalar)
                    intoResult(i, R)
                },
                processOneByOne = { i ->
                    val res = v1Value(i) / scalar
                    intoResultValue(i, res)
                },
                size
            )
        }

        @JvmStatic
        fun calculateL2Norm(vector: FloatArray): Float {
            return sqrt(innerProduct(vector, 0, vector, 0, vector.size).toDouble()).toFloat()
        }

        @JvmStatic
        fun calculateL2Norm(vector: MemorySegment, size: Int): Float {
            return sqrt(innerProduct(vector, 0, vector, 0, size).toDouble()).toFloat()
        }

        @JvmStatic
        fun normalizeL2(vector: MemorySegment, vectorNorm: Float, result: FloatArray, size: Int) = if (vector.isNative) {
            normalizeL2Impl(vector, vectorNorm, result, size)
        } else {
            normalizeL2Impl(vector.heapBase().get() as FloatArray, vectorNorm, result)
        }

        /**
         * Normalizes the vector by L2 norm, writes result to the result
         */
        @JvmStatic
        fun normalizeL2(vector: FloatArray, result: FloatArray) {
            normalizeL2Impl(vector, calculateL2Norm(vector), result)
        }

        @JvmStatic
        protected fun normalizeL2Impl(vector: MemorySegment, vectorNorm: Float, result: FloatArray, size: Int) {
            normalizeL2Impl(
                v1 = { i, species -> FloatVector.fromMemorySegment(species, vector, i.toLong() * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                v1Value = { i -> vector.getAtIndex(ValueLayout.JAVA_FLOAT, i.toLong()) },

                vectorNorm = vectorNorm,

                intoResult = { i, R -> R.intoArray(result, i) },
                intoResultValue = { i, value -> result[i] = value },

                copyVectorToResult = {
                    for (i in 0 until size) {
                        result[i] = vector.getAtIndex(ValueLayout.JAVA_FLOAT, i.toLong())
                    }
                },
                size
            )
        }

        @JvmStatic
        protected fun normalizeL2Impl(vector: FloatArray, vectorNorm: Float, result: FloatArray) {
            normalizeL2Impl(
                v1 = { i, species -> FloatVector.fromArray(species, vector, i) },
                v1Value = { i -> vector[i] },

                vectorNorm = vectorNorm,

                intoResult = { i, R -> R.intoArray(result, i) },
                intoResultValue = { i, value -> result[i] = value },

                copyVectorToResult = { vector.copyInto(result) },
                vector.size
            )
        }

        @JvmStatic
        protected inline fun normalizeL2Impl(
            v1: (Int, VectorSpecies<Float>) -> FloatVector,
            v1Value: (Int) -> Float,

            vectorNorm: Float,

            intoResult: (Int, FloatVector) -> Unit,
            intoResultValue: (Int, Float) -> Unit,

            copyVectorToResult: () -> Unit,
            size: Int
        ) {
            if (abs(vectorNorm - 1) > PRECISION) {
                divImpl(
                    v1 = v1,
                    v1Value = v1Value,
                    scalar = vectorNorm,
                    intoResult = intoResult,
                    intoResultValue = intoResultValue,
                    size
                )
            } else {
                copyVectorToResult()
            }
        }

        @JvmStatic
        fun computeGradientStep(
            currentV: FloatArray,
            currentVIdx: Int,

            targetV: MemorySegment,
            targetVIdx: Long,

            result: FloatArray,
            resultIdx: Int,

            size: Int,

            learningRate: Float
        ) = if (targetV.isNative) {
            computeGradientStepImpl(currentV, currentVIdx, targetV, targetVIdx, result, resultIdx, size, learningRate)
        } else {
            computeGradientStep(currentV, currentVIdx, targetV.heapBase().get() as FloatArray, targetVIdx.toInt(), result, resultIdx, size, learningRate)
        }

        @JvmStatic
        fun computeGradientStep(
            currentV: FloatArray,
            currentVIdx: Int,

            targetV: FloatArray,
            targetVIdx: Int,

            result: FloatArray,
            resultIdx: Int,

            size: Int,

            learningRate: Float
        ) {
            computeGradientStepImpl(
                current = { i, species -> FloatVector.fromArray(species, currentV, currentVIdx + i) },
                currentValue = { i -> currentV[currentVIdx + i] },

                target = { i, species -> FloatVector.fromArray(species, targetV, targetVIdx + i) },
                targetValue = { i -> targetV[targetVIdx + i] },

                intoResult = { i, R -> R.intoArray(result, resultIdx + i) },
                intoResultValue = { i, r -> result[resultIdx + i] = r },

                size,
                learningRate
            )
        }

        @JvmStatic
        protected fun computeGradientStepImpl(
            currentV: FloatArray,
            currentVIdx: Int,

            targetV: MemorySegment,
            targetVIdx: Long,

            result: FloatArray,
            resultIdx: Int,

            size: Int,

            learningRate: Float
        ) {
            computeGradientStepImpl(
                current = { i, species -> FloatVector.fromArray(species, currentV, currentVIdx + i) },
                currentValue = { i -> currentV[currentVIdx + i] },

                target = { i, species -> FloatVector.fromMemorySegment(species, targetV, (targetVIdx + i) * Float.SIZE_BYTES, ByteOrder.nativeOrder()) },
                targetValue = { i -> targetV.getAtIndex(ValueLayout.JAVA_FLOAT, targetVIdx + i) },

                intoResult = { i, R -> R.intoArray(result, resultIdx + i) },
                intoResultValue = { i, r -> result[resultIdx + i] = r },

                size,
                learningRate
            )
        }

        @JvmStatic
        protected inline fun computeGradientStepImpl(
            current: (Int, VectorSpecies<Float>) -> FloatVector,
            currentValue: (Int) -> Float,

            target: (Int, VectorSpecies<Float>) -> FloatVector,
            targetValue: (Int) -> Float,

            intoResult: (Int, FloatVector) -> Unit,
            intoResultValue: (Int, Float) -> Unit,

            size: Int,
            learningRate: Float
        ) {
            val learningRateV = FloatVector.broadcast(FloatVector.SPECIES_PREFERRED, learningRate)
            processVectors(
                process = { i, species ->
                    var currentV = current(i, species)
                    val targetV = target(i, species)

                    val diff = targetV.sub(currentV)
                    currentV = diff.fma(learningRateV, currentV)
                    intoResult(i, currentV)
                },
                processOneByOne = { i ->
                    val currentV = currentValue(i)
                    val targetV = targetValue(i)

                    val diff = targetV - currentV
                    intoResultValue(i, currentV + (diff * learningRate))
                },
                size
            )
        }

        @JvmStatic
        protected inline fun processVectors(
            process: (Int, VectorSpecies<Float>) -> Unit,
            processOneByOne: (Int) -> Unit,
            size: Int
        ) {
            val species = FloatVector.SPECIES_PREFERRED

            val loopBound = species.loopBound(size)
            val step = species.length()
            var i = 0

            while (i < loopBound) {
                process(i, species)
                i += step
            }

            while (i < size) {
                processOneByOne(i)
                i++
            }
        }
    }
}
