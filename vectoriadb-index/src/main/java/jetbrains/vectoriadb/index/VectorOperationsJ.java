package jetbrains.vectoriadb.index;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class VectorOperationsJ {
    //public final static float PRECISION = 1e-5f;

    /*
    *
    * L2 DISTANCE
    *
    * */

    public static float l2Distance(float[] v1, int idx1, float[] v2, int idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromArray(species, v1, idx1 + i);
            var V2 = FloatVector.fromArray(species, v2, idx2 + i);

            var diff = V1.sub(V2);
            sumV = diff.fma(diff, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff = v1[idx1 + i] - v2[idx2 + i];
            sum += diff * diff;
            i++;
        }

        return sum;
    }

    static float l2Distance(MemorySegment v1, long idx1, MemorySegment v2, long idx2, int size) {
        float res;
        if (v1.isNative() && v2.isNative()) {
            res = l2DistanceImpl(v1, idx1, v2, idx2, size);
        } else if (v1.isNative()) {
            res = l2DistanceImpl(v1, idx1, (float[]) v2.heapBase().get(), (int) idx2, size);
        } else if (v2.isNative()) {
            res = l2DistanceImpl(v2, idx2, (float[]) v1.heapBase().get(), (int) idx1, size);
        } else {
            res = l2Distance((float[]) v1.heapBase().get(), (int) idx1, (float[]) v2.heapBase().get(), (int) idx2, size);
        }
        return res;
    }

    static float l2Distance(MemorySegment v1, long idx1, float[] v2, int idx2, int size) {
        float res;
        if (v1.isNative()) {
            res = l2DistanceImpl(v1, idx1, v2, idx2, size);
        } else {
            res = l2Distance((float[]) v1.heapBase().get(), (int) idx1, v2, idx2, size);
        }
        return res;
    }

    private static float l2DistanceImpl(MemorySegment v1, long idx1, float[] v2, int idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromArray(species, v2, idx2 + i);

            var diff = V1.sub(V2);
            sumV = diff.fma(diff, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff = v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) - v2[idx2 + i];
            sum += diff * diff;
            i++;
        }

        return sum;
    }

    private static float l2DistanceImpl(MemorySegment v1, long idx1, MemorySegment v2, long idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());

            var diff = V1.sub(V2);
            sumV = diff.fma(diff, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff = v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i) - v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i);
            sum += diff * diff;
            i++;
        }

        return sum;
    }

    static void l2DistanceBatch(
            float[] q, int idxQ,
            float[] v1, int idx1,
            float[] v2, int idx2,
            float[] v3, int idx3,
            float[] v4, int idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromArray(species, q, (idxQ + i));
            var V1 = FloatVector.fromArray(species, v1, (idx1 + i));
            var V2 = FloatVector.fromArray(species, v2, (idx2 + i));
            var V3 = FloatVector.fromArray(species, v3, (idx3 + i));
            var V4 = FloatVector.fromArray(species, v4, (idx4 + i));

            var diff1 = Q.sub(V1);
            var diff2 = Q.sub(V2);
            var diff3 = Q.sub(V3);
            var diff4 = Q.sub(V4);

            sumV1 = diff1.fma(diff1, sumV1);
            sumV2 = diff2.fma(diff2, sumV2);
            sumV3 = diff3.fma(diff3, sumV3);
            sumV4 = diff4.fma(diff4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff1 = q[idxQ + i] - v1[idx1 + i];
            var diff2 = q[idxQ + i] - v2[idx2 + i];
            var diff3 = q[idxQ + i] - v3[idx3 + i];
            var diff4 = q[idxQ + i] - v4[idx4 + i];
            result[0] += diff1 * diff1;
            result[1] += diff2 * diff2;
            result[2] += diff3 * diff3;
            result[3] += diff4 * diff4;
            i++;
        }
    }

    static void l2DistanceBatch(
            MemorySegment q, long idxQ,
            MemorySegment v1, long idx1,
            MemorySegment v2, long idx2,
            MemorySegment v3, long idx3,
            MemorySegment v4, long idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromMemorySegment(species, q, (idxQ + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V3 = FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V4 = FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.BYTES, ByteOrder.nativeOrder());

            var diff1 = Q.sub(V1);
            var diff2 = Q.sub(V2);
            var diff3 = Q.sub(V3);
            var diff4 = Q.sub(V4);

            sumV1 = diff1.fma(diff1, sumV1);
            sumV2 = diff2.fma(diff2, sumV2);
            sumV3 = diff3.fma(diff3, sumV3);
            sumV4 = diff4.fma(diff4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff1 = q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) - v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i);
            var diff2 = q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) - v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i);
            var diff3 = q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) - v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i);
            var diff4 = q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) - v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i);
            result[0] += diff1 * diff1;
            result[1] += diff2 * diff2;
            result[2] += diff3 * diff3;
            result[3] += diff4 * diff4;
            i++;
        }
    }

    static void l2DistanceBatch(
            float[] q, int idxQ,
            MemorySegment v1, long idx1,
            MemorySegment v2, long idx2,
            MemorySegment v3, long idx3,
            MemorySegment v4, long idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromArray(species, q, idxQ + i);
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V3 = FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V4 = FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.BYTES, ByteOrder.nativeOrder());

            var diff1 = Q.sub(V1);
            var diff2 = Q.sub(V2);
            var diff3 = Q.sub(V3);
            var diff4 = Q.sub(V4);

            sumV1 = diff1.fma(diff1, sumV1);
            sumV2 = diff2.fma(diff2, sumV2);
            sumV3 = diff3.fma(diff3, sumV3);
            sumV4 = diff4.fma(diff4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            var diff1 = q[idxQ + i] - v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i);
            var diff2 = q[idxQ + i] - v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i);
            var diff3 = q[idxQ + i] - v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i);
            var diff4 = q[idxQ + i] - v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i);
            result[0] += diff1 * diff1;
            result[1] += diff2 * diff2;
            result[2] += diff3 * diff3;
            result[3] += diff4 * diff4;
            i++;
        }
    }

    /*
    *
    * INNER PRODUCT
    *
    * */

    public static float innerProduct(float[] v1, int idx1, float[] v2, int idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromArray(species, v1, idx1 + i);
            var V2 = FloatVector.fromArray(species, v2, idx2 + i);

            sumV = V1.fma(V2, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            sum += v1[idx1 + i] * v2[idx2 + i];
            i++;
        }

        return sum;
    }

    static float innerProduct(MemorySegment v1, long idx1, float[] v2, int idx2, int size) {
        float res;
        if (v1.isNative()) {
            res = innerProductImpl(v1, idx1, v2, idx2, size);
        } else {
            res = innerProduct((float[]) v1.heapBase().get(), (int) idx1, v2, idx2, size);
        }
        return res;
    }

    private static float innerProductImpl(MemorySegment v1, long idx1, float[] v2, int idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromArray(species, v2, idx2 + i);

            sumV = V1.fma(V2, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            sum += v1.getAtIndex(ValueLayout.JAVA_FLOAT,idx1 + i) * v2[idx2 + i];
            i++;
        }

        return sum;
    }

    static float innerProduct(MemorySegment v1, long idx1, MemorySegment v2, long idx2, int size) {
        float res;
        if (v1.isNative() && v2.isNative()) {
            res = VectorOperationsJ.innerProductImpl(v1, idx1, v2, idx2, size);
        } else if (v1.isNative()) {
            res = VectorOperationsJ.innerProductImpl(v1, idx1, (float[]) v2.heapBase().get(), (int) (idx2), size);
        } else if (v2.isNative()) {
            res = VectorOperationsJ.innerProductImpl(v2, idx2, (float[]) v1.heapBase().get(), (int) (idx1), size);
        } else {
            res = VectorOperationsJ.innerProduct((float[]) v1.heapBase().get(), (int) (idx1), (float[]) v2.heapBase().get(), (int) (idx2), size);
        }
        return res;
    }

    private static float innerProductImpl(MemorySegment v1, long idx1, MemorySegment v2, long idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV = FloatVector.zero(species);

        while (i < loopBound) {
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());

            sumV = V1.fma(V2, sumV);

            i += step;
        }

        var sum = sumV.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            sum += v1.getAtIndex(ValueLayout.JAVA_FLOAT,idx1 + i) * v2.getAtIndex(ValueLayout.JAVA_FLOAT,idx2 + i);
            i++;
        }

        return sum;
    }

    static void innerProductBatch(
            MemorySegment q, long idxQ,
            MemorySegment v1, long idx1,
            MemorySegment v2, long idx2,
            MemorySegment v3, long idx3,
            MemorySegment v4, long idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromMemorySegment(species, q, (idxQ + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V3 = FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V4 = FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.BYTES, ByteOrder.nativeOrder());

            sumV1 = Q.fma(V1, sumV1);
            sumV2 = Q.fma(V2, sumV2);
            sumV3 = Q.fma(V3, sumV3);
            sumV4 = Q.fma(V4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            result[0] += q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) * v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i);
            result[1] += q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) * v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i);
            result[2] += q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) * v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i);
            result[3] += q.getAtIndex(ValueLayout.JAVA_FLOAT, idxQ + i) * v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i);
            i++;
        }
    }

    static void innerProductBatch(
            float[] q, int idxQ,
            MemorySegment v1, long idx1,
            MemorySegment v2, long idx2,
            MemorySegment v3, long idx3,
            MemorySegment v4, long idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromArray(species, q, idxQ + i);
            var V1 = FloatVector.fromMemorySegment(species, v1, (idx1 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V2 = FloatVector.fromMemorySegment(species, v2, (idx2 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V3 = FloatVector.fromMemorySegment(species, v3, (idx3 + i) * Float.BYTES, ByteOrder.nativeOrder());
            var V4 = FloatVector.fromMemorySegment(species, v4, (idx4 + i) * Float.BYTES, ByteOrder.nativeOrder());

            sumV1 = Q.fma(V1, sumV1);
            sumV2 = Q.fma(V2, sumV2);
            sumV3 = Q.fma(V3, sumV3);
            sumV4 = Q.fma(V4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            result[0] += q[idxQ + i] * v1.getAtIndex(ValueLayout.JAVA_FLOAT, idx1 + i);
            result[1] += q[idxQ + i] * v2.getAtIndex(ValueLayout.JAVA_FLOAT, idx2 + i);
            result[2] += q[idxQ + i] * v3.getAtIndex(ValueLayout.JAVA_FLOAT, idx3 + i);
            result[3] += q[idxQ + i] * v4.getAtIndex(ValueLayout.JAVA_FLOAT, idx4 + i);
            i++;
        }
    }

    static void innerProductBatch(
            float[] q, int idxQ,
            float[] v1, int idx1,
            float[] v2, int idx2,
            float[] v3, int idx3,
            float[] v4, int idx4,
            int size, float[] result
    ) {
        var species = FloatVector.SPECIES_PREFERRED;

        var loopBound = species.loopBound(size);
        var step = species.length();
        var i = 0;

        var sumV1 = FloatVector.zero(species);
        var sumV2 = FloatVector.zero(species);
        var sumV3 = FloatVector.zero(species);
        var sumV4 = FloatVector.zero(species);

        while (i < loopBound) {
            var Q = FloatVector.fromArray(species, q, (idxQ + i));
            var V1 = FloatVector.fromArray(species, v1, (idx1 + i));
            var V2 = FloatVector.fromArray(species, v2, (idx2 + i));
            var V3 = FloatVector.fromArray(species, v3, (idx3 + i));
            var V4 = FloatVector.fromArray(species, v4, (idx4 + i));

            sumV1 = Q.fma(V1, sumV1);
            sumV2 = Q.fma(V2, sumV2);
            sumV3 = Q.fma(V3, sumV3);
            sumV4 = Q.fma(V4, sumV4);

            i += step;
        }

        result[0] = sumV1.reduceLanes(VectorOperators.ADD);
        result[1] = sumV2.reduceLanes(VectorOperators.ADD);
        result[2] = sumV3.reduceLanes(VectorOperators.ADD);
        result[3] = sumV4.reduceLanes(VectorOperators.ADD);

        while (i < size) {
            result[0] += q[idxQ + i] * v1[idx1 + i];
            result[1] += q[idxQ + i] * v2[idx2 + i];
            result[2] += q[idxQ + i] * v3[idx3 + i];
            result[3] += q[idxQ + i] * v4[idx4 + i];
            i++;
        }
    }
}
