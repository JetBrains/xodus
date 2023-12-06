package jetbrains.vectoriadb.index;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static java.lang.Math.abs;

class VectorOperations {

    public static final float PRECISION = (float) 1e-5;

    /**
     * vector1 + vector2 -> result
     * */
    public static void add(MemorySegment v1, long idx1, MemorySegment v2, long idx2, MemorySegment result, long resultIdx, int size) {
        var species = FloatVector.SPECIES_PREFERRED;
        var step = species.length();
        for (int i = 0; i < size; i += step) {
            var mask = species.indexInRange(i, size);
            var V1 = toVector(v1, idx1 + i, species, mask);
            var V2 = toVector(v2, idx2 + i, species, mask);

            var R = V1.add(V2);
            intoResult(R, result, resultIdx + i, mask);
        }
    }

    /**
     * vector1 / scalar -> result
     * */
    public static void div(MemorySegment v1, long idx1, float scalar, MemorySegment result, long resultIdx, int size) {
        var species = FloatVector.SPECIES_PREFERRED;
        var step = species.length();
        for (int i = 0; i < size; i += step) {
            var mask = species.indexInRange(i, size);
            var V1 = toVector(v1, idx1 + i, species, mask);

            var R = V1.div(scalar);
            intoResult(R, result, resultIdx + i, mask);
        }
    }

    public static float innerProduct(MemorySegment v1, long idx1, MemorySegment v2, long idx2, int size) {
        var species = FloatVector.SPECIES_PREFERRED;
        var step = species.length();
        var result = 0f;
        for (int i = 0; i < size; i += step) {
            var mask = species.indexInRange(i, size);
            var V1 = toVector(v1, idx1 + i, species, mask);
            var V2 = toVector(v2, idx2 + i, species, mask);

            var R = V1.mul(V2);
            result += R.reduceLanes(VectorOperators.ADD, mask);
        }
        return result;
    }

    public static float calculateL2Norm(float[] vector) {
        var v = MemorySegment.ofArray(vector);
        return (float) Math.sqrt(innerProduct(v, 0, v, 0, vector.length));
    }

    public static float calculateL2Norm(MemorySegment vector, int size) {
        return (float) Math.sqrt(innerProduct(vector, 0, vector, 0, size));
    }

    /**
     * Normalizes the vector by L2 norm, writes result to the result
    * */
    public static void normalizeL2(float[] vector, float[] result) {
        var vSegment = MemorySegment.ofArray(vector);
        normalizeL2(vSegment, calculateL2Norm(vSegment, vector.length), MemorySegment.ofArray(result), vector.length);
    }

    public static void normalizeL2(MemorySegment vector, float vectorNorm, MemorySegment result, int size) {
        if (abs(vectorNorm - 1) > PRECISION) {
            var species = FloatVector.SPECIES_PREFERRED;
            var step = species.length();
            for (int i = 0; i < size; i += step) {
                var mask = species.indexInRange(i, size);
                var V1 = toVector(vector, i, species, mask);

                var R = V1.div(vectorNorm);
                intoResult(R, result, i, mask);
            }
        } else {
            result.copyFrom(vector);
        }

        assert abs(calculateL2Norm(result, size) - 1) < PRECISION;
    }

    private static FloatVector toVector(MemorySegment v, long valueIdx, VectorSpecies<Float> species, VectorMask<Float> mask) {
        /*
         * Vector API supports only heap-based segments with byte[] arrays behind.
         * If we use heap-based segments with float[] arrays behind, we will get a strange exception at runtime.
         * Most probably it is a temporary problem and some day they will fix it.
         * But for now, we have to hack this case around.
         * */
        FloatVector V;
        if (v.heapBase().isEmpty()) {
            V = FloatVector.fromMemorySegment(species, v, valueIdx * Float.BYTES, ByteOrder.nativeOrder(), mask);
        } else {
            var arr = (float[]) v.heapBase().get();
            V = FloatVector.fromArray(species, arr, (int) valueIdx, mask);
        }
        return V;
    }

    private static void intoResult(FloatVector R, MemorySegment result, long valueIdx, VectorMask<Float> mask) {
        /*
         * Vector API supports only heap-based segments with byte[] arrays behind.
         * If we use heap-based segments with float[] arrays behind, we will get a strange exception at runtime.
         * Most probably it is a temporary problem and some day they will fix it.
         * But for now, we have to hack this case around.
         * */
        if (result.heapBase().isEmpty()) {
            R.intoMemorySegment(result, valueIdx * Float.BYTES, ByteOrder.nativeOrder(), mask);
        } else {
            var arr = (float[]) result.heapBase().get();
            R.intoArray(arr, (int) valueIdx, mask);
        }
    }
}
