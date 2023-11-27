package jetbrains.vectoriadb.index;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class VectorOperations {

    private static final float PRECISION = (float) 1e-5;

    public static float calculateL2Norm(float[] vector) {
        // DotDistance returns -(inner product). It lets us think in the "the smaller, the closer" way.
        return (float) Math.sqrt(-DotDistanceFunction.INSTANCE.computeDistance(vector, 0, vector, 0, vector.length));
    }

    public static float calculateL2Norm(MemorySegment vector, int size) {
        // DotDistance returns -(inner product). It lets us think in the "the smaller, the closer" way.
        return (float) Math.sqrt(-DotDistanceFunction.INSTANCE.computeDistance(vector, 0, vector, 0, size));
    }

    /**
     * Normalizes the vector by L2 norm, writes result to the result and returns the original vector norm
    * */
    public static void normalizeL2(float[] vector, float[] result) {
        var norm = calculateL2Norm(vector);

        if (Math.abs(norm - 1) > PRECISION) {
            for (int i = 0; i < vector.length; i++) {
                result[i] = vector[i] / norm;
            }
        } else {
            System.arraycopy(vector, 0, result, 0, vector.length);
        }

        assert calculateL2Norm(result) < PRECISION;
    }

    public static void normalizeL2(MemorySegment vector, float vectorNorm, int size, MemorySegment result) {
        if (Math.abs(vectorNorm - 1) > PRECISION) {
            for (int i = 0; i < size; i++) {
                result.setAtIndex(ValueLayout.JAVA_FLOAT, i,vector.getAtIndex(ValueLayout.JAVA_FLOAT, i) / vectorNorm);
            }
        } else {
            result.copyFrom(vector);
        }

        assert calculateL2Norm(result, size) < 1e-5;
    }
}
