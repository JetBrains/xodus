package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.TestOnly;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class FloatVectorSegment {
    private static final ValueLayout.OfFloat LAYOUT = ValueLayout.JAVA_FLOAT;
    private static final int BYTES = Float.BYTES;

    private final int dimensions;
    private final int count;
    private final MemorySegment vectors;

    private FloatVectorSegment(int count, int dimensions, MemorySegment vectors) {
        this.dimensions = dimensions;
        this.count = count;
        this.vectors = vectors;
    }

    public int dimensions() {
        return dimensions;
    }

    public int count() {
        return count;
    }

    public void add(int idx1, FloatVectorSegment v2, int idx2) {
        add(idx1, v2.vectors, idx2);
    }

    public void add(int idx1, MemorySegment v2, int idx2) {
        VectorOperations.add(vectors, (long) idx1 * dimensions, v2, (long) idx2 * dimensions, vectors, (long) idx1 * dimensions, dimensions);
    }

    public void div(int idx, float scalar) {
        VectorOperations.div(vectors, (long) idx * dimensions, scalar, vectors, (long) idx * dimensions, dimensions);
    }

    public float get(int vectorIdx, int dimension) {
        return vectors.getAtIndex(LAYOUT,(long) vectorIdx * dimensions + dimension);
    }

    public void set(int vectorIdx, int dimension, float value) {
        vectors.setAtIndex(LAYOUT, (long) vectorIdx * dimensions + dimension, value);
    }

    public void set(int vectorIdx, MemorySegment vector) {
        MemorySegment.copy(vector, 0, vectors, (long) vectorIdx * dimensions * BYTES, (long) dimensions * BYTES);
    }

    public MemorySegment get(int vectorIdx) {
        return vectors.asSlice((long) vectorIdx * dimensions * BYTES, (long) dimensions * BYTES);
    }

    public float[][] toArray() {
        var array = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            MemorySegment.copy(vectors, LAYOUT, (long) i * dimensions * BYTES, array[i], 0, dimensions);
        }
        return array;
    }

    public void fill(byte value) {
        vectors.fill(value);
    }

    public FloatVectorSegment copy() {
        return new FloatVectorSegment(count, dimensions, MemorySegment.ofArray(vectors.toArray(LAYOUT)));
    }

    public boolean equals(int idx1, FloatVectorSegment v2, int idx2) {
        for (int i = 0; i < dimensions; i++) {
            if (Math.abs(this.get(idx1, i) - v2.get(idx2, i)) > VectorOperations.PRECISION) {
                return false;
            }
        }
        return true;
    }

    public boolean equals(FloatVectorSegment v2) {
        if (count != v2.count) return false;
        if (dimensions != v2.dimensions) return false;
        for (int i = 0; i < count; i++) {
            if (!equals(i, v2, i)) return false;
        }
        return true;
    }

    public static FloatVectorSegment makeNativeSegment(Arena arena, int count, int dimensions) {
        var segment = arena.allocate((long) count * dimensions * BYTES, LAYOUT.byteAlignment());
        return new FloatVectorSegment(count, dimensions, segment);
    }

    /**
     * Be careful using this method.
     * L2DistanceFunction and DotDistanceFunction fail when working with array-based segment with float arrays behind.
     * They work only with byte arrays behind. It is a bug in JVM, so until it is fixed, be careful using this method.
     * Do not use it if you pass FloatVectorSegment to code that calculates distance using L2DistanceFunction and DotDistanceFunction.
    * */
    @TestOnly
    public static FloatVectorSegment makeArraySegment(int count, int dimensions) {
        var array = new float[count * dimensions];
        return new FloatVectorSegment(count, dimensions, MemorySegment.ofArray(array));
    }

    public static FloatVectorSegment[] makeNativeSegments(Arena arena, int segmentCount, int vectorsPerSegment, int dimensions) {
        var segment = arena.allocate((long) segmentCount * vectorsPerSegment * dimensions * BYTES, LAYOUT.byteAlignment());

        var result = new FloatVectorSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            result[i] = new FloatVectorSegment(vectorsPerSegment, dimensions, segment.asSlice((long) i * vectorsPerSegment * dimensions * BYTES, (long) vectorsPerSegment * dimensions * BYTES));
        }
        return result;
    }
}
