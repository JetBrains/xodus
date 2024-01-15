package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class NormalizedVectorReader implements VectorReader {

    private final VectorReader source;

    private final Arena arena;
    private MemorySegment vectorNorms;

    public NormalizedVectorReader(VectorReader source) {
        arena = Arena.ofShared();
        this.source = source;
    }

    @Override
    public int size() {
        return source.size();
    }

    @Override
    public int dimensions() {
        return source.dimensions();
    }

    public void precalculateOriginalVectorNorms(@NotNull ParallelBuddy pBuddy, @NotNull ProgressTracker progressTracker) {
        vectorNorms = arena.allocate((long) source.size() * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
        pBuddy.runSplitEvenly(
                "Original vector norms pre-calculation",
                size(),
                progressTracker,
                (_, vectorIdx) -> {
                    var vector = source.read(vectorIdx);
                    var norm = VectorOperations.calculateL2Norm(vector, dimensions());
                    vectorNorms.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, norm);
                }
        );
    }

    public float getOriginalVectorNorm(int index) {
        return vectorNorms.getAtIndex(ValueLayout.JAVA_FLOAT, index);
    }

    @Override
    public MemorySegment read(int index) {
        var vector = source.read(index);
        var norm = vectorNorms.getAtIndex(ValueLayout.JAVA_FLOAT, index);
        // todo what about pooling a bunch of arrays and reusing them to avoid constant allocations?
        // make those arrays thread local and clean them in close()
        var result = MemorySegment.ofArray(new float[dimensions()]);

        VectorOperations.normalizeL2(vector, norm, result, dimensions());
        return result;
    }


    @Override
    public float read(int vectorIdx, int dimension) {
        var originalValue = source.read(vectorIdx, dimension);
        var norm = vectorNorms.getAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx);
        return originalValue / norm;
    }

    @Override
    public MemorySegment id(int index) {
        return source.id(index);
    }

    @Override
    public void close() {
        arena.close();
    }
}
