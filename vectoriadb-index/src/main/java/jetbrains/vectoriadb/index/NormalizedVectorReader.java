package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class NormalizedVectorReader implements VectorReader {

    private final Arena arena;

    private final VectorReader source;
    private final int vectorDimensions;

    private final MemorySegment vectorNorms;

    NormalizedVectorReader(final int vectorDimensions, final VectorReader source) {
        arena = Arena.ofShared();
        this.vectorDimensions = vectorDimensions;
        this.source = source;
        vectorNorms = arena.allocate((long) source.size() * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
        for (int i = 0; i < source.size(); i++) {
            vectorNorms.setAtIndex(ValueLayout.JAVA_FLOAT, i, -1);
        }
    }

    @Override
    public int size() {
        return source.size();
    }

    public void precalculateOriginalVectorNorms(@NotNull ProgressTracker progressTracker) {
        ParallelExecution.execute(
                "Original vector norms pre-calculation",
                "norm-explicit-pq-norm-pre-calculation-",
                size(),
                progressTracker,
                (vectorIdx) -> {
                    var vector = source.read((int) vectorIdx);
                    var norm = VectorOperations.calculateL2Norm(vector, vectorDimensions);
                    vectorNorms.setAtIndex(ValueLayout.JAVA_FLOAT, vectorIdx, norm);
                }
        );
    }

    @Override
    public MemorySegment read(final long index) {
        var vector = source.read(index);
        var result = MemorySegment.ofArray(new float[vectorDimensions]);
        var norm = vectorNorms.getAtIndex(ValueLayout.JAVA_FLOAT, index);
        VectorOperations.normalizeL2(vector, norm, vectorDimensions, result);
        return result;
    }

    public float getOriginalVectorNorm(final long index) {
        return vectorNorms.getAtIndex(ValueLayout.JAVA_FLOAT, index);
    }

    @Override
    public void close() {
        arena.close();
    }
}
