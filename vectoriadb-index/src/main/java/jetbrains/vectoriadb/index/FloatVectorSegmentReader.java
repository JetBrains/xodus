package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;

class FloatVectorSegmentReader implements VectorReader {

    @NotNull
    private final FloatVectorSegment vectors;

    public FloatVectorSegmentReader(@NotNull FloatVectorSegment vectors) {
        this.vectors = vectors;
    }

    @Override
    public int size() {
        return vectors.count();
    }

    @Override
    public int dimensions() {
        return vectors.dimensions();
    }

    @Override
    public MemorySegment read(int index) {
        return vectors.get(index);
    }

    @Override
    public float read(int vectorIdx, int dimension) {
        return vectors.get(vectorIdx, dimension);
    }

    @Override
    public MemorySegment id(int index) {
        // this vector reader should not be used in a context where vector id is required
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {

    }
}
