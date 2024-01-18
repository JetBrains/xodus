package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;

class ScalarArrayReader implements VectorReader {

    @NotNull
    private final float[] values;

    public ScalarArrayReader(@NotNull float[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public int dimensions() {
        return 1;
    }

    @Override
    public float read(int vectorIdx, int dimension) {
        return values[vectorIdx];
    }

    @Override
    public MemorySegment read(int index) {
        return MemorySegment.ofArray(new float[] { values[index] });
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
