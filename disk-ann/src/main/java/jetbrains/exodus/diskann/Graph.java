package jetbrains.exodus.diskann;

import org.jetbrains.annotations.NotNull;

public interface Graph {
    long fetchId(long vertexIndex);

    @NotNull
    float[] fetchVector(long vertexIndex);

    @NotNull
    long[] fetchNeighbours(long vertexIndex);

    long medoid();

    long vertexVersion(long vertexIndex);

    boolean validateVertexVersion(long vertexIndex, long version);

    void acquireVertex(long vertexIndex);

    void releaseVertex(long vertexIndex);
}
