package jetbrains.exodus.diskann;

public interface VectorReader {
    int size();

    float[] read(int index);
}
