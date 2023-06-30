package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;

public interface VectorReader {
    int size();

    LongObjectImmutablePair<float[]> read(int index);
}
