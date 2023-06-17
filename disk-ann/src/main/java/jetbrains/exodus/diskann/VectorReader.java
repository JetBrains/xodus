package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;

public interface VectorReader {
    long size();

    LongObjectImmutablePair<float[]> read(long index);
}
