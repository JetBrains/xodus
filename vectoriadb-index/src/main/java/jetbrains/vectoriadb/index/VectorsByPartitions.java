package jetbrains.vectoriadb.index;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public record VectorsByPartitions(float[][] partitionCentroids, IntArrayList[] vectorsByCentroidIdx) {}
