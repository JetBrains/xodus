package jetbrains.exodus.diskann;

final class RobustPruneVertex implements Comparable<RobustPruneVertex> {
    final long index;
    final float[] vector;
    final double distance;

    RobustPruneVertex(long index, float[] vector, double distance) {
        this.index = index;
        this.vector = vector;
        this.distance = distance;
    }

    public int compareTo(RobustPruneVertex other) {
        return Double.compare(distance, other.distance);
    }
}
