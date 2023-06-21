package jetbrains.exodus.diskann;

final class RobustPruneVertex implements Comparable<RobustPruneVertex> {
    final long index;
    final double distance;

    RobustPruneVertex(long index, double distance) {
        this.index = index;
        this.distance = distance;
    }

    public int compareTo(RobustPruneVertex other) {
        return Double.compare(distance, other.distance);
    }
}
