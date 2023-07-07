package jetbrains.exodus.diskann;

final class RobustPruneVertex implements Comparable<RobustPruneVertex> {
    final int index;
    final double distance;

    RobustPruneVertex(int index, double distance) {
        this.index = index;
        this.distance = distance;
    }

    public int compareTo(RobustPruneVertex other) {
        return Double.compare(distance, other.distance);
    }
}
