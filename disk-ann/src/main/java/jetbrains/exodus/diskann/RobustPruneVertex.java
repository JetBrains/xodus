package jetbrains.exodus.diskann;

final class RobustPruneVertex implements Comparable<RobustPruneVertex> {
    final int index;
    final float distance;

    RobustPruneVertex(int index, float distance) {
        this.index = index;
        this.distance = distance;
    }

    public int compareTo(RobustPruneVertex other) {
        return Double.compare(distance, other.distance);
    }
}
