package jetbrains.exodus.diskann;

final class GreedyVertex implements Comparable<GreedyVertex> {
    final long index;
    double distance;
    boolean visited;

    boolean isPqDistance;

    GreedyVertex(long index, double distance, boolean isPqDistance) {
        this.index = index;
        this.distance = distance;
        this.isPqDistance = isPqDistance;
    }

    public int compareTo(GreedyVertex other) {
        return Double.compare(distance, other.distance);
    }
}
