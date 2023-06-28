package jetbrains.exodus.diskann;

final class GreedyVertex implements Comparable<GreedyVertex> {
    final long index;
    final double distance;
    volatile boolean visited;

    GreedyVertex(long index, double distance) {
        this.index = index;
        this.distance = distance;
    }

    public int compareTo(GreedyVertex other) {
        return Double.compare(distance, other.distance);
    }
}
