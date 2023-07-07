package jetbrains.exodus.diskann;

final class GreedyVertex implements Comparable<GreedyVertex> {
    final int index;
    double distance;

    boolean isPqDistance;

    GreedyVertex(int index, double distance, boolean isPqDistance) {
        this.index = index;
        this.distance = distance;
        this.isPqDistance = isPqDistance;
    }

    public int compareTo(GreedyVertex other) {
        return Double.compare(distance, other.distance);
    }
}
