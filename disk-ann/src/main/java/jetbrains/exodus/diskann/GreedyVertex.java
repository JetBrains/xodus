package jetbrains.exodus.diskann;

final class GreedyVertex implements Comparable<GreedyVertex> {
    final int index;
    float distance;

    boolean isPqDistance;

    GreedyVertex(int index, float distance, boolean isPqDistance) {
        this.index = index;
        this.distance = distance;
        this.isPqDistance = isPqDistance;
    }

    public int compareTo(GreedyVertex other) {
        return Float.compare(distance, other.distance);
    }
}
