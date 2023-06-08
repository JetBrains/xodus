package jetbrains.exodus.diskann

internal class GreedyVertex(
    @JvmField val index: Long, @JvmField var distance: Double,
    @JvmField var visited: Boolean = false
) : Comparable<GreedyVertex> {
    override fun compareTo(other: GreedyVertex): Int {
        return distance.compareTo(other.distance)
    }
}