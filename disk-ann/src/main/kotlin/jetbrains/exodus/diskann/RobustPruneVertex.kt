package jetbrains.exodus.diskann

internal class RobustPruneVertex(@JvmField val index: Long, @JvmField val vector: FloatArray,
                                 @JvmField val distance: Double) : Comparable<RobustPruneVertex> {
    override fun compareTo(other: RobustPruneVertex): Int {
        return distance.compareTo(other.distance)
    }
}