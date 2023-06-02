package jetbrains.exodus.diskann

internal interface Graph {
    fun fetchId(vertexIndex: Long): Long
    fun fetchVector(vertexIndex: Long): FloatArray
    fun fetchNeighbours(vertexIndex: Long): LongArray
    fun medoid(): Long
}