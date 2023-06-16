package jetbrains.exodus.diskann

internal interface Graph {
    fun fetchId(vertexIndex: Long): Long
    fun fetchVector(vertexIndex: Long): FloatArray
    fun fetchNeighbours(vertexIndex: Long): LongArray
    fun medoid(): Long
    fun vertexVersion(vertexIndex: Long): Long
    fun validateVertexVersion(vertexIndex: Long, version: Long): Boolean

    fun acquireVertex(vertexIndex: Long)

    fun releaseVertex(vertexIndex: Long)
}