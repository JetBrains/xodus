package jetbrains.exodus.diskann

interface VectorReader {
    fun size(): Long
    fun read(index: Long): Pair<Long, FloatArray>
}