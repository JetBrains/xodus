package jetbrains.vectoriadb.index

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import kotlin.random.Random

internal fun createRandomFloatVectorSegment(count: Int, dimensions: Int): FloatVectorSegment {
    val v1 = FloatVectorSegment.makeArraySegment(count, dimensions)
    v1.fillRandom()
    return v1
}

internal fun Arena.createRandomFloatVectorSegment(count: Int, dimensions: Int, heapBased: Boolean = false): FloatVectorSegment {
    val v1 = if (heapBased) {
        FloatVectorSegment.makeArraySegment(count, dimensions)
    } else {
        FloatVectorSegment.makeNativeSegment(this, count, dimensions)
    }
    v1.fillRandom()
    return v1
}

private fun FloatVectorSegment.fillRandom() {
    repeat(this.count()) { vectorIdx ->
        repeat(this.dimensions()) { dimensionIdx ->
            this.set(vectorIdx, dimensionIdx, Random.nextDouble(1000.0).toFloat())
        }
    }
}

internal class FloatArrayToByteArrayVectorReader: VectorReader {

    private val vectors: Array<FloatArray>
    private val size: Int

    constructor(vectors: Array<FloatArray>) {
        this.vectors = vectors
        size = vectors.size
    }

    constructor(vectors: Array<FloatArray>, size: Int) {
        this.vectors = vectors
        this.size = size
    }

    override fun size(): Int {
        return size
    }

    override fun dimensions(): Int {
        return vectors[0].size
    }

    override fun read(index: Int): MemorySegment {
        val vectorSegment = MemorySegment.ofArray(
            ByteArray(vectors[index].size * java.lang.Float.BYTES)
        )

        MemorySegment.copy(
            MemorySegment.ofArray(vectors[index]), 0,
            vectorSegment, 0, vectors[index].size.toLong() * java.lang.Float.BYTES
        )

        return vectorSegment
    }

    override fun id(index: Int): MemorySegment? {
        return null
    }

    override fun close() {
    }
}

internal fun parallelTest(numWorkers: Int = ParallelExecution.availableCores(), test: (ParallelBuddy, Arena) -> Unit) {
    ParallelBuddy(numWorkers, "k-means clustering test").use { pBuddy ->
        Arena.ofShared().use { arena ->
            test(pBuddy, arena)
        }
    }
}