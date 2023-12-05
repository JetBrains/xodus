package jetbrains.vectoriadb.index.vector

import jetbrains.vectoriadb.index.segment.FloatVectorSegment
import java.lang.foreign.Arena
import kotlin.random.Random

fun createRandomFloatVectorSegment(count: Int, dimensions: Int): FloatVectorSegment {
    val v1 = FloatVectorSegment.makeArraySegment(count, dimensions)
    v1.fillRandom()
    return v1
}

fun Arena.createRandomFloatVectorSegment(count: Int, dimensions: Int, heapBased: Boolean = false): FloatVectorSegment {
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
