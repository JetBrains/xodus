package jetbrains.vectoriadb.index.vector

import jetbrains.vectoriadb.index.segment.FloatVectorSegment
import org.junit.Assert
import org.junit.Test
import java.lang.foreign.ValueLayout
import kotlin.random.Random

class FloatVectorSegmentReaderTest {

    @Test
    fun read() {
        val count = 100
        val dimensions = 300
        val v = FloatVectorSegment.makeArraySegment(count, dimensions)
        for (vectorIdx in 0 until count) {
            for (dimension in 0 until dimensions) {
                v.set(vectorIdx, dimension, Random.nextDouble(1000.0).toFloat())
            }
        }

        val reader = FloatVectorSegmentReader(v)

        assert(reader.size() == v.count())

        repeat(count) { vectorIdx ->
            val v1 = v.get(vectorIdx)
            val v2 = reader.read(vectorIdx)
            repeat(dimensions) { dimensionIdx ->
                val value1 = v1.getAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx.toLong())
                val value2 = v2.getAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx.toLong())
                Assert.assertEquals(value1, value2, VectorOperations.PRECISION)
            }
        }
    }
}