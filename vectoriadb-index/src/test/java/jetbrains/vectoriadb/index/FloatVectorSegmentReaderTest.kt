package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import java.lang.foreign.ValueLayout

class FloatVectorSegmentReaderTest {

    @Test
    fun read() {
        val count = 100
        val dimensions = 300
        val v = createRandomFloatVectorSegment(count, dimensions)

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