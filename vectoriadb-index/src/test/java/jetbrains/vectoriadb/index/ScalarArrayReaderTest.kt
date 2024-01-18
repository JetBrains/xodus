package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import java.lang.foreign.ValueLayout

class ScalarArrayReaderTest {

    @Test
    fun read() {
        val count = 100
        val dimensions = 1
        val v = createRandomFloatVectorSegment(count, dimensions)

        val reader = ScalarArrayReader(v.internalArray)

        assert(reader.size() == v.count())

        repeat(count) { vectorIdx ->
            val v2 = reader.read(vectorIdx)
            repeat(dimensions) { dimensionIdx ->
                val value1 = v.internalArray[vectorIdx * dimensions + dimensionIdx]
                val value2 = v2.getAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx.toLong())
                Assert.assertEquals(value1, value2, VectorOperations.PRECISION)
            }
        }
    }
}