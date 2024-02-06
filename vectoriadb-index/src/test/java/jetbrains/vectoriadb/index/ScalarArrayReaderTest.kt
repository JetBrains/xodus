/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import java.lang.foreign.ValueLayout

class ScalarArrayReaderTest {

    @Test
    fun read() {
        val count = 100
        val dimensions = 1
        val v = randomFloatVectorSegment(count, dimensions)

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