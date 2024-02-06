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
package jetbrains.exodus.core.cache.persistent

import jetbrains.exodus.testutil.runInParallel
import org.junit.Assert.assertEquals
import org.junit.Test


class ValueMapTest {

    @Test
    fun `should calculate total weight when put`() {
        // Given
        val map = givenValueMap()

        // When
        map.put(1, "value1")
        map.put(2, "value2")
        map.put(3, "value3")
        // Replace value1
        map.put(1, "value4")

        // Then
        assertEquals(18, map.totalWeight)
    }

    @Test
    fun `should calculate total weight when remove`() {
        // Given
        val map = givenValueMap()

        // When
        map.put(1, "value1")
        map.put(2, "value2")
        map.put(3, "value3")
        map.remove(1)

        // Then
        assertEquals(12, map.totalWeight)
    }

    @Test
    fun `should be thread-safe`() {
        // Given
        val map = givenValueMap()
        val value = "value" // weight is 5
        val iterations = 100000
        val concurrencyLevel = 10

        // When
        runInParallel(concurrencyLevel, iterations) { i ->
            map.put(i.toLong(), "value")
            if (i % 2 == 0) {
                map.remove(i.toLong())
            }
        }

        // Then
        assertEquals("Unexpected collection size", iterations / 2, map.size)
        assertEquals("Unexpected total weight", iterations * value.length / 2, map.totalWeight)
    }


    private fun givenValueMap(): ValueMap<Long, String> {
        return ValueMap { value: String -> value.length }
    }
}