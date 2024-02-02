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

import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CyclicBarrier
import kotlin.concurrent.thread


class WeightedValueMapTest {

    @Test
    fun `should calculate total weight when put`() {
        // Given
        val map = givenWeightedValueMap()

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
        val map = givenWeightedValueMap()

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
        val map = givenWeightedValueMap()
        val value = "value" // weight is 5
        val putValue = { i: Long -> map.put(i, "value") }
        val removeValue = { i: Long -> map.remove(i) }
        val n = 100

        // When
        val barrier = CyclicBarrier(n + 1)
        val threads = (1..n).map { i ->
            thread {
                barrier.await()
                putValue(i.toLong())
                if (i % 2 == 0) {
                    removeValue(i.toLong())
                }
            }
        }
        barrier.await()
        threads.forEach { it.join() }

        // Then
        assertEquals(n * value.length / 2, map.totalWeight)
    }


    private fun givenWeightedValueMap(): ValueMap<Long, String> {
        return ValueMap { value: String -> value.length }
    }
}