package jetbrains.exodus.core.cache.persistent

import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CyclicBarrier
import kotlin.concurrent.thread
import kotlin.random.Random


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
        val putTask = { i: Long -> map.put(i, "value") }
        val removeTask = { i: Long -> map.remove(i) }
        val rnd = Random(0)
        val n = 100

        // When
        val barrier = CyclicBarrier(n + 1)
        val threads = (1..n).map { i ->
            thread {
                barrier.await()
                putTask(i.toLong())
                Thread.sleep(rnd.nextLong(1, 10))
                if (i % 2 == 0) {
                    // Remove only even keys
                    removeTask(i.toLong())
                }
            }
        }
        barrier.await()
        threads.forEach { it.join() }

        // Then
        assertEquals(n * value.length / 2, map.totalWeight)
    }


    private fun givenWeightedValueMap(): WeightedValueMap<Long, String> {
        return WeightedValueMap { value: String -> value.length }
    }
}